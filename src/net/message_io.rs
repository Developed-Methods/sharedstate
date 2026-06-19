use std::{fmt::Debug, hint::black_box, time::Duration};

use message_encoding::MessageEncoding;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type MessageSizeHeader = u32;
const MESSAGE_HEADER_SIZE: usize = std::mem::size_of::<MessageSizeHeader>();
const KEEPALIVE_FRAME_SIZE: MessageSizeHeader = 0;
const CLOSE_FRAME_SIZE: MessageSizeHeader = MessageSizeHeader::MAX;

#[derive(Debug)]
pub enum ReadMessageResult<M> {
    Message(M),
    KeepAlive,
    Close,
}

#[derive(Debug)]
pub enum ReadMessageToVecResult {
    Message,
    KeepAlive,
    Close,
}

pub async fn read_message_opt<M: MessageEncoding, R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
    recv_timeout: Option<Duration>,
) -> Result<ReadMessageResult<M>, ReadMessageError> {
    let _assert = black_box(M::_ASSERT);

    match read_message_to_vec(buffer, read, progress_timeout, recv_timeout).await? {
        ReadMessageToVecResult::Message => {}
        ReadMessageToVecResult::KeepAlive => return Ok(ReadMessageResult::KeepAlive),
        ReadMessageToVecResult::Close => return Ok(ReadMessageResult::Close),
    };

    let mut reader = &buffer[..];
    let msg = M::read_from(&mut reader).map_err(ReadMessageError::EncodingError)?;
    Ok(ReadMessageResult::Message(msg))
}

pub async fn read_message_to_vec<R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
    recv_timeout: Option<Duration>,
) -> Result<ReadMessageToVecResult, ReadMessageError> {
    let mut len_bytes = [0u8; MESSAGE_HEADER_SIZE];

    if let Some(timeout) = recv_timeout {
        tokio::time::timeout(timeout, read.read_exact(&mut len_bytes[..]))
            .await
            .map_err(|_| ReadMessageError::NextMessageTimeout(timeout))?
            .map_err(ReadMessageError::SizeReadError)?;
    } else {
        read.read_exact(&mut len_bytes[..])
            .await
            .map_err(ReadMessageError::SizeReadError)?;
    }

    let msg_len = MessageSizeHeader::from_be_bytes(len_bytes);
    match msg_len {
        KEEPALIVE_FRAME_SIZE => return Ok(ReadMessageToVecResult::KeepAlive),
        CLOSE_FRAME_SIZE => return Ok(ReadMessageToVecResult::Close),
        _ => {}
    }

    let msg_len = msg_len as usize;
    buffer.clear();
    buffer.resize(msg_len, 0u8);

    let mut bytes_read = 0;
    while bytes_read < msg_len {
        match tokio::time::timeout(progress_timeout, read.read(&mut buffer[bytes_read..])).await {
            Err(_) => return Err(ReadMessageError::MessageReadTimeout),
            Ok(Err(error)) => return Err(ReadMessageError::MessageReadError(error)),
            Ok(Ok(0)) => return Err(ReadMessageError::Closed),
            Ok(Ok(bytes)) => {
                bytes_read += bytes;
            }
        }
    }

    Ok(ReadMessageToVecResult::Message)
}

#[derive(Debug)]
pub enum ReadMessageError {
    MessageReadTimeout,
    NextMessageTimeout(Duration),
    SizeReadError(std::io::Error),
    MessageReadError(std::io::Error),
    EncodingError(std::io::Error),
    Closed,
}

impl From<ReadMessageError> for std::io::Error {
    fn from(value: ReadMessageError) -> Self {
        match value {
            ReadMessageError::MessageReadTimeout | ReadMessageError::NextMessageTimeout(_) => {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout reading remaining message data")
            }
            ReadMessageError::Closed => {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "end of file reading message")
            }
            ReadMessageError::SizeReadError(e)
            | ReadMessageError::MessageReadError(e)
            | ReadMessageError::EncodingError(e) => e,
        }
    }
}

pub async fn send_zero_message<W: AsyncWrite + Unpin>(out: &mut W) -> std::io::Result<()> {
    out.write_all(&KEEPALIVE_FRAME_SIZE.to_be_bytes()).await?;
    out.flush().await
}

pub async fn send_close_message<W: AsyncWrite + Unpin>(out: &mut W) -> std::io::Result<()> {
    out.write_all(&CLOSE_FRAME_SIZE.to_be_bytes()).await?;
    out.flush().await
}

pub async fn send_message<M: MessageEncoding, W: AsyncWrite + Unpin>(
    buffer: &mut Vec<u8>,
    message: &M,
    out: &mut W,
    progress_timeout: Duration,
) -> std::io::Result<()> {
    let _assert = black_box(M::_ASSERT);

    buffer.clear();

    if let Some(max_size) = M::MAX_SIZE {
        buffer.reserve(MESSAGE_HEADER_SIZE + max_size);
        assert!(max_size < (MessageSizeHeader::MAX as usize));
    }

    buffer.extend((0 as MessageSizeHeader).to_be_bytes());
    assert_eq!(buffer.len(), MESSAGE_HEADER_SIZE);

    let bytes_written = message.write_to(buffer)?;
    assert_eq!(bytes_written + MESSAGE_HEADER_SIZE, buffer.len(), "M::write_to returned incorrect number of bytes");
    if bytes_written >= CLOSE_FRAME_SIZE as usize {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "message too large for framing protocol"));
    }

    /* if static size is known, we've already written size with MAX_SIZE var */
    if let Some(size) = M::STATIC_SIZE {
        assert_eq!(size, bytes_written, "M::STATIC_SIZE does not match M::write_to");
    }
    /* write size to start of buffer */
    else {
        buffer[..MESSAGE_HEADER_SIZE].copy_from_slice(&(bytes_written as MessageSizeHeader).to_be_bytes());
    }

    /* note: send in batches with timeout to ensure connection isn't hanging and we also can
     * support sending really large messages */
    let mut written = 0;
    while written < buffer.len() {
        match tokio::time::timeout(progress_timeout, out.write(&buffer[written..])).await {
            Ok(Ok(bytes)) => {
                written += bytes;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout writing data")),
        }
    }

    out.flush().await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn read_close_frame_returns_close() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&CLOSE_FRAME_SIZE.to_be_bytes()).await.unwrap();

        let mut buffer = Vec::new();
        let result = read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None)
            .await
            .unwrap();

        assert!(matches!(result, ReadMessageResult::Close));
    }

    #[tokio::test]
    async fn read_keepalive_frame_returns_keepalive() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&KEEPALIVE_FRAME_SIZE.to_be_bytes()).await.unwrap();

        let mut buffer = Vec::new();
        let result = read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None)
            .await
            .unwrap();

        assert!(matches!(result, ReadMessageResult::KeepAlive));
    }

    #[tokio::test]
    async fn send_close_message_writes_close_header() {
        let (mut writer, mut reader) = duplex(64);
        send_close_message(&mut writer).await.unwrap();

        let mut header = [0; MESSAGE_HEADER_SIZE];
        reader.read_exact(&mut header).await.unwrap();

        assert_eq!(header, CLOSE_FRAME_SIZE.to_be_bytes());
    }
}
