use std::{fmt::Debug, hint::black_box, time::Duration};

use message_encoding::MessageEncoding;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type MessageSizeHeader = u32;
const MESSAGE_HEADER_SIZE: usize = std::mem::size_of::<MessageSizeHeader>();
const KEEPALIVE_FRAME_SIZE: MessageSizeHeader = 0;
const CLOSE_FRAME_SIZE: MessageSizeHeader = MessageSizeHeader::MAX;
pub const DEFAULT_MAX_FRAME_SIZE: MessageSizeHeader = 64 * 1024 * 1024;

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
    max_frame_size: MessageSizeHeader,
) -> Result<ReadMessageResult<M>, ReadMessageError> {
    let _assert = black_box(M::_ASSERT);

    match read_message_to_vec(buffer, read, progress_timeout, recv_timeout, max_frame_size).await? {
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
    max_frame_size: MessageSizeHeader,
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
    if max_frame_size == KEEPALIVE_FRAME_SIZE || msg_len > max_frame_size {
        return Err(ReadMessageError::FrameTooLarge {
            size: msg_len,
            max_size: max_frame_size,
        });
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
    FrameTooLarge {
        size: MessageSizeHeader,
        max_size: MessageSizeHeader,
    },
    Closed,
}

impl ReadMessageError {
    pub fn is_disconnect(&self) -> bool {
        match self {
            ReadMessageError::Closed => true,
            ReadMessageError::SizeReadError(error) | ReadMessageError::MessageReadError(error) => matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ),
            ReadMessageError::MessageReadTimeout
            | ReadMessageError::NextMessageTimeout(_)
            | ReadMessageError::EncodingError(_)
            | ReadMessageError::FrameTooLarge { .. } => false,
        }
    }
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
            ReadMessageError::FrameTooLarge { size, max_size } => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame size {size} exceeds maximum frame size {max_size}"),
            ),
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
    max_frame_size: MessageSizeHeader,
) -> std::io::Result<()> {
    let _assert = black_box(M::_ASSERT);

    buffer.clear();

    if let Some(max_size) = M::MAX_SIZE {
        buffer.reserve(MESSAGE_HEADER_SIZE + max_size);
        debug_assert!(max_size < (MessageSizeHeader::MAX as usize));
    }

    buffer.extend((0 as MessageSizeHeader).to_be_bytes());
    debug_assert_eq!(buffer.len(), MESSAGE_HEADER_SIZE);

    let bytes_written = message.write_to(buffer)?;
    debug_assert_eq!(
        bytes_written + MESSAGE_HEADER_SIZE,
        buffer.len(),
        "M::write_to returned incorrect number of bytes"
    );
    if bytes_written >= CLOSE_FRAME_SIZE as usize {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "message too large for framing protocol"));
    }
    if bytes_written == KEEPALIVE_FRAME_SIZE as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "zero-length messages are reserved for keepalive frames",
        ));
    }
    if bytes_written > max_frame_size as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("message size {bytes_written} exceeds maximum frame size {max_frame_size}"),
        ));
    }

    if let Some(size) = M::STATIC_SIZE {
        debug_assert_eq!(size, bytes_written, "M::STATIC_SIZE does not match M::write_to");
    }
    buffer[..MESSAGE_HEADER_SIZE].copy_from_slice(&(bytes_written as MessageSizeHeader).to_be_bytes());

    /* note: send in batches with timeout to ensure connection isn't hanging and we also can
     * support sending really large messages */
    let mut written = 0;
    while written < buffer.len() {
        match tokio::time::timeout(progress_timeout, out.write(&buffer[written..])).await {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write message data"));
            }
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
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::io::{duplex, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::*;

    struct EmptyMessage;

    impl MessageEncoding for EmptyMessage {
        const STATIC_SIZE: Option<usize> = Some(0);
        const MAX_SIZE: Option<usize> = Some(0);

        fn write_to<T: std::io::Write>(&self, _out: &mut T) -> std::io::Result<usize> {
            Ok(0)
        }

        fn read_from<T: std::io::Read>(_read: &mut T) -> std::io::Result<Self> {
            Ok(EmptyMessage)
        }
    }

    struct ZeroWrite;

    impl AsyncWrite for ZeroWrite {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_close_frame_returns_close() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&CLOSE_FRAME_SIZE.to_be_bytes()).await.unwrap();

        let mut buffer = Vec::new();
        let result =
            read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None, DEFAULT_MAX_FRAME_SIZE)
                .await
                .unwrap();

        assert!(matches!(result, ReadMessageResult::Close));
    }

    #[tokio::test]
    async fn read_keepalive_frame_returns_keepalive() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&KEEPALIVE_FRAME_SIZE.to_be_bytes()).await.unwrap();

        let mut buffer = Vec::new();
        let result =
            read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None, DEFAULT_MAX_FRAME_SIZE)
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

    #[tokio::test]
    async fn send_message_returns_write_zero_when_writer_makes_no_progress() {
        let mut writer = ZeroWrite;
        let mut buffer = Vec::new();

        let error = send_message(&mut buffer, &42u64, &mut writer, Duration::from_secs(1), DEFAULT_MAX_FRAME_SIZE)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn statically_sized_message_roundtrips() {
        let (mut writer, mut reader) = duplex(64);
        let mut send_buffer = Vec::new();
        let mut recv_buffer = Vec::new();

        send_message(&mut send_buffer, &42u64, &mut writer, Duration::from_secs(1), DEFAULT_MAX_FRAME_SIZE)
            .await
            .unwrap();

        assert_eq!(
            &send_buffer[..MESSAGE_HEADER_SIZE],
            &(<u64 as MessageEncoding>::STATIC_SIZE.unwrap() as MessageSizeHeader).to_be_bytes()
        );

        let result = read_message_opt::<u64, _>(
            &mut recv_buffer,
            &mut reader,
            Duration::from_secs(1),
            None,
            DEFAULT_MAX_FRAME_SIZE,
        )
        .await
        .unwrap();

        assert!(matches!(result, ReadMessageResult::Message(42)));
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_allocation() {
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&16u32.to_be_bytes()).await.unwrap();

        let mut buffer = Vec::new();
        let error = read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None, 8)
            .await
            .unwrap_err();

        assert!(matches!(error, ReadMessageError::FrameTooLarge { size: 16, max_size: 8 }));
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn send_message_rejects_oversized_frame() {
        let (mut writer, _reader) = duplex(64);
        let mut buffer = Vec::new();

        let error = send_message(&mut buffer, &42u64, &mut writer, Duration::from_secs(1), 7)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn send_message_rejects_zero_length_messages() {
        let (mut writer, _reader) = duplex(64);
        let mut buffer = Vec::new();

        let error =
            send_message(&mut buffer, &EmptyMessage, &mut writer, Duration::from_secs(1), DEFAULT_MAX_FRAME_SIZE)
                .await
                .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn read_message_error_classifies_disconnects() {
        let closed = ReadMessageError::Closed;
        let eof = ReadMessageError::SizeReadError(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
        let reset =
            ReadMessageError::MessageReadError(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"));

        assert!(closed.is_disconnect());
        assert!(eof.is_disconnect());
        assert!(reset.is_disconnect());
    }

    #[test]
    fn read_message_error_does_not_classify_protocol_failures_as_disconnects() {
        let timeout = ReadMessageError::MessageReadTimeout;
        let encoding = ReadMessageError::EncodingError(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid"));

        assert!(!timeout.is_disconnect());
        assert!(!encoding.is_disconnect());
        assert!(!ReadMessageError::FrameTooLarge { size: 2, max_size: 1 }.is_disconnect());
    }
}
