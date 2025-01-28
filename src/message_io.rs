use std::{fmt::Debug, hint::black_box, time::Duration};

use message_encoding::MessageEncoding;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type MessageSizeHeader = u32;
const MESSAGE_HEADER_SIZE: usize = std::mem::size_of::<MessageSizeHeader>();

pub async fn read_message<M: MessageEncoding, R: AsyncRead + Unpin>(buffer: &mut Vec<u8>, read: &mut R, progress_timeout: Duration) -> Result<Option<M>, ReadMessageError> {
    read_message_opt(buffer, read, progress_timeout, None).await
}

pub async fn read_message_opt<M: MessageEncoding, R: AsyncRead + Unpin>(buffer: &mut Vec<u8>, read: &mut R, progress_timeout: Duration, recv_timeout: Option<Duration>) -> Result<Option<M>, ReadMessageError> {
    let _assert = black_box(M::_ASSERT);

    if !read_message_to_vec(buffer, read, progress_timeout, recv_timeout).await? {
        return Ok(None);
    }

    let mut reader = &buffer[..];
    let msg = M::read_from(&mut reader).map_err(ReadMessageError::EncodingError)?;
    Ok(Some(msg))
}

pub async fn read_message_to_vec<R: AsyncRead + Unpin>(buffer: &mut Vec<u8>, read: &mut R, progress_timeout: Duration, recv_timeout: Option<Duration>) -> Result<bool, ReadMessageError> {
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

    let msg_len = MessageSizeHeader::from_be_bytes(len_bytes) as usize;
    if msg_len == 0 {
        return Ok(false);
    }

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

    Ok(true)
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
            ReadMessageError::MessageReadTimeout | ReadMessageError::NextMessageTimeout(_) => std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout reading remaining message data"),
            ReadMessageError::Closed => std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "end of file reading message"),
            ReadMessageError::SizeReadError(e) | ReadMessageError::MessageReadError(e) | ReadMessageError::EncodingError(e) => e,
        }
    }
}

pub async fn send_zero_message<W: AsyncWrite + Unpin>(out: &mut W) -> std::io::Result<()> {
    out.write_all(&(0 as MessageSizeHeader).to_be_bytes()).await?;
    out.flush().await
}

pub async fn send_message<M: MessageEncoding, W: AsyncWrite + Unpin>(buffer: &mut Vec<u8>, message: &M, out: &mut W, progress_timeout: Duration) -> std::io::Result<()> {
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

pub fn require_version<T: std::io::prelude::Read>(version: u16, read: &mut T) -> std::io::Result<()> {
    let actual = u16::read_from(read)?;
    if version != actual {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid message version, expected: {} but got {}", version, actual)));
    }
    Ok(())
}

pub fn unknown_id_err(id: u16, name: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown id for {}: {}", name, id))
}

pub const fn m_max_opt_list(samples: &'static [Option<usize>]) -> Option<usize> {
    const fn scan(max: usize, idx: usize, samples: &'static [Option<usize>]) -> Option<usize> {
        if idx == samples.len() {
            return Some(max);
        }

        match samples[idx] {
            None => None,
            Some(next) if next < max => scan(max, idx + 1, samples),
            Some(larger) => scan(larger, idx + 1, samples),
        }
    }

    if samples.is_empty() {
        panic!("m_max_opt_list provided 0 samples");
    }

    match samples[0] {
        None => None,
        Some(max) => scan(max, 1, samples),
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::io::duplex;

    use super::*;
    use crate::testing::state_tests::TestStateAction;

    #[tokio::test]
    async fn message_io_test() {
        let (mut a, mut b) = duplex(2048);
        let mut buffer = Vec::with_capacity(1024);

        let send = TestStateAction::Add { slot: 0, value: 123 };
        send_message(&mut buffer, &send, &mut a, Duration::from_secs(1)).await.unwrap();

        let read = read_message_opt(&mut buffer, &mut b, Duration::from_secs(1), Some(Duration::from_secs(2))).await.unwrap();
        assert_eq!(Some(send), read);
    }
}
