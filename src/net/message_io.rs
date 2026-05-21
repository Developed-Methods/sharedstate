use std::{fmt::Debug, hint::black_box, time::Duration};

use message_encoding::MessageEncoding;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type MessageSizeHeader = u32;
const MESSAGE_HEADER_SIZE: usize = std::mem::size_of::<MessageSizeHeader>();

pub async fn read_message<M: MessageEncoding, R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
) -> Result<Option<M>, ReadMessageError> {
    read_message_opt(
        buffer,
        read,
        progress_timeout,
        None,
        MessageSizeHeader::MAX as usize,
    )
    .await
}

pub async fn read_message_opt<M: MessageEncoding, R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
    recv_timeout: Option<Duration>,
    max_message_size: usize,
) -> Result<Option<M>, ReadMessageError> {
    read_message_opt_with_oversized_id(
        buffer,
        read,
        progress_timeout,
        recv_timeout,
        max_message_size,
        None,
    )
    .await
}

pub async fn read_message_opt_with_oversized_id<M: MessageEncoding, R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
    recv_timeout: Option<Duration>,
    max_message_size: usize,
    oversized_message_id: Option<(u16, Option<usize>)>,
) -> Result<Option<M>, ReadMessageError> {
    let _assert = black_box(M::_ASSERT);

    if !read_message_to_vec(
        buffer,
        read,
        progress_timeout,
        recv_timeout,
        max_message_size,
        oversized_message_id,
    )
    .await?
    {
        return Ok(None);
    }

    let mut reader = &buffer[..];
    let msg = M::read_from(&mut reader).map_err(ReadMessageError::EncodingError)?;
    Ok(Some(msg))
}

pub async fn read_message_to_vec<R: AsyncRead + Unpin>(
    buffer: &mut Vec<u8>,
    read: &mut R,
    progress_timeout: Duration,
    recv_timeout: Option<Duration>,
    max_message_size: usize,
    oversized_message_id: Option<(u16, Option<usize>)>,
) -> Result<bool, ReadMessageError> {
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
    if max_message_size < msg_len {
        let Some((allowed_id, max_oversized_message_size)) = oversized_message_id else {
            return Err(ReadMessageError::MessageTooLarge(msg_len));
        };

        let message_id_size = std::mem::size_of::<u16>();
        if msg_len < message_id_size {
            return Err(ReadMessageError::MessageTooLarge(msg_len));
        }

        let mut msg_id_bytes = [0u8; std::mem::size_of::<u16>()];
        read_message_bytes(read, progress_timeout, &mut msg_id_bytes).await?;
        let msg_id = u16::from_be_bytes(msg_id_bytes);
        if msg_id != allowed_id || max_oversized_message_size.is_some_and(|max| max < msg_len) {
            return Err(ReadMessageError::MessageTooLarge(msg_len));
        }

        buffer.clear();
        buffer.resize(msg_len, 0u8);
        buffer[..message_id_size].copy_from_slice(&msg_id_bytes);
        read_message_bytes(read, progress_timeout, &mut buffer[message_id_size..]).await?;
        return Ok(true);
    }

    buffer.clear();
    buffer.resize(msg_len, 0u8);
    read_message_bytes(read, progress_timeout, buffer).await?;
    Ok(true)
}

async fn read_message_bytes<R: AsyncRead + Unpin>(
    read: &mut R,
    progress_timeout: Duration,
    buffer: &mut [u8],
) -> Result<(), ReadMessageError> {
    let mut bytes_read = 0;
    while bytes_read < buffer.len() {
        match tokio::time::timeout(progress_timeout, read.read(&mut buffer[bytes_read..])).await {
            Err(_) => return Err(ReadMessageError::MessageReadTimeout),
            Ok(Err(error)) => return Err(ReadMessageError::MessageReadError(error)),
            Ok(Ok(0)) => return Err(ReadMessageError::Closed),
            Ok(Ok(bytes)) => {
                bytes_read += bytes;
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum ReadMessageError {
    MessageReadTimeout,
    NextMessageTimeout(Duration),
    MessageTooLarge(usize),
    SizeReadError(std::io::Error),
    MessageReadError(std::io::Error),
    EncodingError(std::io::Error),
    Closed,
}

impl From<ReadMessageError> for std::io::Error {
    fn from(value: ReadMessageError) -> Self {
        match value {
            ReadMessageError::MessageReadTimeout | ReadMessageError::NextMessageTimeout(_) => {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout reading remaining message data",
                )
            }
            ReadMessageError::MessageTooLarge(len) => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message size {len} exceeds configured maximum"),
            ),
            ReadMessageError::Closed => std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "end of file reading message",
            ),
            ReadMessageError::SizeReadError(e)
            | ReadMessageError::MessageReadError(e)
            | ReadMessageError::EncodingError(e) => e,
        }
    }
}

pub async fn send_zero_message<W: AsyncWrite + Unpin>(out: &mut W) -> std::io::Result<()> {
    out.write_all(&(0 as MessageSizeHeader).to_be_bytes())
        .await?;
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
    assert_eq!(
        bytes_written + MESSAGE_HEADER_SIZE,
        buffer.len(),
        "M::write_to returned incorrect number of bytes"
    );

    if let Some(size) = M::STATIC_SIZE {
        assert_eq!(
            size, bytes_written,
            "M::STATIC_SIZE does not match M::write_to"
        );
    }
    buffer[..MESSAGE_HEADER_SIZE]
        .copy_from_slice(&(bytes_written as MessageSizeHeader).to_be_bytes());

    /* note: send in batches with timeout to ensure connection isn't hanging and we also can
     * support sending really large messages */
    let mut written = 0;
    while written < buffer.len() {
        match tokio::time::timeout(progress_timeout, out.write(&buffer[written..])).await {
            Ok(Ok(bytes)) => {
                written += bytes;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout writing data",
                ))
            }
        }
    }

    out.flush().await
}

pub fn require_version<T: std::io::prelude::Read>(
    version: u16,
    read: &mut T,
) -> std::io::Result<()> {
    let actual = u16::read_from(read)?;
    if version != actual {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid message version, expected: {} but got {}",
                version, actual
            ),
        ));
    }
    Ok(())
}

pub fn unknown_id_err(id: u16, name: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("unknown id for {}: {}", name, id),
    )
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

    use message_encoding::m_opt_sum;
    use tokio::io::duplex;

    use super::*;
    use crate::testing::state_tests::TestStateAction;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct FixedMessage {
        a: u64,
        b: u64,
    }

    impl MessageEncoding for FixedMessage {
        const STATIC_SIZE: Option<usize> = m_opt_sum(&[u64::STATIC_SIZE, u64::STATIC_SIZE]);
        const MAX_SIZE: Option<usize> = Self::STATIC_SIZE;

        fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
            Ok(self.a.write_to(out)? + self.b.write_to(out)?)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
            Ok(Self {
                a: MessageEncoding::read_from(read)?,
                b: MessageEncoding::read_from(read)?,
            })
        }
    }

    #[tokio::test]
    async fn message_io_test() {
        let (mut a, mut b) = duplex(2048);
        let mut buffer = Vec::with_capacity(1024);

        let send = TestStateAction::Add {
            slot: 0,
            value: 123,
        };
        send_message(&mut buffer, &send, &mut a, Duration::from_secs(1))
            .await
            .unwrap();

        let read = read_message_opt(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            1024,
        )
        .await
        .unwrap();
        assert_eq!(Some(send), read);
    }

    #[tokio::test]
    async fn message_io_fixed_size_message_test() {
        let (mut a, mut b) = duplex(2048);
        let mut buffer = Vec::with_capacity(1024);

        let send = FixedMessage { a: 7, b: 11 };
        send_message(&mut buffer, &send, &mut a, Duration::from_secs(1))
            .await
            .unwrap();

        let read = read_message_opt(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            1024,
        )
        .await
        .unwrap();
        assert_eq!(Some(send), read);
    }

    #[tokio::test]
    async fn message_io_rejects_oversized_message_test() {
        let (mut a, mut b) = duplex(2048);
        a.write_all(&(128u32).to_be_bytes()).await.unwrap();
        a.write_all(&[1u8; 128]).await.unwrap();

        let mut buffer = Vec::new();
        let err = read_message_opt::<FixedMessage, _>(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            64,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ReadMessageError::MessageTooLarge(128)));
    }

    #[tokio::test]
    async fn message_io_allows_unbounded_oversized_message_id_test() {
        let (mut a, mut b) = duplex(2048);
        a.write_all(&(128u32).to_be_bytes()).await.unwrap();
        a.write_all(&3u16.to_be_bytes()).await.unwrap();
        a.write_all(&[1u8; 126]).await.unwrap();

        let mut buffer = Vec::new();
        let read = read_message_to_vec(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            64,
            Some((3, None)),
        )
        .await
        .unwrap();

        assert!(read);
        assert_eq!(buffer.len(), 128);
    }

    #[tokio::test]
    async fn message_io_rejects_bounded_oversized_message_id_test() {
        let (mut a, mut b) = duplex(2048);
        a.write_all(&(128u32).to_be_bytes()).await.unwrap();
        a.write_all(&3u16.to_be_bytes()).await.unwrap();
        a.write_all(&[1u8; 126]).await.unwrap();

        let mut buffer = Vec::new();
        let err = read_message_to_vec(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            64,
            Some((3, Some(100))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ReadMessageError::MessageTooLarge(128)));
    }

    #[tokio::test]
    async fn message_io_rejects_wrong_oversized_message_id_test() {
        let (mut a, mut b) = duplex(2048);
        a.write_all(&(128u32).to_be_bytes()).await.unwrap();
        a.write_all(&4u16.to_be_bytes()).await.unwrap();
        a.write_all(&[1u8; 126]).await.unwrap();

        let mut buffer = Vec::new();
        let err = read_message_to_vec(
            &mut buffer,
            &mut b,
            Duration::from_secs(1),
            Some(Duration::from_secs(2)),
            64,
            Some((3, None)),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ReadMessageError::MessageTooLarge(128)));
    }
}
