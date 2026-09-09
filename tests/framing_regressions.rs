use sharedstate::protocol::framing::{ReadMessageResult, read_message_opt, send_message};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn t19_static_frame_roundtrips_without_consuming_next_frame() {
    let (mut writer, mut reader) = tokio::io::duplex(128);
    let mut buffer = Vec::new();
    send_message(&mut buffer, &42u64, &mut writer, Duration::from_secs(1))
        .await
        .unwrap();
    send_message(&mut buffer, &43u64, &mut writer, Duration::from_secs(1))
        .await
        .unwrap();
    for expected in [42, 43] {
        let result = read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None)
            .await
            .unwrap();
        assert!(matches!(result, ReadMessageResult::Message(value) if value == expected));
    }
}

#[tokio::test]
async fn t18_reject_trailing_payload() {
    let (mut writer, mut reader) = tokio::io::duplex(128);
    let mut buffer = Vec::new();
    send_message(&mut buffer, &42u64, &mut writer, Duration::from_secs(1))
        .await
        .unwrap();
    let mut original = vec![0; buffer.len()];
    reader.read_exact(&mut original).await.unwrap();
    let payload_size = original.len() - 4;
    original[..4].copy_from_slice(&((payload_size + 1) as u32).to_be_bytes());
    original.push(0);
    writer.write_all(&original).await.unwrap();
    let result = read_message_opt::<u64, _>(&mut buffer, &mut reader, Duration::from_secs(1), None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn t18_oversize_header_fails_before_allocation() {
    let mut input = &((sharedstate::protocol::framing::MAX_FRAME_BYTES + 1) as u32).to_be_bytes()[..];
    let mut buffer = Vec::new();
    assert!(
        read_message_opt::<u64, _>(&mut buffer, &mut input, Duration::from_secs(1), None)
            .await
            .is_err()
    );
    assert_eq!(buffer.capacity(), 0);
}

#[tokio::test]
async fn t19_dynamic_frame_roundtrips() {
    let (mut writer, mut reader) = tokio::io::duplex(128);
    let value = String::from("committed state");
    let mut buffer = Vec::new();
    send_message(&mut buffer, &value, &mut writer, Duration::from_secs(1))
        .await
        .unwrap();
    let result = read_message_opt::<String, _>(&mut buffer, &mut reader, Duration::from_secs(1), None)
        .await
        .unwrap();
    assert!(matches!(result, ReadMessageResult::Message(decoded) if decoded == value));
}
