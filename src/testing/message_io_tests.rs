use std::time::Duration;

use tokio::io::{duplex, AsyncWriteExt};

use crate::message_io::{read_message, send_message, MessageSizeHeader};

const PROGRES_TIMEOUT: Duration = Duration::from_millis(100);

#[tokio::test]
async fn message_io_send_receive() {
    super::setup_logging();

    let (mut a, mut b) = duplex(2048);

    let mut buffer = Vec::new();
    send_message(&mut buffer, &"hello world".as_bytes().to_vec(), &mut a, PROGRES_TIMEOUT).await.unwrap();
    let received = read_message::<Vec<u8>, _>(&mut buffer, &mut b, PROGRES_TIMEOUT).await.unwrap().unwrap();

    let message = String::from_utf8(received).unwrap();
    assert_eq!("hello world", &message);
}

#[tokio::test]
async fn message_io_zero_send() {
    super::setup_logging();

    let (mut a, mut b) = duplex(2048);

    let mut buffer = Vec::new();
    send_message(&mut buffer, &"not zero".as_bytes().to_vec(), &mut a, PROGRES_TIMEOUT).await.unwrap();
    let received = read_message::<Vec<u8>, _>(&mut buffer, &mut b, PROGRES_TIMEOUT).await.unwrap().unwrap();

    let message = String::from_utf8(received).unwrap();
    assert_eq!("not zero", &message);

    a.write_all(&(0 as MessageSizeHeader).to_be_bytes()).await.unwrap();
    let received = read_message::<Vec<u8>, _>(&mut buffer, &mut b, PROGRES_TIMEOUT).await.unwrap();
    assert_eq!(received, None);
}
