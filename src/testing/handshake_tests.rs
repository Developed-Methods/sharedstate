use std::borrow::Cow;
use super::setup_logging;

use message_encoding::MessageEncoding;

use crate::{handshake::{ClientConnected, ClientConnectionAccepted, ClientConnectionAcceptedDirect, HandshakeClient, HandshakeServer, RecoveryAttempt, ServerClientReady, ServerNewClient}, io::{SyncConnection, SyncIO}, recoverable_state::RecoverableState};
use crate::state::DeterministicState;

struct SyncTestIo;

impl SyncIO for SyncTestIo {
    type Read = tokio::io::ReadHalf<tokio::io::DuplexStream>;
    type Write = tokio::io::WriteHalf<tokio::io::DuplexStream>;
    type Address = u64;

    async fn connect(&self, _remote: &Self::Address) -> std::io::Result<SyncConnection<Self>> {
        todo!()
    }

    async fn next_client(&self) -> std::io::Result<SyncConnection<Self>> {
        todo!()
    }
}

#[derive(Clone, Debug)]
struct TestAddState {
    id: u64,
    sequence: u64,
    number: i64,
}

impl MessageEncoding for TestAddState {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.id.write_to(out)?;
        sum += self.sequence.write_to(out)?;
        sum += (self.number as u64).write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(TestAddState {
            id: MessageEncoding::read_from(read)?,
            sequence: MessageEncoding::read_from(read)?,
            number: u64::read_from(read)? as i64,
        })
    }
}

impl DeterministicState for TestAddState {
    type Action = i64;

    fn id(&self) -> u64 {
        self.id
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn update(&mut self, action: &Self::Action) {
        self.sequence += 1;
        self.number += *action;
    }
}

#[tokio::test]
async fn handshake_fresh_connect_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.connect().await.unwrap();
        let ClientConnectionAccepted::FreshConnection(conn) = connect else { panic!() };
        conn.load_state::<TestAddState>().await.unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.accept().await.unwrap();
        let ServerNewClient::Fresh(client) = accept else { panic!() };

        client.send_state(&RecoverableState::first_generation(TestAddState {
            id: 1,
            sequence: 1,
            number: 100,
        })).await
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let server: ServerClientReady<SyncTestIo> = accept.await.unwrap().unwrap();

    assert_eq!(client.state.id(), server.state_id);
    assert_eq!(client.state.sequence(), server.state_sequence);
    assert_eq!(client.state.state().number, 100);
}

#[tokio::test]
async fn handshake_reconnect_recover_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.reconnect(RecoveryAttempt {
            state_id: 20,
            state_generation: 1,
            state_sequence: 100,
        }).await.unwrap();

        let ClientConnectionAccepted::RecoveringConnection(conn) = connect else { panic!() };

        conn.recover(RecoverableState::first_generation(TestAddState {
            id: 20,
            sequence: 100,
            number: 999,
        })).unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.accept().await.unwrap();
        let ServerNewClient::WithData(client) = accept else { panic!() };
        client.accept_recover(vec![]).await.unwrap()
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let server: ServerClientReady<SyncTestIo> = accept.await.unwrap();

    assert_eq!(client.state.id(), server.state_id);
    assert_eq!(client.state.sequence(), server.state_sequence);
    assert_eq!(client.state.state().number, 999);
}

#[tokio::test]
async fn handshake_reconnect_request_state_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.reconnect(RecoveryAttempt {
            state_id: 20,
            state_generation: 1,
            state_sequence: 100,
        }).await.unwrap();

        let ClientConnectionAccepted::RequestingState(conn) = connect else { panic!() };

        conn.provide_state(Cow::Owned(RecoverableState::first_generation(TestAddState {
            id: 20,
            sequence: 100,
            number: 999,
        }))).await.unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.accept().await.unwrap();
        let ServerNewClient::WithData(client) = accept else { panic!() };
        client.request_state().await.unwrap().accept().await.unwrap()
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let (server, server_state): (ServerClientReady<SyncTestIo>, RecoverableState<TestAddState>) = accept.await.unwrap().unbundle();

    assert_eq!(client.state.id(), server_state.id());
    assert_eq!(client.state.sequence(), server_state.sequence());
    assert_eq!(client.state.state().number, server_state.state().number);
}

#[tokio::test]
async fn handshake_reconnect_request_state_reject_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.reconnect(RecoveryAttempt {
            state_id: 20,
            state_generation: 1,
            state_sequence: 100,
        }).await.unwrap();

        let ClientConnectionAccepted::RequestingState(conn) = connect else { panic!() };

        conn.provide_state(Cow::Owned(RecoverableState::first_generation(TestAddState {
            id: 20,
            sequence: 100,
            number: 999,
        }))).await.unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.accept().await.unwrap();
        let ServerNewClient::WithData(client) = accept else { panic!() };

        client
            .request_state::<TestAddState>().await.unwrap()
            .reject().await.unwrap()
            .accept_client_with_state(&RecoverableState::first_generation(TestAddState {
                id: 20,
                sequence: 200,
                number: 3000,
            })).await.unwrap()
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let server: ServerClientReady<SyncTestIo> = accept.await.unwrap();

    assert_eq!(client.state.id(), server.state_id);
    assert_eq!(client.state.sequence(), server.state_sequence);
    assert_eq!(client.state.id(), 20);
    assert_eq!(client.state.sequence(), 200);
}

#[tokio::test]
async fn handshake_relay_connect_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.connect().await.unwrap();
        let ClientConnectionAccepted::RelayOption(relay) = connect else { panic!() };
        assert_eq!(relay.leader_address(), 99412);

        let ClientConnectionAcceptedDirect::FreshConnection(conn) = relay.accept_relay().await.unwrap() else { panic!() };
        conn.load_state::<TestAddState>().await.unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.relay(99412).await.unwrap();
        let Some(ServerNewClient::Fresh(client)) = accept else { panic!() };

        client.send_state(&RecoverableState::first_generation(TestAddState {
            id: 1,
            sequence: 1,
            number: 100,
        })).await
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let server: ServerClientReady<SyncTestIo> = accept.await.unwrap().unwrap();

    assert_eq!(client.state.id(), server.state_id);
    assert_eq!(client.state.sequence(), server.state_sequence);
    assert_eq!(client.state.state().number, 100);
}

#[tokio::test]
async fn handshake_reconnect_request_state_recover_test() {
    setup_logging();

    let (a, b) = tokio::io::duplex(2048);
    let (a_read, a_write) = tokio::io::split(a);
    let (b_read, b_write) = tokio::io::split(b);

    let client = HandshakeClient::new(SyncConnection::<SyncTestIo> {
        remote: 1,
        write: a_write,
        read: a_read,
    });

    let server = HandshakeServer::new(SyncConnection::<SyncTestIo> {
        remote: 2,
        write: b_write,
        read: b_read,
    });

    let connect = tokio::spawn(async move {
        let connect = client.reconnect(RecoveryAttempt {
            state_id: 20,
            state_generation: 1,
            state_sequence: 100,
        }).await.unwrap();
        let ClientConnectionAccepted::RequestingState(conn) = connect else { panic!() };

        conn.provide_state(Cow::Owned(RecoverableState::first_generation(TestAddState {
            id: 20,
            sequence: 100,
            number: 999,
        }))).await.unwrap()
    });

    let accept = tokio::spawn(async move {
        let accept = server.accept().await.unwrap();
        let ServerNewClient::WithData(client) = accept else { panic!() };

        client
            .request_state::<TestAddState>().await.unwrap()
            .reject().await.unwrap()
            .accept_recover(vec![]).await.unwrap()
    });

    let client: ClientConnected<SyncTestIo, TestAddState> = connect.await.unwrap();
    let server: ServerClientReady<SyncTestIo> = accept.await.unwrap();

    assert_eq!(client.state.id(), server.state_id);
    assert_eq!(client.state.sequence(), server.state_sequence);
}
