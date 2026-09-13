use std::sync::Arc;

use message_encoding::MessageEncoding;
use sequenced_broadcast::SequencedReceiver;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
    task::JoinHandle,
};

use crate::{
    cluster::node_state::{NodeState, PeerState, SyncStatus},
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
};

pub struct RpcServer<A: SyncIOAddress, D: DeterministicState> {
    state: Arc<NodeState<A, D>>,
    actions_tx: Sender<(A, D::Action)>,
}

impl<A: SyncIOAddress, D: DeterministicState> RpcServer<A, D> {
    pub fn new(state: Arc<NodeState<A, D>>, actions_tx: Sender<(A, D::Action)>) -> Self {
        RpcServer { state, actions_tx }
    }

    pub async fn handle(&self, peer_addr: A, request: SyncRequest<A, D>) -> ResponseOrFeed<A, D> {
        if !self.state.note_known_peer_activity(peer_addr).await {
            tracing::debug!(?peer_addr, "learned about new peer from inbound connection");
        }

        let resp = match request {
            SyncRequest::ProtocolVersion(_) => SyncResponse::UnexpectedRequest,
            SyncRequest::MyAddress(_) => SyncResponse::UnexpectedRequest,

            SyncRequest::Action { source, action } => {
                if self.actions_tx.send((source, action)).await.is_ok() {
                    SyncResponse::Ok
                } else {
                    SyncResponse::FailedToQueueAction { source }
                }
            }
            SyncRequest::LeaderInformation(info) => {
                let mut lock = self.state.peers.lock().await;
                let peer = lock.entry(peer_addr).or_insert_with(|| PeerState::empty(peer_addr));
                peer.can_lead = Some(info.can_lead);
                peer.leader_info = Some(info);

                SyncResponse::Ok
            }
            SyncRequest::SubscribeRecovery(details) => match self.live_source(peer_addr) {
                Some(source) => match self.state.state.subscribe(details).await {
                    Ok(feed) => return ResponseOrFeed::Subscription { feed, source },
                    Err(error) => {
                        tracing::warn!(?error, "client recovery failed");
                        SyncResponse::RecoveryFailed
                    }
                },
                None => SyncResponse::NotSynced,
            },
            SyncRequest::SubscribeFresh => match self.live_source(peer_addr) {
                Some(source) => {
                    let (state, feed) = self.state.state.subscribe_fresh().await;
                    return ResponseOrFeed::FreshState { state, feed, source };
                }
                None => SyncResponse::NotSynced,
            },
            SyncRequest::LeaderQuery => {
                let leader_state = self.state.leader_state.lock().await.clone();
                SyncResponse::LeaderState(leader_state)
            }
            SyncRequest::SharePeers(shared_peers) => {
                self.state.merge_peer_details(shared_peers).await;
                let share_peer_details = self.state.known_peer_details().await;
                SyncResponse::Peers(share_peer_details)
            }
        };

        ResponseOrFeed::Response(resp)
    }

    /// A watch on our sync status, only if we are currently a live source
    /// (leading or fed directly by the leader). Subscribing before checking
    /// means a status change right after the check still shows up as a
    /// change on the returned watch.
    fn live_source(&self, peer_addr: A) -> Option<watch::Receiver<SyncStatus<A>>> {
        let source = self.state.sync_status.subscribe();
        let status = *source.borrow();
        if status.can_relay() {
            Some(source)
        } else {
            tracing::info!(?peer_addr, ?status, "refusing subscription, not a live source for the state");
            None
        }
    }
}

impl<A, D> RpcServer<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn start_listener<I>(self: Arc<Self>, io: Arc<I>, settings: NetIoSettings) -> JoinHandle<()>
    where
        I: SyncIOListener<Address = A>,
    {
        tokio::spawn(async move {
            loop {
                match io.next_client().await {
                    Ok(conn) => {
                        let server = self.clone();
                        let settings = settings.clone();
                        tokio::spawn(async move {
                            server.handle_client(conn, settings).await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!(?error, "failed to accept client");
                        continue;
                    }
                }
            }
        })
    }

    pub async fn handle_client<I>(self: Arc<Self>, conn: SyncConnection<I>, settings: NetIoSettings)
    where
        I: SyncIO<Address = A>,
    {
        let (transport_addr, write, mut read) = conn.server_channels::<D>(settings.clone());
        let Some(peer_addr) = handshake_client(&write, &mut read, settings.message_timeout).await else {
            tracing::debug!(?transport_addr, "rpc client handshake failed");
            return;
        };

        while let Some(request) = read.recv().await {
            match self.handle(peer_addr, request).await {
                ResponseOrFeed::Response(response) => {
                    if write.send(response).await.is_err() {
                        break;
                    }
                }
                ResponseOrFeed::FreshState { state, feed, source } => {
                    if write.send(SyncResponse::FreshState(state)).await.is_err() {
                        break;
                    }
                    stream_feed(peer_addr, write, &mut read, feed, source).await;
                    break;
                }
                ResponseOrFeed::Subscription { feed, source } => {
                    if write.send(SyncResponse::Accepted(feed.next_seq())).await.is_err() {
                        break;
                    }
                    stream_feed(peer_addr, write, &mut read, feed, source).await;
                    break;
                }
            }
        }
    }
}

async fn handshake_client<A, D>(
    write: &Sender<SyncResponse<A, D>>,
    read: &mut Receiver<SyncRequest<A, D>>,
    timeout: std::time::Duration,
) -> Option<A>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let version = tokio::time::timeout(timeout, read.recv()).await.ok().flatten()?;
    match version {
        SyncRequest::ProtocolVersion(PROTOCOL_VERSION) => {
            write.send(SyncResponse::Ok).await.ok()?;
        }
        _ => {
            let _ = write.send(SyncResponse::UnexpectedRequest).await;
            return None;
        }
    }

    let address = tokio::time::timeout(timeout, read.recv()).await.ok().flatten()?;
    match address {
        SyncRequest::MyAddress(address) => {
            write.send(SyncResponse::Ok).await.ok()?;
            Some(address)
        }
        _ => {
            let _ = write.send(SyncResponse::UnexpectedRequest).await;
            None
        }
    }
}

/// Streams the action feed to a subscriber until the feed closes, the
/// subscriber goes away, or this node stops being a live source. The last
/// case matters for relayed subscribers: once our own upstream is gone they
/// would otherwise sit on a silent feed instead of finding a live source.
async fn stream_feed<A, D>(
    peer_addr: A,
    write: Sender<SyncResponse<A, D>>,
    read: &mut Receiver<SyncRequest<A, D>>,
    mut feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    mut source: watch::Receiver<SyncStatus<A>>,
) where
    A: SyncIOAddress,
    D: DeterministicState,
{
    loop {
        tokio::select! {
            received = feed.recv() => match received {
                Ok((seq, action)) => {
                    if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::debug!(?peer_addr, ?error, "rpc subscription feed closed");
                    let _ = write.send(SyncResponse::ActionStreamClosed).await;
                    break;
                }
            },
            changed = source.changed() => {
                let status = changed.map(|()| *source.borrow_and_update());
                match status {
                    Ok(status) if status.can_relay() => continue,
                    Ok(status) => {
                        tracing::info!(?peer_addr, ?status, "no longer a live source, closing subscription");
                    }
                    Err(_) => {
                        tracing::info!(?peer_addr, "sync status watch closed, closing subscription");
                    }
                }
                let _ = write.send(SyncResponse::ActionStreamClosed).await;
                break;
            }
            request = read.recv() => match request {
                Some(request) => {
                    tracing::debug!(?peer_addr, ?request, "ignoring request on subscription stream");
                }
                None => {
                    tracing::debug!(?peer_addr, "subscriber connection closed");
                    break;
                }
            },
        }
    }
}

pub enum ResponseOrFeed<A: SyncIOAddress, D: DeterministicState> {
    Response(SyncResponse<A, D>),
    FreshState {
        state: RecoverableState<D>,
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
        source: watch::Receiver<SyncStatus<A>>,
    },
    Subscription {
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
        source: watch::Receiver<SyncStatus<A>>,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::{mpsc, Mutex};

    use super::*;
    use crate::{
        protocol::messages::{ElectionTerm, LeaderMode, LeaderState},
        state::{recoverable_state::RecoverableStateDetails, subscribable_state::SubscribableState},
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    impl DeterministicState for TestState {
        type Action = u64;
        type AuthorityAction = u64;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    fn server(status: SyncStatus<u64>) -> (RpcServer<u64, TestState>, Arc<NodeState<u64, TestState>>) {
        let state = Arc::new(NodeState {
            my_address: 1,
            can_lead: true,
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(
                RecoverableState::new(1, TestState(0)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_state: Mutex::new(LeaderState {
                term: ElectionTerm::from_term(0),
                mode: LeaderMode::Leading,
            }),
            sync_status: watch::Sender::new(status),
        });
        let (actions_tx, _actions_rx) = mpsc::channel(4);
        let actions_tx: Sender<(u64, u64)> = actions_tx;
        (RpcServer::new(state.clone(), actions_tx), state)
    }

    fn matching_details(state: &NodeState<u64, TestState>) -> RecoverableStateDetails {
        state.state.create_handle().recover_details()
    }

    #[tokio::test]
    async fn subscriptions_are_refused_unless_the_node_is_a_live_source() {
        for status in [
            SyncStatus::NotSynced,
            SyncStatus::Relayed { relay: 2, leader: 3 },
        ] {
            let (server, state) = server(status);

            let response = server.handle(9, SyncRequest::SubscribeRecovery(matching_details(&state))).await;
            assert!(
                matches!(response, ResponseOrFeed::Response(SyncResponse::NotSynced)),
                "recovery subscription must be refused while {status:?}"
            );

            let response = server.handle(9, SyncRequest::SubscribeFresh).await;
            assert!(
                matches!(response, ResponseOrFeed::Response(SyncResponse::NotSynced)),
                "fresh subscription must be refused while {status:?}"
            );
        }

        for status in [SyncStatus::Leading, SyncStatus::Direct { leader: 3 }] {
            let (server, state) = server(status);

            let response = server.handle(9, SyncRequest::SubscribeRecovery(matching_details(&state))).await;
            assert!(
                matches!(response, ResponseOrFeed::Subscription { .. }),
                "recovery subscription must be served while {status:?}"
            );

            let response = server.handle(9, SyncRequest::SubscribeFresh).await;
            assert!(
                matches!(response, ResponseOrFeed::FreshState { .. }),
                "fresh subscription must be served while {status:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn feed_closes_when_the_node_stops_being_a_live_source() {
        let (server, state) = server(SyncStatus::Direct { leader: 3 });

        let ResponseOrFeed::Subscription { feed, source } =
            server.handle(9, SyncRequest::SubscribeRecovery(matching_details(&state))).await
        else {
            panic!("expected a subscription");
        };

        let (write, mut subscriber) = mpsc::channel(8);
        let (_requests, mut read) = mpsc::channel::<SyncRequest<u64, TestState>>(1);
        let streaming = tokio::spawn(async move { stream_feed(9, write, &mut read, feed, source).await });

        state
            .state
            .update(std::iter::once(RecoverableStateAction::StateAction { action: 5 }))
            .await;
        assert!(matches!(
            subscriber.recv().await,
            Some(SyncResponse::AuthorityAction(1, RecoverableStateAction::StateAction { action: 5 }))
        ));

        /* staying a live source (now leading) keeps the feed open */
        state.sync_status.send_replace(SyncStatus::Leading);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!streaming.is_finished(), "feed must stay open across live-source transitions");

        /* losing the upstream must close the feed rather than leave the
         * subscriber on a silent stream */
        state.sync_status.send_replace(SyncStatus::NotSynced);
        assert!(matches!(subscriber.recv().await, Some(SyncResponse::ActionStreamClosed)));
        tokio::time::timeout(Duration::from_secs(1), streaming)
            .await
            .expect("stream task must finish after closing the feed")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn feed_ends_when_the_subscriber_disconnects() {
        let (server, _state) = server(SyncStatus::Leading);

        let ResponseOrFeed::FreshState { feed, source, .. } = server.handle(9, SyncRequest::SubscribeFresh).await
        else {
            panic!("expected a fresh subscription");
        };

        let (write, _subscriber) = mpsc::channel(8);
        let (requests, mut read) = mpsc::channel::<SyncRequest<u64, TestState>>(1);
        let streaming = tokio::spawn(async move { stream_feed(9, write, &mut read, feed, source).await });

        /* nothing flows on an idle feed; only the subscriber's read side
         * closing tells us it went away */
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!streaming.is_finished());

        drop(requests);
        tokio::time::timeout(Duration::from_secs(1), streaming)
            .await
            .expect("stream task must finish once the subscriber disconnects")
            .unwrap();
    }
}
