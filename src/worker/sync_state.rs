use std::{collections::VecDeque, sync::Arc, time::Duration};

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcast, SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, error::TryRecvError, Receiver, Sender};
use tracing::Instrument;

use crate::{handshake::{ConnectedToLeader, ConnectionEstablished, HandshakeClient, HandshakeServer, LeaderChannels, NewClient, RecoveringConnection, WantsRecovery, WantsState}, io::{SyncConnection, SyncIO}, recoverable_state::{RecoverableState, RecoverableStateAction}, state::{self, DeterministicState, LeaderUpdater, SharedState, SharedStateUpdater}, utils::{LogHelper, PanicHelper, TimeoutPanicHelper}};

use super::{message_relay::{MessageRelay, MpscMessages, SequencedMessages}, state_updater::{StateAndReceiver, StateUpdater}};

pub struct SyncState<I: SyncIO, D: DeterministicState> {
    shared: SharedState<RecoverableState<D>>,
    event_tx: Sender<Event<I, D>>,
    client_actions_tx: Sender<D::Action>,
}

enum Event<I: SyncIO, D: DeterministicState> {
    NewLeader(I::Address),
    AttemptConnect(VecDeque<I::Address>),
    FreshConnection(ConnectedToLeader<I, D>, RecoverableState<D>),
    RecoveringConnection(RecoveringConnection<I>),
    NewClient(SyncConnection<I>),
    NewFreshClient(WantsState<I>),
    NewClientWantsRecovery(WantsRecovery<I>),
}

struct SyncStateWorker<I: SyncIO, D: DeterministicState> {
    status: Status<D>,
    io: Arc<I>,

    local: I::Address,
    leader: I::Address,
    alt: Vec<I::Address>,

    shared: SharedState<RecoverableState<D>>,
    event_tx: Sender<Event<I, D>>,
    event_rx: Receiver<Event<I, D>>,
    updater: StateUpdater<RecoverableState<D>>,

    actions_tx: Sender<D::Action>,
    action_relay: MessageRelay<MpscMessages<D::Action>>,
    authority_relay: MessageRelay<SequencedMessages<RecoverableStateAction<D::AuthorityAction>>>,

    // lead_actions_rx: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,

    broadcast_settings: SequencedBroadcastSettings,
    broadcast: SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,
    // broadcast_tx: SequencedSender<RecoverableStateAction<D::AuthorityAction>>,

    // client_actions_tx: Sender<D::Action>,
    // client_actions_rx: Receiver<D::Action>,
}

enum Status<D: DeterministicState> {
    Leading(Sender<RecoverableStateAction<D::Action>>),
    Following,
    Disconnected(SequencedSender<RecoverableStateAction<D::AuthorityAction>>),
    Connecting,
}

impl<I: SyncIO, D: DeterministicState> SyncState<I, D> where D::AuthorityAction: Clone {
    pub fn new(
        io: I,
        local: I::Address,
        leader: I::Address,
        alt_leaders: Vec<I::Address>,
        state: RecoverableState<D>,
        broadcast_settings: SequencedBroadcastSettings,
    ) -> Self {
        // let (broadcast, broadcast_tx) = SequencedBroadcast::new(state.sequence(), broadcast_settings.clone());
        // let (shared, updater) = SharedState::new(state);

        // let (event_tx, event_rx) = channel(32);
        // let (client_actions_tx, client_actions_rx) = channel(2048);

        todo!()

        // let (status, leader_rx) = if local == leader {
        //     let (actions_tx, actions_rx) = channel(1024);
        //     let (updater, leader_rx) = StateUpdater::leader(actions_rx, updater.into_lead());

        //     (
        //         Status::Leading(actions_tx),
        //         leader_rx,
        //     )
        // } else {
        //     let (leader_tx, leader_rx) = channel(1024);
        //     SequencedReceiver::new(state.sequence(), receiver)
        //     StateUpdater::follower(rx, updater)
        // };

        // tokio::spawn(SyncStateWorker {
        //     status,
        //     io,
        //     local,
        //     leader,
        //     shared: shared.clone(),
        //     event_tx: event_tx.clone(),
        //     event_rx,
        //     updater,
        //     lead_actions_rx,
        //     client_actions_tx: client_actions_tx.clone(),
        //     client_actions_rx,
        // }.start());

        // Self {
        //     shared,
        //     event_tx,
        //     client_actions_tx,
        // }
    }

    pub async fn set_leader(&self, leader: I::Address) {
        self.event_tx.send(Event::NewLeader(leader)).await.panic("worker closed");
    }
}

impl<I: SyncIO, D: DeterministicState + MessageEncoding> SyncStateWorker<I, D>
    where D::Action: MessageEncoding,
          D::AuthorityAction: MessageEncoding + Clone
 {
    async fn start(mut self) {
        loop {
            let Some(event) = self.event_rx.recv().await else { panic!("should not be possible, have local reference") };

            match event {
                Event::NewLeader(leader) => {
                    // if leader == self.leader {
                    //     continue;
                    // }

                    // tracing::info!("leader updated, from: {:?} to: {:?}", self.leader, leader);
                    // self.leader = leader;

                    // if self.leader == self.local {
                    // }
                }
                Event::AttemptConnect(mut options) => {
                    if self.leader == self.local {
                        tracing::info!("attempting connect but currently leader");
                        continue;
                    }

                    if options.is_empty() {
                        options.push_back(self.leader.clone());
                        options.extend(self.alt.iter().cloned());
                    }

                    let event_tx = self.event_tx.clone();
                    let addr = options.pop_front().unwrap();
                    let io = self.io.clone();

                    let state_details = self.shared.read().details().clone();

                    let span = tracing::info_span!("AttemptConnect", ?addr);
                    tokio::spawn(async move {
                        let Ok(conn) = io.connect(&addr).await.err_log("failed to connect to leader") else {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            event_tx.send(Event::AttemptConnect(options)).await.panic("worker crashed");
                            return;
                        };

                        let handshake = HandshakeClient::new(conn);

                        let established_res = if 0 < state_details.state_sequence {
                            handshake.reconnect(state_details).await.err_log("failed to reconnect")
                        } else {
                            handshake.connect().await.err_log("failed to connect")
                        };

                        let Ok(established) = established_res else {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            event_tx.send(Event::AttemptConnect(options)).await.panic("wroker crashed");
                            return;
                        };

                        match established {
                            ConnectionEstablished::FreshConnection(fresh) => {
                                let Ok((connection, state)) = fresh.load_state::<D>().await.err_log("failed to load state from fresh connection") else {
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                    event_tx.send(Event::AttemptConnect(options)).await.panic("worker crashed");
                                    return;
                                };

                                event_tx.send(Event::FreshConnection(connection, state)).await.panic("worker crashed");
                            }
                            ConnectionEstablished::RecoveringConnection(recovering) => {
                                event_tx.send(Event::RecoveringConnection(recovering)).await.panic("worker crashed");
                            }
                        }
                    }.instrument(span));
                }
                Event::FreshConnection(connection, state) => {
                    if self.leader == self.local {
                        tracing::info!("drop new leader connection as local now leader");
                        continue;
                    }

                    let (_, LeaderChannels {
                        action_tx,
                        authority_rx
                    }) = connection.start_io_workers();

                    let state_and_rx = StateAndReceiver::create(state, authority_rx)
                        .panic("fresh connection has invalid sequence");

                    let authority_rx = self.updater
                        .update_internals(false)
                        .timeout(Duration::from_millis(100), "update internals").await
                        .reset_follow(state_and_rx);

                    self.action_relay
                        .replace_output(action_tx)
                        .timeout(Duration::from_millis(100), "timeout replacing output").await
                        .panic("failed to replace output");

                    /* replace broadcast and drop all subscribers */
                    let (broadcast, broadcast_tx) = SequencedBroadcast::new(authority_rx.next_seq(), self.broadcast_settings.clone());
                    self.broadcast = broadcast;
                    self.authority_relay = MessageRelay::new(authority_rx, broadcast_tx);
                }
                Event::RecoveringConnection(recovering) => {
                    if self.leader == self.local {
                        tracing::info!("drop new leader connection as local now leader");
                        continue;
                    }

                    let (leader_tx, leader_rx) = channel(1024);
                    let leader_tx = SequencedSender::new(recovering.sequence(), leader_tx);
                    let leader_rx = SequencedReceiver::new(recovering.sequence(), leader_rx);

                    let connection = self.updater
                        .update_internals(false)
                        .timeout(Duration::from_millis(100), "update internals").await
                        .setup_follow(leader_rx, |state| recovering.recover(state))
                        .panic("recovering with invalid sequence")
                        .panic("state updated during connection, sequence change");

                    let (_, action_tx) = connection.start_io_workers2(leader_tx).panic("invalid sequence");

                    self.action_relay
                        .replace_output(action_tx)
                        .timeout(Duration::from_millis(100), "timeout replacing output").await
                        .panic("failed to replace output");
                }
                Event::NewClient(conn) => {
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        let Ok(accepted) = HandshakeServer::new(conn)
                            .accept().await
                            .err_log("failed to accept new client") else { return; };

                        match accepted {
                            NewClient::Fresh(fresh) => {
                                event_tx.send(Event::NewFreshClient(fresh)).await.panic("worker crashed");
                            }
                            NewClient::WithData(with_data) => {
                                event_tx.send(Event::NewClientWantsRecovery(with_data)).await.panic("worker crashed");
                            }
                        }
                    });
                }
                Event::NewFreshClient(fresh) => {
                    let state = self.shared.read().clone();

                    let Some(authority_rx) = self.broadcast.add_client(state.sequence(), true)
                        .timeout(Duration::from_millis(100), "failed to add client").await else {
                            tracing::error!("failed to add fresh client to broadcast");
                            continue;
                        };

                    let actions_tx = self.actions_tx.clone();

                    tokio::spawn(async move {
                        let Ok(ready) = fresh.send_state(&state).await
                            .err_log("failed to send client fresh state") else { return };

                        let remote = ready.start_io_tasks2(actions_tx, authority_rx)
                            .panic("sequence and state sequences do not match");

                        tracing::info!(?remote, "Fresh client accepted");
                    });
                }
                Event::NewClientWantsRecovery(recover) => {
                    let details = self.shared.read().details().clone();
                    let can_recover = details.can_recover_follower(recover.details());

                    let authority_rx_opt = if can_recover {
                        self.broadcast.add_client(recover.details().sequence, true)
                            .timeout(Duration::from_millis(100), "failed to add client").await
                    } else {
                        None
                    };

                    let actions_tx = self.actions_tx.clone();

                    if let Some(authority_rx) = authority_rx_opt {
                        tokio::spawn(async move {
                            let Ok(ready) = recover
                                .accept_recover::<D>(details).await
                                .err_log("failed to recover state") else { return };

                            let remote = ready.start_io_tasks2(actions_tx, authority_rx);

                            tracing::info!(?remote, "Recover client accepted");
                        });
                    }
                    else {
                        let state = self.shared.read().clone();

                        let Some(authority_rx) = self.broadcast.add_client(state.sequence(), true)
                            .timeout(Duration::from_millis(100), "failed to add client").await else {
                                tracing::error!("failed to add new fresh client");
                                continue;
                            };

                        tokio::spawn(async move {
                            let Ok(ready) = recover
                                .accept_client_with_state(&state).await
                                .err_log("failed to send client new state") else { return };

                            let remote = ready.start_io_tasks2(actions_tx, authority_rx);

                            tracing::info!(?remote, "Recover client accepted");
                        });
                    }
                }
                _ => {}
            }
        }
    }
}
