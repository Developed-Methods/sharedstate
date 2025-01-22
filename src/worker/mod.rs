// use std::{borrow::Cow, collections::{HashSet, VecDeque}, fmt::Debug, sync::Arc, time::Duration};
// 
// use message_encoding::MessageEncoding;
// use message_relay::{MessageRelaySender, RelayOutput};
// use sequenced_broadcast::{SequencedReceiver, SequencedSender};
// use state_manager::StateMaintainer;
// use tokio::sync::{mpsc::{channel, Receiver, Sender}, oneshot};
// use tokio_util::sync::CancellationToken;
// use tracing::Instrument;
// 
// pub use sequenced_broadcast::SequencedBroadcastSettings;
// 
// use crate::{state::{DeterministicState, SharedState}, utils::{LogHelper, PanicHelper, TimeoutPanicHelper}};
// 
// use super::{handshake::{ClientConnected, ClientConnectionAccepted, ClientConnectionAcceptedDirect, HandshakeClient, HandshakeServer, RecoveryAttempt, ServerClientReady, ServerNewClient}, io::{SyncConnection, SyncIO}, message_io::{read_message_opt, send_message, send_zero_message}, recoverable_state::{RecoverableState, StateReplaceCheck}};

pub mod state_updater;
pub mod sync_state;

// pub mod message_relay;
// pub mod state_manager;

// pub struct SyncState<I: SyncIO, D: DeterministicState<Action: MessageEncoding + Clone>> {
//     events_tx: Sender<Event<I, D>>,
//     shared_state: SharedState<RecoverableState<D>>,
//     actions_tx: Sender<D::Action>,
//     shutdown: CancellationToken,
// }
// 
// impl<I: SyncIO, D: DeterministicState> SyncState<I, D> where D::Action: MessageEncoding + Clone, D: MessageEncoding
// {
//     pub async fn new(io: Arc<I>, state: RecoverableState<D>, local: I::Address, settings: SequencedBroadcastSettings) -> Self {
//         let span = tracing::info_span!("SyncWorker", addr = ?local);
// 
//         async {
//             let (
//                 state_maintainer,
//                 sender,
//             ) = StateMaintainer::new(state, settings).await;
// 
//             let shared_state = state_maintainer.shared_state();
//             let (events_tx, events) = channel(1024);
//             let shutdown = CancellationToken::new();
// 
//             let message_sender = MessageRelaySender::new(RelayOutput::SequencedSender(sender));
//             let actions_tx = message_sender.sender();
// 
//             tokio::spawn(SyncWorker {
//                 io: io.clone(),
//                 replace_check: None,
//                 local_address: local.clone(),
//                 leader_address: local.clone(),
//                 alt_leader_addresses: vec![],
//                 state_maintainer,
//                 shared_state: shared_state.clone(),
//                 events,
//                 events_tx: events_tx.clone(),
//                 message_sender,
//                 shutdown: shutdown.clone(),
//             }.start().instrument(tracing::Span::current()));
// 
//             {
//                 let events_tx = events_tx.clone();
//                 let shutdown = shutdown.clone();
// 
//                 tokio::spawn(async move {
//                     loop {
//                         tokio::task::yield_now().await;
// 
//                         let next = tokio::select! {
//                             next = io.next_client() => next,
//                             _ = shutdown.cancelled() => {
//                                 break;
//                             }
//                         };
// 
//                         match next {
//                             Ok(conn) => {
//                                 if events_tx.is_closed() {
//                                     tracing::error!("event_loop is closed, wtf");
//                                 }
//                                 events_tx.send(Event::NewConn(conn)).await.panic("failed to queue event");
//                             }
//                             Err(error) => {
//                                 tracing::error!(?error, "got error from io::next_client");
//                                 tokio::time::sleep(Duration::from_secs(2)).await;
//                             }
//                         }
//                     }
//                 }.instrument(tracing::info_span!("SyncWorkerAccept", addr = ?local)));
//             }
// 
//             SyncState {
//                 events_tx,
//                 shared_state,
//                 actions_tx,
//                 shutdown,
//             }
//         }.instrument(span).await
//     }
// 
//     pub fn actions_tx(&self) -> Sender<D::Action> {
//         self.actions_tx.clone()
//     }
// 
//     pub fn shared_state(&self) -> SharedState<RecoverableState<D>> {
//         self.shared_state.clone()
//     }
// 
//     pub async fn set_replacement_check(&self, check: Option<Arc<dyn StateReplaceCheck<RecoverableState<D>>>>) {
//         let (tx, rx) = oneshot::channel();
//         self.events_tx.send(Event::SetReplaceCheck(check, tx)).await.panic("worker shutdown");
//         let _ = rx.await;
//     }
// 
//     pub async fn set_leader(&self, leader: I::Address) {
//         self.patch_leader_and_alt(Some(leader), None).await;
//     }
// 
//     pub async fn set_leader_alt(&self, alt: Vec<I::Address>) {
//         self.patch_leader_and_alt(None, Some(alt)).await;
//     }
// 
//     pub async fn set_leader_and_alt(&self, leader: I::Address, alt: Vec<I::Address>) {
//         self.patch_leader_and_alt(Some(leader), Some(alt)).await;
//     }
// 
//     pub async fn patch_leader_and_alt(&self, leader: Option<I::Address>, alt: Option<Vec<I::Address>>) {
//         if leader.is_none() && alt.is_none() {
//             return;
//         }
// 
//         let (tx, rx) = oneshot::channel();
//         self.events_tx.send(Event::PatchLeader { leader, alt_leader_addresses: alt, response: tx }).await.panic("worker shutdown");
//         let _ = rx.await;
//     }
// }
// 
// impl<I: SyncIO, D: DeterministicState<Action: MessageEncoding + Clone>> Drop for SyncState<I, D> {
//     fn drop(&mut self) {
//         self.shutdown.cancel();
//     }
// }
// 
// struct SyncWorker<I: SyncIO, D: DeterministicState<Action: MessageEncoding + Clone>> {
//     io: Arc<I>,
// 
//     replace_check: Option<Arc<dyn StateReplaceCheck<RecoverableState<D>>>>,
//     local_address: I::Address,
//     leader_address: I::Address,
//     alt_leader_addresses: Vec<I::Address>,
// 
//     state_maintainer: StateMaintainer<RecoverableState<D>>,
//     shared_state: SharedState<RecoverableState<D>>,
//     events: Receiver<Event<I, D>>,
//     events_tx: Sender<Event<I, D>>,
// 
//     message_sender: MessageRelaySender<D::Action>,
//     shutdown: CancellationToken,
// }
// 
// struct LeaderConnection<I: SyncIO, A> {
//     local_address: I::Address,
//     actions_from_clients: Receiver<A>,
//     feed_state: SequencedSender<A>,
//     attempt_queue: VecDeque<I::Address>,
//     attempted: HashSet<I::Address>,
// }
// 
// enum Event<I: SyncIO, D: DeterministicState> {
//     NewLeader {
//         leader: ClientConnected<'static, I, D>,
//         actions_from_clients: Receiver<D::Action>,
//         leader_actions: SequencedSender<D::Action>
//     },
//     ConnectToLeader,
//     AttemptConnect(LeaderConnection<I, D::Action>),
//     NewConn(SyncConnection<I>),
//     ServerNewClient(ServerNewClient<I>),
//     ClientSubscribed(I::Address, SequencedReceiver<D::Action>, I::Write, Vec<u8>),
//     StartActionReceiver(I::Read),
//     UpdatedState(RecoverableState<D>, ServerClientReady<I>),
//     PatchLeader {
//         leader: Option<I::Address>,
//         alt_leader_addresses: Option<Vec<I::Address>>,
//         response: oneshot::Sender<()>,
//     },
//     SetReplaceCheck(Option<Arc<dyn StateReplaceCheck<RecoverableState<D>>>>, oneshot::Sender<()>),
// }
// 
// impl<I: SyncIO, D: DeterministicState> Debug for Event<I, D> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::NewLeader { .. } => write!(f, "NewLeader"),
//             Self::AttemptConnect(attempt) => write!(f, "AttemptConnect{{ queue: {}, attempted: {} }}", attempt.attempt_queue.len(), attempt.attempted.len()),
//             Self::NewConn(conn) => write!(f, "NewConn {{ remote: {:?} }}", conn.remote),
//             Self::ServerNewClient(ServerNewClient::Fresh(fresh)) => write!(f, "ServerNewClient::Fresh{{ remote: {:?} }}", fresh.remote()),
//             Self::ServerNewClient(ServerNewClient::WithData(data)) => write!(f,
//                 "ServerNewClient::WithData{{ remote: {:?}, state:{{ id: {}, gen: {}, seq: {} }} }}",
//                 data.remote(), data.state_id(), data.state_generation(), data.state_sequence()
//             ),
//             Self::ClientSubscribed(..) => write!(f, "ClientSubscribed"),
//             Self::StartActionReceiver(..) => write!(f, "StartActionReceiver"),
//             Self::UpdatedState(..) => write!(f, "UpdatedState"),
//             Self::PatchLeader { leader, alt_leader_addresses, .. }  => write!(f, "PatchLeader{{ leader: {:?}, alt: {:?} }}", leader, alt_leader_addresses.as_ref().map(|v| v.len())),
//             Self::SetReplaceCheck(..) => write!(f, "SetReplaceCheck"),
//             Self::ConnectToLeader => write!(f, "ConnectToLeader"),
//         }
//     }
// }
// 
// impl<I: SyncIO, D: DeterministicState<Action: MessageEncoding + Clone> + MessageEncoding> SyncWorker<I, D> {
//     async fn start(mut self) {
//         'event_loop: loop {
//             tokio::task::yield_now().await;
// 
//             let event = tokio::select! {
//                 event = self.events.recv() => event.panic("failed to recv event"),
//                 _ = self.shutdown.cancelled() => break,
//             };
// 
//             tracing::info!("Event: {:?}", event);
// 
//             match event {
//                 Event::NewLeader { leader, mut actions_from_clients, mut leader_actions } => {
//                     let state = leader.state.into_owned();
// 
//                     'apply_state: {
//                         /* try to recover with existing state as to not kick clients */
//                         if !leader.remote_state {
//                             let mut replace = self.state_maintainer.replace().await;
//                             let current = replace.state();
//                             let can_replace = current.sequence() == state.sequence()
//                                 && state.is_recoverable(current.id(), current.generation(), current.sequence());
// 
//                             if can_replace {
//                                 replace.replace_state(state).panic("failed to replace state");
//                                 break 'apply_state;
//                             }
//                         }
// 
//                         /* reset state with data from leader */
//                         let new_actions = self.state_maintainer.reset(state).await;
// 
//                         leader_actions.closed()
//                             .timeout(Duration::from_millis(100), "timeout waiting for old leader actions to close after replace")
//                             .await;
// 
//                         leader_actions = new_actions;
//                     }
// 
//                     let SyncConnection { mut read, mut write, .. } = leader.conn;
//                     let mut buffer = leader.buffer;
// 
//                     let events_tx = self.events_tx.clone();
//                     let leader_kill = CancellationToken::new();
// 
//                     tokio::spawn({
//                         let leader_kill = leader_kill.clone();
// 
//                         async move {
//                             loop {
//                                 tokio::task::yield_now().await;
// 
//                                 let read_fut = read_message_opt::<D::Action, _>(
//                                     &mut buffer,
//                                     &mut read,
//                                     Duration::from_secs(2),
//                                     Some(Duration::from_secs(12))
//                                 );
// 
//                                 let read_res = tokio::select! {
//                                     read_res = read_fut => read_res,
//                                     _ = leader_kill.cancelled() => {
//                                         tracing::info!("leader killed from write task, closing read");
//                                         break;
//                                     }
//                                 };
// 
//                                 let Ok(msg_opt) = read_res.err_log("failed to read next message") else { break };
//                                 if let Some(msg) = msg_opt {
//                                     if leader_actions.send(msg).await.is_err() {
//                                         tracing::info!("leader actions queue closed, stopping read from leader task");
//                                         break;
//                                     }
//                                 }
//                             }
// 
//                             tracing::info!("Connection to leader lost, attempting connect");
// 
//                             events_tx
//                                 .send(Event::ConnectToLeader).await
//                                 .panic("failed to queue event");
//                             
//                             leader_kill.cancel();
//                         }.instrument(tracing::info_span!("NewLeader::read"))
//                     });
// 
//                     tokio::spawn(async move {
//                         let mut buffer = Vec::with_capacity(2048);
// 
//                         loop {
//                             tokio::task::yield_now().await;
// 
//                             tokio::select! {
//                                 _ = tokio::time::sleep(Duration::from_secs(2)) => {
//                                     if send_zero_message(&mut write).await.err_log("failed to send zero message").is_err() {
//                                         break;
//                                     }
//                                 }
//                                 msg_opt = actions_from_clients.recv() => {
//                                     match msg_opt {
//                                         Some(msg) => {
//                                             if send_message(&mut buffer, &msg, &mut write, Duration::from_secs(2)).await.err_log("Failed to write message to leader").is_err() {
//                                                 break;
//                                             }
//                                         }
//                                         None => {
//                                             tracing::info!("queue to leader closed, stopping write to leader task");
//                                             break;
//                                         }
//                                     }
//                                 }
//                                 _ = leader_kill.cancelled() => {
//                                     tracing::info!("leader killed from read task, closing write");
//                                     break;
//                                 }
//                             }
//                         }
// 
//                         leader_kill.cancel();
//                     }.instrument(tracing::info_span!("NewLeader::write")));
//                 }
//                 Event::AttemptConnect(mut attempt) => {
//                     if self.leader_address == self.local_address {
//                         tracing::warn!("Attempting to connect but am leader");
//                         continue;
//                     }
// 
//                     let events_tx = self.events_tx.clone();
//                     let Some(attempt_addr) = attempt.attempt_queue.pop_front() else {
//                         tracing::warn!(
//                             "Attempted {} connections to leader, failed. Retrying in 2s",
//                             attempt.attempted.len()
//                         );
// 
//                         attempt.attempt_queue.push_back(self.leader_address.clone());
//                         attempt.attempt_queue.extend(self.alt_leader_addresses.iter().cloned());
//                         attempt.attempted.clear();
// 
//                         tokio::spawn(async move {
//                             tokio::time::sleep(Duration::from_secs(2)).await;
//                             events_tx.send(Event::AttemptConnect(attempt)).await.panic("failed to queue");
//                         });
// 
//                         continue;
//                     };
// 
//                     let io = self.io.clone();
//                     let shared_state = self.shared_state.clone();
//                     let span = tracing::info_span!("ConnectAttempt", addr = ?attempt_addr);
// 
//                     tokio::spawn(async move {
//                         attempt.attempted.insert(attempt_addr.clone());
// 
//                         'attempt: {
//                             let Ok(conn) = io.connect(&attempt_addr).await.err_log("failed to connect to leader") else {
//                                 break 'attempt;
//                             };
// 
//                             tracing::info!("connection established");
// 
//                             let reconnect = {
//                                 let state = shared_state.read();
//                                 if shared_state.read().sequence() == 0 {
//                                     None
//                                 } else {
//                                     Some(RecoveryAttempt {
//                                         state_id: state.id(),
//                                         state_generation: state.generation(),
//                                         state_sequence: state.sequence(),
//                                     })
//                                 }
//                             };
// 
//                             let client = HandshakeClient::new(conn);
//                             let Ok(client) = client.reconnect_opt(reconnect).await
//                                 .err_log("failed to send connection attempt to leader") else { break 'attempt };
// 
//                             let client = match client {
//                                 ClientConnectionAccepted::RelayOption(opt) => {
//                                     if opt.leader_address() == attempt.local_address {
//                                         tracing::error!("leader is relaying back to us");
//                                         break 'attempt;
//                                     }
// 
//                                     if !attempt.attempted.contains(&opt.leader_address()) {
//                                         tracing::info!("we haven't tried the relay leader, trying that first");
//                                         attempt.attempt_queue.push_front(opt.leader_address());
//                                         attempt.attempted.remove(&attempt_addr);
//                                         break 'attempt;
//                                     }
// 
//                                     let Ok(client) = opt.accept_relay().await
//                                         .err_log("failed to accept realy") else { break 'attempt };
// 
//                                     client
//                                 }
//                                 other => other.into_no_relay().panic("relay option should already be handled"),
//                             };
// 
//                             let client = match client {
//                                 ClientConnectionAcceptedDirect::FreshConnection(client) => {
//                                     let Ok(client) = client.load_state().await
//                                         .err_log("failed to load state") else { break 'attempt };
//                                     client
//                                 }
//                                 ClientConnectionAcceptedDirect::RequestingState(client) => {
//                                     let state = shared_state.read().clone();
//                                     let Ok(client) = client.provide_state(Cow::Owned(state)).await
//                                         .err_log("failed to provide state") else { break 'attempt };
//                                     client
//                                 }
//                                 ClientConnectionAcceptedDirect::RecoveringConnection(client) => {
//                                     let state = shared_state.read().clone();
//                                     let Ok(client) = client.recover(state)
//                                         .err_log("failed to recover state") else { break 'attempt };
//                                     client
//                                }
//                             };
// 
//                             events_tx.send(Event::NewLeader {
//                                 leader: client,
//                                 actions_from_clients: attempt.actions_from_clients,
//                                 leader_actions: attempt.feed_state,
//                             }).await.panic("failed to queue event");
// 
//                             return;
//                         }
// 
//                         tokio::spawn(async move {
//                             tracing::info!("failed to connect to {:?}, trying next in 1s", attempt_addr);
//                             tokio::time::sleep(Duration::from_secs(1)).await;
//                             events_tx.send(Event::AttemptConnect(attempt)).await.panic("failed to queue");
//                         });
//                     }.instrument(span));
//                 }
//                 Event::SetReplaceCheck(check, response) => {
//                     self.replace_check = check;
//                     let _ = response.send(());
//                 }
//                 Event::PatchLeader { leader, alt_leader_addresses, response } => {
//                     let was_leader = self.local_address == self.leader_address;
// 
//                     if let Some(leader) = leader {
//                         if leader != self.leader_address {
//                             tracing::info!("Update: leader changed {:?} => {:?}", self.leader_address, leader);
//                         }
//                         self.leader_address = leader;
//                     }
// 
//                     if let Some(alt) = alt_leader_addresses {
//                         self.alt_leader_addresses = alt;
//                     }
// 
//                     let is_leader = self.local_address == self.leader_address;
//                     if was_leader != is_leader {
//                         if is_leader {
//                             tracing::info!("Update: now leader");
// 
//                             let leader_sender = self.state_maintainer.new_source().await;
//                             self.message_sender.replace_output(RelayOutput::SequencedSender(leader_sender)).await;
//                         } else {
//                             tracing::info!("Update: now follower, leader: {:?}", self.leader_address);
// 
//                             let events_tx = self.events_tx.clone();
//                             tokio::spawn(async move {
//                                 events_tx.send(Event::ConnectToLeader).await
//                                     .panic("failed to queue event");
//                             });
//                         }
//                     }
// 
//                     let _ = response.send(());
//                 }
//                 Event::ConnectToLeader => {
//                     if self.local_address == self.leader_address {
//                         continue;
//                     }
// 
//                     let (tx, rx) = channel(2048);
// 
//                     let leader_sender = self.state_maintainer.new_source().await;
//                     self.message_sender.replace_output(RelayOutput::Sender(tx)).await;
// 
//                     let attempt = LeaderConnection {
//                         local_address: self.local_address.clone(),
//                         actions_from_clients: rx,
//                         feed_state: leader_sender,
//                         attempt_queue: {
//                             let mut queue = VecDeque::with_capacity(1 + self.alt_leader_addresses.len());
//                             queue.push_back(self.leader_address.clone());
//                             queue.extend(self.alt_leader_addresses.iter().cloned());
//                             queue
//                         },
//                         attempted: HashSet::new(),
//                     };
// 
//                     let events_tx = self.events_tx.clone();
//                     tokio::spawn(async move {
//                         events_tx
//                             .send(Event::AttemptConnect(attempt)).await
//                             .panic("failed to queue event");
//                     });
//                 }
//                 Event::NewConn(conn) => {
//                     let relay = if self.local_address == self.leader_address {
//                         None
//                     }
//                     else {
//                         Some(self.leader_address.clone())
//                     };
// 
//                     let span = tracing::info_span!("NewConn", remote = ?conn.remote);
// 
//                     let events_tx = self.events_tx.clone();
//                     tokio::spawn(async move {
//                         let client = if let Some(relay) = relay {
//                             let Ok(Some(res)) = HandshakeServer::new(conn).relay(relay).await
//                                 .err_log("failed to accept client as relay") else { return };
//                             res
//                         } else {
//                             let Ok(res) = HandshakeServer::new(conn).accept().await
//                                 .err_log("failed to accept client") else { return };
//                             res
//                         };
// 
//                         events_tx.send(Event::ServerNewClient(client)).await
//                             .panic("failed to queue event");
//                     }.instrument(span));
//                 }
//                 Event::ServerNewClient(client) => {
//                     match client {
//                         ServerNewClient::Fresh(client) => {
//                             let (rx, state) = self.state_maintainer.fresh_subscribe().await;
//                             let events_tx = self.events_tx.clone();
// 
//                             tokio::spawn(async move {
//                                 let Ok(client) = client.send_state(&state).await
//                                     .err_log("failed to send fresh state to client") else { return };
// 
//                                 events_tx.send(Event::ClientSubscribed(client.conn.remote, rx, client.conn.write, client.buffer)).await
//                                     .panic("failed to queue event");
//                                 events_tx.send(Event::StartActionReceiver(client.conn.read)).await
//                                     .panic("failed to queue event");
//                             }.instrument(tracing::info_span!("NewClient::Accept", mode = "fresh")));
//                         }
//                         ServerNewClient::WithData(client) => {
//                             let is_recoverable = self.shared_state.read().is_recoverable(
//                                 client.state_id(),
//                                 client.state_generation(),
//                                 client.state_sequence()
//                             );
// 
//                             let (rx, fresh_state) = 'try_recover: {
//                                 if is_recoverable {
//                                     let Some(recover_rx) = self.state_maintainer.subscribe(client.state_sequence()).await else {
//                                         tracing::warn!("failed to subscribe at sequence: {}", client.state_sequence());
//                                         break 'try_recover self.state_maintainer.fresh_subscribe().await;
//                                     };
// 
//                                     let gens = self.shared_state.read().generations();
//                                     let events_tx = self.events_tx.clone();
// 
//                                     tokio::spawn(async move {
//                                         let Ok(client) = client.accept_recover(gens).await
//                                             .err_log("failed to accept recovery")
//                                             else { return };
// 
//                                         if events_tx.is_closed() {
//                                             tracing::error!("event_loop is closed, wtf");
//                                         }
//                                         events_tx.send(Event::ClientSubscribed(client.conn.remote, recover_rx, client.conn.write, client.buffer)).await
//                                             .panic("failed to queue event");
//                                         events_tx.send(Event::StartActionReceiver(client.conn.read)).await
//                                             .panic("failed to queue event");
//                                     }.instrument(tracing::info_span!("NewClient::Accept", mode = "recover")));
// 
//                                     continue 'event_loop;
//                                 }
// 
//                                 let should_replace = 'should_replace: {
//                                     if self.local_address != self.leader_address {
//                                         break 'should_replace false;
//                                     }
// 
//                                     let Some(check) = &self.replace_check else { break 'should_replace false };
// 
//                                     check.should_replace_state(
//                                         &self.shared_state.read(),
//                                         client.state_id(),
//                                         client.state_generation(),
//                                         client.state_sequence()
//                                     )
//                                 };
// 
//                                 if should_replace {
//                                     let state_manager = self.state_maintainer.clone();
//                                     let shared_state = self.shared_state.clone();
//                                     let events_tx = self.events_tx.clone();
// 
//                                     let check = self.replace_check.clone().unwrap();
// 
//                                     tokio::spawn(async move {
//                                         let Ok(client) = client.request_state::<D>().await
//                                             .err_log("failed to request state from client")
//                                             else { return };
// 
//                                         let should_replace = {
//                                             let read = shared_state.read();
//                                             check.verify_replace_state(&read, client.state())
//                                         };
// 
//                                         if should_replace {
//                                             let Ok(with_state) = client.accept().await
//                                                 .err_log("failed to accept client new state")
//                                                 else { return };
// 
//                                             let (ready, state) = with_state.unbundle();
//                                             events_tx.send(Event::UpdatedState(
//                                                 state,
//                                                 ready,
//                                             )).await.panic("failed to queue event");
//                                         } else {
//                                             let Ok(with_data) = client.reject().await
//                                                 .err_log("failed to reject client's new state")
//                                                 else { return };
// 
//                                             let (rx, state) = state_manager.fresh_subscribe().await;
//                                             let Ok(client) = with_data.accept_client_with_state(&state).await
//                                                 .err_log("failed to accept client with fresh state after receiving their state")
//                                                 else { return };
// 
//                                             events_tx.send(Event::ClientSubscribed(client.conn.remote, rx, client.conn.write, client.buffer)).await
//                                                 .panic("failed to queue event");
//                                             events_tx.send(Event::StartActionReceiver(client.conn.read)).await
//                                                 .panic("failed to queue event");
//                                         }
//                                     }.instrument(tracing::info_span!("NewClient::Accept", mode = "replace-state")));
// 
//                                     continue 'event_loop;
//                                 }
// 
//                                 self.state_maintainer.fresh_subscribe().await
//                             };
// 
//                             let events_tx = self.events_tx.clone();
//                             tokio::spawn(async move {
//                                 let Ok(client) = client.accept_client_with_state(&fresh_state).await
//                                     .err_log("failed to accept client with fresh state after receiving their state")
//                                     else { return };
// 
//                                 events_tx.send(Event::ClientSubscribed(client.conn.remote, rx, client.conn.write, client.buffer)).await
//                                     .panic("failed to queue event");
//                                 events_tx.send(Event::StartActionReceiver(client.conn.read)).await
//                                     .panic("failed to queue event");
//                             }.instrument(tracing::info_span!("NewClient::Accept", mode = "fresh")));
//                         }
//                     }
//                 }
//                 Event::ClientSubscribed(peer_addr, mut rx, mut write, mut buffer) => {
//                     tokio::spawn(async move {
//                         loop {
//                             tokio::task::yield_now().await;
// 
//                             let Ok(msg) = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await else {
//                                 let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), send_zero_message(&mut write)).await else {
//                                     tracing::error!("failed to send zero message");
//                                     break;
//                                 };
// 
//                                 continue;
//                             };
// 
//                             let Some((_, action)) = msg else { break };
// 
//                             let success = send_message(&mut buffer, &action, &mut write, Duration::from_secs(2)).await
//                                 .err_log("failed to send message").is_ok();
// 
//                             if !success {
//                                 break;
//                             }
//                         }
//                     }.instrument(tracing::info_span!("FeedClient", ?peer_addr)));
//                 }
//                 Event::StartActionReceiver(mut read) => {
//                     let sender = self.message_sender.clone();
// 
//                     tokio::spawn(async move {
//                         let mut buffer = Vec::with_capacity(2048);
// 
//                         loop {
//                             tokio::task::yield_now().await;
// 
//                             let next = read_message_opt::<D::Action, _>(
//                                 &mut buffer,
//                                 &mut read,
//                                 Duration::from_secs(2),
//                                 Some(Duration::from_secs(8))
//                             ).await;
// 
//                             match next {
//                                 Ok(None) => {
//                                     continue;
//                                 },
//                                 Ok(Some(msg)) => {
//                                     if sender.send(msg).await.is_err() {
//                                         break;
//                                     }
//                                 }
//                                 Err(error) => {
//                                     tracing::error!(?error, "failed to read message");
//                                     break;
//                                 }
//                             }
//                         }
//                     }.instrument(tracing::info_span!("ActionReceiver")));
//                 }
//                 Event::UpdatedState(state, client) => {
//                     let Some(check) = &self.replace_check else { continue };
//                     if self.local_address != self.leader_address {
//                         continue;
//                     }
// 
//                     if !check.verify_replace_state(&self.shared_state.read(), &state) {
//                         continue;
//                     }
// 
//                     let sender = self.state_maintainer.reset(state).await;
//                     self.message_sender.replace_output(RelayOutput::SequencedSender(sender)).await;
// 
//                     let Some(rx) = self.state_maintainer.subscribe(client.state_sequence).await else {
//                         tracing::error!("failed to subscribe to recently reset state");
//                         continue;
//                     };
// 
//                     let events_tx = self.events_tx.clone();
//                     tokio::spawn(async move {
//                         events_tx.send(Event::ClientSubscribed(client.conn.remote, rx, client.conn.write, client.buffer)).await
//                             .panic("failed to queue event");
//                         events_tx.send(Event::StartActionReceiver(client.conn.read)).await
//                             .panic("failed to queue event");
//                     });
//                 }
//             }
//         }
// 
//         tracing::info!("Worker shutting down");
// 
//         let (tx, mut rx) = channel(1024);
//         self.events_tx = tx;
// 
//         loop {
//             tokio::task::yield_now().await;
// 
//             tokio::select! {
//                 event = self.events.recv() => {
//                     if event.is_none() {
//                         break;
//                     }
//                 }
//                 _ = rx.recv() => {}
//             }
//         }
// 
//         tracing::info!("Worker shutdown");
//     }
// }
// 
// #[cfg(test)]
// mod test {
//     use std::time::Instant;
// 
//     use super::*;
//     use crate::testing::{setup_logging, state_tests::{TestState, TestStateAction}, test_sync_io::{TestIOKillMode, TestSyncNet}};
// 
//     #[tokio::test]
//     async fn sync_state_change_leader_test() {
//         setup_logging();
// 
//         let io = TestSyncNet::new();
//         let net1 = io.io(1).await;
//         let net2 = io.io(2).await;
//         let net3 = io.io(3).await;
// 
//         let sync1 = SyncState::new(
//             net1,
//             RecoverableState::first_generation(TestState::default()),
//             1,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         let sync2 = SyncState::new(
//             net2,
//             RecoverableState::first_generation(TestState::default()),
//             2,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         let sync3 = SyncState::new(
//             net3,
//             RecoverableState::first_generation(TestState::default()),
//             3,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         sync2.set_leader(1).await;
//         sync3.set_leader(1).await;
// 
//         sync1.actions_tx.send(TestStateAction::Add { slot: 1, value: 10 }).await.unwrap();
//         sync2.actions_tx.send(TestStateAction::Add { slot: 2, value: 22 }).await.unwrap();
//         sync3.actions_tx.send(TestStateAction::Add { slot: 3, value: 33 }).await.unwrap();
// 
//         tokio::time::sleep(Duration::from_millis(10)).await;
// 
//         assert_eq!(sync1.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync1.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync1.shared_state.read().state().numbers[3], 33);
// 
//         assert_eq!(sync2.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync2.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync2.shared_state.read().state().numbers[3], 33);
// 
//         assert_eq!(sync3.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync3.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync3.shared_state.read().state().numbers[3], 33);
// 
//         sync1.set_leader(2).test_timeout().await;
//         sync2.set_leader(2).test_timeout().await;
//         sync3.set_leader(2).test_timeout().await;
// 
//         sync3.actions_tx.send(TestStateAction::Add { slot: 1, value: 300 }).test_timeout().await.unwrap();
// 
//         tokio::time::sleep(Duration::from_secs(5)).await;
//         assert_eq!(sync1.shared_state.read().state().numbers[1], 310);
//         assert_eq!(sync2.shared_state.read().state().numbers[1], 310);
//         assert_eq!(sync3.shared_state.read().state().numbers[1], 310);
//     }
// 
//     #[tokio::test]
//     async fn sync_state_connect_through_alt() {
//         setup_logging();
// 
//         let io = TestSyncNet::new();
//         let net1 = io.io(1).await;
//         let net2 = io.io(2).await;
//         let net3 = io.io(3).await;
//         io.block_connection(3, 1, true).await;
// 
//         let sync1 = SyncState::new(
//             net1,
//             RecoverableState::first_generation(TestState::default()),
//             1,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         let sync2 = SyncState::new(
//             net2,
//             RecoverableState::first_generation(TestState::default()),
//             2,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         let sync3 = SyncState::new(
//             net3,
//             RecoverableState::first_generation(TestState::default()),
//             3,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         sync2.set_leader_and_alt(1, vec![1, 2, 3]).await;
//         sync3.set_leader_and_alt(1, vec![1, 2, 3]).await;
// 
//         sync1.actions_tx.send(TestStateAction::Add { slot: 1, value: 10 }).await.unwrap();
//         sync2.actions_tx.send(TestStateAction::Add { slot: 2, value: 22 }).await.unwrap();
//         sync3.actions_tx.send(TestStateAction::Add { slot: 3, value: 33 }).await.unwrap();
// 
//         tokio::time::sleep(Duration::from_secs(5)).await;
// 
//         assert_eq!(sync1.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync1.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync1.shared_state.read().state().numbers[3], 33);
// 
//         assert_eq!(sync2.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync2.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync2.shared_state.read().state().numbers[3], 33);
// 
//         assert_eq!(sync3.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync3.shared_state.read().state().numbers[2], 22);
//         assert_eq!(sync3.shared_state.read().state().numbers[3], 33);
//     }
// 
//     #[tokio::test]
//     async fn sync_state_should_reconnect() {
//         setup_logging();
// 
//         let io = TestSyncNet::new();
//         let net1 = io.io(1).await;
//         let net2 = io.io(2).await;
// 
//         let sync1 = SyncState::new(
//             net1,
//             RecoverableState::first_generation(TestState::default()),
//             1,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         let sync2 = SyncState::new(
//             net2,
//             RecoverableState::first_generation(TestState::default()),
//             2,
//             SequencedBroadcastSettings::default()
//         ).await;
// 
//         sync2.set_leader(1).await;
// 
//         sync1.actions_tx.send(TestStateAction::Add { slot: 1, value: 10 }).await.unwrap();
//         sync2.actions_tx.send(TestStateAction::Add { slot: 2, value: 22 }).await.unwrap();
// 
//         tokio::time::sleep(Duration::from_secs(1)).await;
// 
//         assert_eq!(sync1.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync1.shared_state.read().state().numbers[2], 22);
// 
//         assert_eq!(sync2.shared_state.read().state().numbers[1], 10);
//         assert_eq!(sync2.shared_state.read().state().numbers[2], 22);
// 
//         tracing::info!("Kill connection to leader");
//         let count = io.kill_connection(1, 2, TestIOKillMode::Timeout).await;
//         tracing::info!("Killed {} connections", count);
// 
//         sync1.actions_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
// 
//         tracing::info!("wait till client gets new leader data");
//         tokio::time::sleep(Duration::from_millis(10)).await;
//         assert_eq!(sync1.shared_state.read().state().numbers[1], 110);
// 
//         for _ in 0..20 {
//             tokio::time::sleep(Duration::from_secs(1)).await;
// 
//             let value = sync2.shared_state.read().state().numbers[1];
//             if value == 110 {
//                 break;
//             }
//         }
// 
//         tracing::info!("DONE");
//     }
// }
