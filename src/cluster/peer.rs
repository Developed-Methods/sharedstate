use std::{future::Future, num::NonZeroU64, panic::Location, sync::Arc};

use crate::{
    cluster::{
        client::PROTOCOL_VERSION,
        election::valid_local_leader_path,
        inner::Inner,
        messages::{LeaderStatus, SyncRequest, SyncResponse},
    },
    net::sync_io::SyncIOAddress,
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::RecoverableStateAction,
    },
    utils::now_ms,
};
use tokio::sync::mpsc;

pub(crate) struct PeerWorker<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) transport_addr: A,
    pub(crate) node_addr: Option<A>,
    pub(crate) counted_connected: bool,
    pub(crate) inner: Arc<Inner<A, D>>,
    pub(crate) read: mpsc::Receiver<SyncRequest<A, D>>,
    pub(crate) write: mpsc::Sender<SyncResponse<A, D>>,
}

impl<A: SyncIOAddress, D: DeterministicState> PeerWorker<A, D>
where
    D::Action: Clone,
{
    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.read.recv().await {
            match msg {
                SyncRequest::Ping(id) => {
                    self.record_activity_ts(true).await;
                    if !self.send(SyncResponse::Pong(id)).await {
                        break;
                    }
                }
                SyncRequest::ProtocolVersion(version) => {
                    if version != PROTOCOL_VERSION {
                        break;
                    }
                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::SharePeers(peers) => {
                    self.inner.discover_peers(peers.into_iter()).await;
                    if !self.send(SyncResponse::Peers(self.inner.peer_snapshot().await)).await {
                        break;
                    }
                }
                SyncRequest::WhoIsLeader => {
                    if !self
                        .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
                        .await
                    {
                        break;
                    }
                }
                SyncRequest::ShareElection => {
                    let observation = self.inner.local_observation().await;
                    if !self.send(SyncResponse::Election(observation)).await {
                        break;
                    }
                }
                SyncRequest::SubscribeFresh => {
                    if !self.subscribe_fresh().await {
                        break;
                    }
                }
                SyncRequest::SubscribeRecovery(details) => {
                    if !self.subscribe_recovery(details).await {
                        break;
                    }
                }
                SyncRequest::MyAddress(addr) => {
                    self.node_addr = Some(addr);
                    self.record_activity_ts(true).await;
                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::ShareLeaderPath => {
                    let info = self.inner.leader_info_message().await;
                    let resp = match info.path {
                        None => SyncResponse::NoPathToLeader,
                        Some(path) if valid_local_leader_path(path.first().copied(), &path, self.inner.address) => {
                            SyncResponse::LeaderPath(path)
                        }
                        Some(_) => SyncResponse::NoPathToLeader,
                    };
                    if !self.send(resp).await {
                        break;
                    }
                }
                SyncRequest::Action { source, action } => {
                    self.handle_action(source, action).await;
                }
                SyncRequest::LeaderStatus { address, status } => {
                    self.handle_leader_status(address, status).await;
                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
            };
        }

        self.record_activity_ts(false).await;
    }

    async fn subscribe_fresh(&self) -> bool {
        let subscribe_result = {
            let state = self.inner.state.lock().await;
            state.subscribe().await
        };

        let (state, mut feed) = match subscribe_result {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("failed to subscribe follower to leader state: {e}");
                return false;
            }
        };

        if !self.send(SyncResponse::FreshState(state)).await {
            return false;
        }
        if !self
            .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
            .await
        {
            return false;
        }

        let write = self.write.clone();
        let mut leader_updates = self.inner.leader_updates.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    action = feed.recv() => {
                        match action {
                            Ok((seq, action)) => {
                                if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    changed = leader_updates.changed() => {
                        if changed.is_err() { break; }
                        let info = leader_updates.borrow().clone();
                        if write.send(SyncResponse::LeaderInfo(info)).await.is_err() { break; }
                    }
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn subscribe_recovery(&self, details: crate::state::recoverable_state::RecoverableStateDetails) -> bool {
        let leader_state = self.inner.state.lock().await.recoverable_state_details().await;

        if !leader_state.can_recover_follower(&details) {
            return self.send(SyncResponse::RecoveryFailed(leader_state)).await;
        }

        let feed = {
            let state = self.inner.state.lock().await;
            state.subscribe_at(details.next_seq()).await
        };

        let Ok(mut feed) = feed else {
            return self.send(SyncResponse::RecoveryFailed(leader_state)).await;
        };

        if !self.send(SyncResponse::Accepted(feed.next_seq())).await {
            return false;
        }
        if !self
            .send(SyncResponse::LeaderInfo(self.inner.leader_info_message().await))
            .await
        {
            return false;
        }

        let write = self.write.clone();
        let mut leader_updates = self.inner.leader_updates.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    action = feed.recv() => {
                        match action {
                            Ok((seq, action)) => {
                                if write.send(SyncResponse::AuthorityAction(seq, action)).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    changed = leader_updates.changed() => {
                        if changed.is_err() { break; }
                        let info = leader_updates.borrow().clone();
                        if write.send(SyncResponse::LeaderInfo(info)).await.is_err() { break; }
                    }
                }
            }

            let _ = write.send(SyncResponse::ActionStreamClosed).await;
        });

        true
    }

    async fn handle_action(&self, source: A, action: D::Action) {
        let (is_leader, follow_tx) = {
            let control = self.inner.control.lock().await;
            (
                control.leader.leader == Some(self.inner.address),
                control.follow.as_ref().map(|follow| follow.to_peer.clone()),
            )
        };
        if is_leader {
            let authority = {
                let state = self.inner.state.lock().await;
                let state_clone = state.state_clone().await;
                state_clone.state().authority(action)
            };
            if let Err(e) = self
                .inner
                .state
                .lock()
                .await
                .apply_authority(RecoverableStateAction::StateAction { action: authority })
                .await
            {
                tracing::error!("failed to apply authority action from peer: {e}");
            }
            return;
        }

        if let Some(follow_tx) = follow_tx {
            if follow_tx.send(SyncRequest::Action { source, action }).await.is_err() {
                tracing::warn!("failed to forward action to upstream leader");
            }
            return;
        }

        tracing::warn!(?source, "dropping remote action because no leader path is available");
    }

    async fn handle_leader_status(&self, address: A, status: LeaderStatus<A>) {
        let mut changed = false;
        let mut observed_term = None;
        match status {
            LeaderStatus::Promoted { term, leader } => {
                observed_term = Some(term);
                let mut control = self.inner.control.lock().await;
                if control.leader.term < term || (control.leader.term == term && Some(leader) < control.leader.leader) {
                    control.leader.term = term;
                    control.leader.leader = Some(leader);
                    control.leader.path = if leader == self.inner.address {
                        Some(vec![self.inner.address])
                    } else if leader == address {
                        Some(vec![leader, self.inner.address])
                    } else {
                        None
                    };
                    changed = true;
                }
            }
            LeaderStatus::Offline { term, leader } => {
                observed_term = Some(term);
                let mut control = self.inner.control.lock().await;
                if control.leader.term <= term && control.leader.leader == Some(leader) {
                    control.leader.leader = None;
                    control.leader.path = None;
                    control.leader.term = term;
                    changed = true;
                }
            }
            LeaderStatus::Observation(observation) => {
                self.inner.record_observation(observation).await;
            }
        }
        if changed {
            self.inner.publish_leader_info().await;
        }
        if let Some(term) = observed_term {
            self.inner.observe_term(term).await;
        }
    }

    async fn record_activity_ts(&mut self, connected: bool) {
        let now = NonZeroU64::new(now_ms());
        let Some(node_addr) = self.node_addr else {
            tracing::debug!(
                ?self.transport_addr,
                "not recording peer activity before node address is identified"
            );
            return;
        };

        let mut control = self.inner.control.lock().await;
        let Some(Some(details)) = control.peers.get_mut(&node_addr) else {
            return;
        };

        details.last_activity = now;
        details.last_global_activity = now;
        if connected {
            if !self.counted_connected {
                details.active_connections = details.active_connections.saturating_add(1);
                self.counted_connected = true;
            }
            details.connected = true;
        } else if self.counted_connected {
            details.active_connections = details.active_connections.saturating_sub(1);
            self.counted_connected = false;
            details.connected = details.active_connections > 0;
        }
    }

    #[track_caller]
    fn send(&self, msg: SyncResponse<A, D>) -> impl Future<Output = bool> + '_ {
        let caller = Location::caller();

        async {
            if self.write.send(msg).await.is_err() {
                tracing::warn!("failed to send response to peer {}:{}", caller.file(), caller.line());
                false
            } else {
                true
            }
        }
    }
}
