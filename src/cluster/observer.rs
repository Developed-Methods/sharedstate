use std::{num::NonZeroU64, sync::Arc};

use futures_util::StreamExt;
use message_encoding::MessageEncoding;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    protocol::messages::{SyncRequest, SyncResponse},
    state::determinstic_state::DeterministicState,
    transport::{channels::NetIoSettings, traits::SyncIO},
    utils::now_ms,
};

use super::{
    control::FollowConnection,
    election::{
        append_path, observation_targets, sort_follow_candidates, valid_remote_leader_path, FollowCandidate,
        ObservationTargetInput, PeerKind,
    },
    node::{recv_timeout, send_expect_ok, Inner, PROTOCOL_VERSION},
};

pub(crate) struct ClientWorker<I: SyncIO, D: DeterministicState> {
    pub(crate) inner: Arc<Inner<I::Address, D>>,
    pub(crate) io: Arc<I>,
    pub(crate) io_settings: NetIoSettings,
}

impl<I, D> ClientWorker<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding + Clone,
    D::AuthorityAction: MessageEncoding,
{
    pub(crate) async fn run(self) {
        loop {
            self.observe_election_peers().await;
            self.inner.apply_election().await;
            self.ensure_follow_connection().await;
            tokio::time::sleep(self.inner.timing.observation_interval).await;
        }
    }

    async fn observe_election_peers(&self) {
        let targets = self.observation_targets().await;

        let mut results = futures_util::stream::iter(targets.into_iter())
            .map(|target| self.observe_peer(target))
            .buffered(8);

        while results.next().await.is_some() {}
    }

    async fn observation_targets(&self) -> Vec<I::Address> {
        let control = self.inner.control.lock().await;
        let leader = control.leader.leader;
        let follow_remote = control.follow.as_ref().map(|follow| follow.remote);
        let has_usable_path = leader.is_some() && (leader == Some(self.inner.address) || follow_remote.is_some());

        observation_targets(ObservationTargetInput {
            local: self.inner.address,
            can_lead: self.inner.can_lead,
            leader,
            follow_remote,
            has_usable_path,
            peers: control
                .peers
                .iter()
                .map(|(addr, details)| {
                    (
                        *addr,
                        details
                            .as_ref()
                            .map(|details| PeerKind::Known {
                                can_lead: details.can_lead,
                            })
                            .unwrap_or(PeerKind::Unknown),
                    )
                })
                .collect(),
        })
    }

    async fn observe_peer(&self, target: I::Address) {
        self.mark_connect_attempt(target).await;

        let Ok(Ok(conn)) = tokio::time::timeout(self.inner.timing.rpc_timeout, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (_addr, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());

        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
            self.inner.timing.rpc_timeout,
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        if !send_expect_ok(&write, &mut read, SyncRequest::MyAddress(self.inner.address), self.inner.timing.rpc_timeout)
            .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let started = now_ms();
        if write.send(SyncRequest::Ping(started)).await.is_err() {
            self.mark_connect_fail(target).await;
            return;
        }

        match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
            Some(SyncResponse::Pong(id)) if id == started => {
                self.mark_observed(target, now_ms().saturating_sub(started)).await;
            }
            _ => {
                self.mark_connect_fail(target).await;
                return;
            }
        }

        let peers = self.inner.peer_snapshot().await;
        if write.send(SyncRequest::SharePeers(peers)).await.is_ok() {
            if let Some(SyncResponse::Peers(peers)) = recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                self.inner.discover_peers(peers.into_iter()).await;
            }
        }

        if write.send(SyncRequest::ShareElection).await.is_ok() {
            if let Some(SyncResponse::Election(observation)) =
                recv_timeout(&mut read, self.inner.timing.rpc_timeout).await
            {
                self.inner.record_observation(observation).await;
            }
        }
    }

    async fn ensure_follow_connection(&self) {
        let (leader, has_follow) = {
            let control = self.inner.control.lock().await;
            (control.leader.leader, control.follow.is_some())
        };
        if leader == Some(self.inner.address) {
            self.inner.clear_follow().await;
            return;
        }

        if has_follow {
            return;
        }

        let Some(target) = self.select_follow_target(leader).await else {
            tokio::time::sleep(self.inner.timing.follow_retry_interval).await;
            return;
        };

        self.connect_follow(target, leader).await;
    }

    async fn select_follow_target(&self, leader: Option<I::Address>) -> Option<I::Address> {
        let control = self.inner.control.lock().await;
        let candidates = control
            .peers
            .iter()
            .filter_map(|(addr, details)| {
                if *addr == self.inner.address {
                    return None;
                }
                Some(match details {
                    Some(details) => FollowCandidate {
                        address: *addr,
                        connected: details.connected,
                        latency_ms: details.latency_ms,
                        repeat_connect_fails: details.repeat_connect_fails,
                        last_connect_fail_ms: details.last_connect_fail.map(|v| v.get()),
                        failed_without_activity: details.last_activity.is_none() && details.last_connect_fail.is_some(),
                        can_lead: details.can_lead,
                        observed_leader: details.last_observation.as_ref().and_then(|o| o.leader),
                    },
                    None => FollowCandidate {
                        address: *addr,
                        connected: false,
                        latency_ms: None,
                        repeat_connect_fails: 0,
                        last_connect_fail_ms: None,
                        failed_without_activity: false,
                        can_lead: false,
                        observed_leader: None,
                    },
                })
            })
            .collect::<Vec<_>>();

        sort_follow_candidates(leader, candidates)
            .first()
            .map(|candidate| candidate.address)
    }

    async fn connect_follow(&self, target: I::Address, selected_leader: Option<I::Address>) {
        self.mark_connect_attempt(target).await;

        let Ok(Ok(conn)) = tokio::time::timeout(self.inner.timing.rpc_timeout, self.io.connect(&target)).await else {
            self.mark_connect_fail(target).await;
            return;
        };

        let (_transport_remote, write, mut read) = conn.client_channels::<D>(self.io_settings.clone());
        if !send_expect_ok(
            &write,
            &mut read,
            SyncRequest::ProtocolVersion(PROTOCOL_VERSION),
            self.inner.timing.rpc_timeout,
        )
        .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        if !send_expect_ok(&write, &mut read, SyncRequest::MyAddress(self.inner.address), self.inner.timing.rpc_timeout)
            .await
        {
            self.mark_connect_fail(target).await;
            return;
        }

        let leader_info = if write.send(SyncRequest::WhoIsLeader).await.is_ok() {
            match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                Some(SyncResponse::LeaderInfo(info)) => info,
                _ => {
                    self.mark_connect_fail(target).await;
                    return;
                }
            }
        } else {
            self.mark_connect_fail(target).await;
            return;
        };

        let Some(leader) = leader_info.leader.or(selected_leader) else {
            self.mark_connect_fail(target).await;
            return;
        };

        let Some(path) = leader_info.path.clone() else {
            self.mark_connect_fail(target).await;
            return;
        };

        if !valid_remote_leader_path(Some(leader), &path, target, self.inner.address) {
            self.mark_connect_fail(target).await;
            return;
        }

        let local_path = append_path(path.clone(), self.inner.address);
        self.inner
            .set_remote_leader_path(leader, Some(leader_info.term), local_path.clone(), None)
            .await;

        let details = self.inner.state.lock().await.recoverable_state_details().await;
        if write.send(SyncRequest::SubscribeRecovery(details)).await.is_err() {
            self.mark_connect_fail(target).await;
            return;
        }

        let first_state_msg = match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
            Some(SyncResponse::Accepted(_seq)) => None,
            Some(SyncResponse::RecoveryFailed(_)) => {
                if write.send(SyncRequest::SubscribeFresh).await.is_err() {
                    self.mark_connect_fail(target).await;
                    return;
                }
                match recv_timeout(&mut read, self.inner.timing.rpc_timeout).await {
                    Some(SyncResponse::FreshState(state)) => Some(state),
                    _ => {
                        self.mark_connect_fail(target).await;
                        return;
                    }
                }
            }
            Some(SyncResponse::FreshState(state)) => Some(state),
            _ => {
                self.mark_connect_fail(target).await;
                return;
            }
        };

        if let Some(state) = first_state_msg {
            if let Err(error) = self.inner.state.lock().await.reset(state).await {
                tracing::error!(?error, "failed to reset follower state");
                self.inner.fail(format!("failed to reset follower state: {error:?}"));
                self.mark_connect_fail(target).await;
                return;
            }
        }

        let cancel = CancellationToken::new();
        let follow = FollowConnection {
            remote: target,
            leader_path: local_path,
            to_peer: write.clone(),
            cancel: cancel.clone(),
        };

        if let Some(existing) = self.inner.control.lock().await.follow.replace(follow) {
            existing.cancel.cancel();
        }

        self.mark_connected(target, 0).await;
        self.inner.publish_leader_info().await;
        self.spawn_follow_reader(target, read, cancel);
    }

    fn spawn_follow_reader(
        &self,
        target: I::Address,
        mut read: mpsc::Receiver<SyncResponse<I::Address, D>>,
        cancel: CancellationToken,
    ) {
        let inner = self.inner.clone();

        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = cancel.cancelled() => break,
                    msg = read.recv() => msg,
                };

                match msg {
                    Some(SyncResponse::AuthorityAction(_, action)) => {
                        if let Err(error) = inner.state.lock().await.apply_authority(action).await {
                            tracing::error!(?error, "failed to apply followed authority action");
                            inner.fail(format!("failed to apply followed authority action: {error:?}"));
                            break;
                        }
                    }
                    Some(SyncResponse::LeaderInfo(info)) => match (info.leader, info.path) {
                        (Some(leader_addr), Some(path))
                            if valid_remote_leader_path(Some(leader_addr), &path, target, inner.address) =>
                        {
                            inner
                                .set_remote_leader_path(
                                    leader_addr,
                                    Some(info.term),
                                    append_path(path, inner.address),
                                    Some(target),
                                )
                                .await;
                        }
                        _ => {
                            if inner.clear_follow_to(target).await {
                                inner.clear_remote_leader().await;
                            }
                        }
                    },
                    Some(SyncResponse::LeaderPath(path)) => {
                        if let Some(leader_addr) = path.first().copied() {
                            if valid_remote_leader_path(Some(leader_addr), &path, target, inner.address) {
                                inner
                                    .set_remote_leader_path(
                                        leader_addr,
                                        None,
                                        append_path(path, inner.address),
                                        Some(target),
                                    )
                                    .await;
                            }
                        }
                    }
                    Some(SyncResponse::NoPathToLeader) => {
                        if inner.clear_follow_to(target).await {
                            inner.clear_remote_leader().await;
                        }
                    }
                    Some(SyncResponse::Peers(peers)) => {
                        inner.discover_peers(peers.into_iter()).await;
                    }
                    Some(SyncResponse::ActionStreamClosed) | None => break,
                    _ => {}
                }
            }

            let closed_follow = { inner.control.lock().await.follow.take() };
            if let Some(follow) = closed_follow {
                if follow.remote == target {
                    follow.cancel.cancel();
                    if let Some(Some(details)) = inner.control.lock().await.peers.get_mut(&target) {
                        details.connected = details.active_connections > 0;
                    }
                    inner.clear_remote_leader().await;
                } else {
                    let _ = inner.control.lock().await.follow.replace(follow);
                }
            }
        });
    }

    async fn mark_connect_attempt(&self, target: I::Address) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_connect_attempt = NonZeroU64::new(now_ms());
        }
    }

    async fn mark_connect_fail(&self, target: I::Address) {
        {
            let mut control = self.inner.control.lock().await;
            let has_follow_to_target = control.follow.as_ref().is_some_and(|follow| follow.remote == target);
            if let Some(Some(details)) = control.peers.get_mut(&target) {
                if !has_follow_to_target && details.active_connections == 0 {
                    details.last_activity = None;
                    details.connected = false;
                }
                details.last_connect_fail = NonZeroU64::new(now_ms());
                details.repeat_connect_fails = details.repeat_connect_fails.saturating_add(1);
                details.last_observation = None;
            }

            for details in control.peers.values_mut().filter_map(Option::as_mut) {
                let invalid_target_observation = details
                    .last_observation
                    .as_ref()
                    .filter(|observation| observation.leader == Some(target))
                    .map(|observation| {
                        !observation
                            .leader_path
                            .as_deref()
                            .map(|path| {
                                valid_remote_leader_path(Some(target), path, observation.observer, self.inner.address)
                            })
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if invalid_target_observation {
                    details.last_observation = None;
                }
            }

            control.election.observations.retain(|observer, observation| {
                if *observer == target {
                    return false;
                }
                if observation.leader != Some(target) {
                    return true;
                }
                observation
                    .leader_path
                    .as_deref()
                    .map(|path| valid_remote_leader_path(Some(target), path, *observer, self.inner.address))
                    .unwrap_or(false)
            });
            let has_relay_follow = control
                .follow
                .as_ref()
                .is_some_and(|follow| follow.remote != target && follow.leader_path.first().copied() == Some(target));
            let should_clear = !has_relay_follow
                && control.leader.leader == Some(target)
                && control.leader.leader != Some(self.inner.address);
            if should_clear {
                control.leader.leader = None;
                control.leader.path = None;
            }
            drop(control);
            if should_clear {
                self.inner.publish_leader_info().await;
            }
        }
    }

    async fn mark_connected(&self, target: I::Address, latency_ms: u64) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_activity = NonZeroU64::new(now_ms());
            details.last_global_activity = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = 0;
            details.latency_ms = Some(latency_ms);
            details.connected = true;
        }
    }

    async fn mark_observed(&self, target: I::Address, latency_ms: u64) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_activity = NonZeroU64::new(now_ms());
            details.last_global_activity = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = 0;
            details.latency_ms = Some(latency_ms);
        }
    }
}
