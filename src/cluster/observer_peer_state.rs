use std::num::NonZeroU64;

use message_encoding::MessageEncoding;

use crate::{state::deterministic::DeterministicState, transport::traits::SyncIO, utils::now_ms};

use super::{election::valid_remote_leader_path, observer::ClientWorker};

impl<I, D> ClientWorker<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding + Clone,
    D::AuthorityAction: MessageEncoding,
{
    pub(crate) async fn mark_connect_attempt(&self, target: I::Address) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_connect_attempt = NonZeroU64::new(now_ms());
        }
    }

    pub(crate) async fn mark_connect_fail(&self, target: I::Address) {
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

    pub(crate) async fn mark_connected(&self, target: I::Address, latency_ms: u64) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_activity = NonZeroU64::new(now_ms());
            details.last_global_activity = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = 0;
            details.latency_ms = Some(latency_ms);
            details.connected = true;
        }
    }

    pub(crate) async fn mark_observed(&self, target: I::Address, latency_ms: u64) {
        let mut control = self.inner.control.lock().await;
        if let Some(Some(details)) = control.peers.get_mut(&target) {
            details.last_activity = NonZeroU64::new(now_ms());
            details.last_global_activity = NonZeroU64::new(now_ms());
            details.repeat_connect_fails = 0;
            details.latency_ms = Some(latency_ms);
        }
    }
}
