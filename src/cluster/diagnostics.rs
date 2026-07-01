use crate::{state::deterministic::DeterministicState, transport::traits::SyncIOAddress, utils::now_ms};

use super::node::{Inner, NodeDebugInfo, PeerDebugInfo};

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    pub(crate) async fn debug_info(&self) -> NodeDebugInfo<A> {
        let now = now_ms();
        let control = self.control.lock().await;
        let leader = control.leader.clone();
        let follow = control
            .follow
            .as_ref()
            .map(|follow| (follow.remote, follow.leader_path.clone()));
        let follow_remote = follow.as_ref().map(|(remote, _)| *remote);

        let mut peer_debug = control
            .peers
            .iter()
            .map(|(address, details)| {
                let observation = details.as_ref().and_then(|details| details.last_observation.clone());
                let connected = details
                    .as_ref()
                    .map(|details| details.connected || Some(*address) == follow_remote);
                PeerDebugInfo {
                    address: *address,
                    known: details.is_some(),
                    can_lead: details.as_ref().map(|details| details.can_lead),
                    connected,
                    latency_ms: details.as_ref().and_then(|details| details.latency_ms),
                    repeat_connect_fails: details.as_ref().map(|details| details.repeat_connect_fails),
                    last_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_global_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_global_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_attempt_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_attempt)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_fail_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_fail)
                        .map(|ts| now.saturating_sub(ts.get())),
                    observed_leader: observation.as_ref().and_then(|observation| observation.leader),
                    observed_term: observation.as_ref().map(|observation| observation.term),
                    observed_leader_path: observation
                        .as_ref()
                        .and_then(|observation| observation.leader_path.clone()),
                    observed_reachable_can_lead: observation.map(|observation| observation.reachable_can_lead),
                }
            })
            .collect::<Vec<_>>();
        peer_debug.sort_by_key(|peer| peer.address);

        let mut observations = control.election.observations.values().cloned().collect::<Vec<_>>();
        observations.sort_by_key(|observation| observation.observer);

        NodeDebugInfo {
            address: self.address,
            can_lead: self.can_lead,
            leader: leader.leader,
            leader_path: leader.path,
            term: leader.term,
            follow_remote,
            follow_leader_path: follow.map(|(_, path)| path),
            known_can_lead: control.election.known_can_lead.iter().copied().collect(),
            last_promoted_leader: control.election.last_promoted_leader,
            observations,
            peers: peer_debug,
        }
    }
}
