use std::{collections::HashMap, num::NonZeroU64};

use crate::{
    protocol::messages::ElectionObservation, state::deterministic::DeterministicState, transport::traits::SyncIOAddress,
};

use super::{
    election::ElectionState,
    follower::FollowConnection,
    node::{Inner, NodeStatus},
};

pub(crate) struct ControlState<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) leader: LeaderInfo<A>,
    pub(crate) peers: HashMap<A, Option<PeerDetails<A>>>,
    pub(crate) follow: Option<FollowConnection<A, D>>,
    pub(crate) election: ElectionState<A>,
}

#[derive(Clone)]
pub(crate) struct LeaderInfo<A: SyncIOAddress> {
    pub(crate) leader: Option<A>,
    pub(crate) path: Option<Vec<A>>,
    pub(crate) term: u64,
}

#[derive(Clone)]
pub(crate) struct PeerDetails<A: SyncIOAddress> {
    pub(crate) last_activity: Option<NonZeroU64>,
    pub(crate) last_global_activity: Option<NonZeroU64>,
    pub(crate) last_connect_attempt: Option<NonZeroU64>,
    pub(crate) last_connect_fail: Option<NonZeroU64>,
    pub(crate) repeat_connect_fails: u64,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) can_lead: bool,
    pub(crate) connected: bool,
    pub(crate) active_connections: u64,
    pub(crate) last_observation: Option<ElectionObservation<A>>,
}

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    pub(crate) fn fail(&self, reason: impl Into<String>) {
        let _ = self.status_updates.send(NodeStatus::Failed { reason: reason.into() });
    }
}
