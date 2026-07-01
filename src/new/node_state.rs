use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    new::{subscribable_state::SubscribableState, tasks::current_leader::CurrentLeaderStatus},
    protocol::messages::LeaderWithElectionInfo,
    state::determinstic_state::DeterministicState,
    transport::traits::SyncIOAddress,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub can_lead: bool,
    pub peers: Mutex<HashMap<A, PeerState<A>>>,
    pub state: SubscribableState<D>,
    pub leader_status: Arc<CurrentLeaderStatus<A>>,
}

pub struct PeerState<A: SyncIOAddress> {
    pub addr: A,
    pub latency: Option<NonZeroU64>,
    pub can_lead: Option<bool>,
    pub is_connected: bool,
    pub last_global_connectivity: Option<NonZeroU64>,
    pub leader_observation: Option<LeaderWithElectionInfo<A>>,
}
