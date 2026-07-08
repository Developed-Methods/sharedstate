use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub leader_address: A,
    pub state: SubscribableState<D>,
    connected_to_leader: AtomicBool,
}

impl<A, D> NodeState<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    pub fn new(my_address: A, leader_address: A, state: SubscribableState<D>) -> Self {
        let is_leader = my_address == leader_address;
        Self {
            my_address,
            leader_address,
            state,
            connected_to_leader: AtomicBool::new(is_leader),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.my_address == self.leader_address
    }

    pub fn is_connected_to_leader(&self) -> bool {
        self.connected_to_leader.load(Ordering::Acquire)
    }

    pub fn set_connected_to_leader(&self, connected: bool) {
        self.connected_to_leader
            .store(connected || self.is_leader(), Ordering::Release);
    }
}
