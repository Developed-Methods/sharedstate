use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

use crate::{
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub leader_address: watch::Receiver<A>,
    pub available_peers: watch::Receiver<Vec<A>>,
    pub state: SubscribableState<D>,
    connected_to_leader: AtomicBool,
}

impl<A, D> NodeState<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    pub fn new(
        my_address: A,
        leader_address: watch::Receiver<A>,
        available_peers: watch::Receiver<Vec<A>>,
        state: SubscribableState<D>,
    ) -> Self {
        let is_leader = my_address == *leader_address.borrow();
        Self {
            my_address,
            leader_address,
            available_peers,
            state,
            connected_to_leader: AtomicBool::new(is_leader),
        }
    }

    pub fn leader_address(&self) -> A {
        *self.leader_address.borrow()
    }

    pub fn available_peers(&self) -> Vec<A> {
        self.available_peers.borrow().clone()
    }

    pub fn is_leader(&self) -> bool {
        self.my_address == self.leader_address()
    }

    pub fn is_connected_to_leader(&self) -> bool {
        self.connected_to_leader.load(Ordering::Acquire)
    }

    pub fn set_connected_to_leader(&self, connected: bool) {
        self.connected_to_leader
            .store(connected || self.is_leader(), Ordering::Release);
    }
}
