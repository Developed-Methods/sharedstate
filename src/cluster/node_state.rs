use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use message_encoding::MessageEncoding;
use tokio::sync::watch;

use crate::{
    state::{deterministic_state::DeterministicState, subscribable_state::SubscribableState},
    transport::traits::SyncIOAddress,
    utils::unknown_id_err,
};

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub my_address: A,
    pub leader_address: watch::Receiver<A>,
    pub available_peers: watch::Receiver<Vec<A>>,
    pub state: SubscribableState<D>,
    connected_to_leader: AtomicBool,
    connected_peer: Mutex<Option<A>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebugInfo<A: SyncIOAddress> {
    pub my_address: A,
    pub leader_address: A,
    pub available_peers: Vec<A>,
    pub status: NodeStatus<A>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeStatus<A: SyncIOAddress> {
    Leader,
    Follower(FollowerStatus<A>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowerStatus<A: SyncIOAddress> {
    Disconnected,
    SubscribedToLeader,
    SubscribedToPeer { peer: A },
}

impl<A: SyncIOAddress> MessageEncoding for DebugInfo<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.my_address.write_to(out)?;
        sum += self.leader_address.write_to(out)?;
        sum += (self.available_peers.len() as u64).write_to(out)?;
        for peer in &self.available_peers {
            sum += peer.write_to(out)?;
        }
        sum += self.status.write_to(out)?;
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        let my_address = MessageEncoding::read_from(read)?;
        let leader_address = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut available_peers = Vec::with_capacity(len);
        for _ in 0..len {
            available_peers.push(MessageEncoding::read_from(read)?);
        }
        let status = MessageEncoding::read_from(read)?;
        Ok(Self {
            my_address,
            leader_address,
            available_peers,
            status,
        })
    }
}

impl<A: SyncIOAddress> MessageEncoding for NodeStatus<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        match self {
            Self::Leader => 0u16.write_to(out),
            Self::Follower(status) => Ok(1u16.write_to(out)? + status.write_to(out)?),
        }
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            0 => Ok(Self::Leader),
            1 => Ok(Self::Follower(MessageEncoding::read_from(read)?)),
            other => Err(unknown_id_err(other, "NodeStatus")),
        }
    }
}

impl<A: SyncIOAddress> MessageEncoding for FollowerStatus<A> {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        match self {
            Self::Disconnected => 0u16.write_to(out),
            Self::SubscribedToLeader => 1u16.write_to(out),
            Self::SubscribedToPeer { peer } => Ok(2u16.write_to(out)? + peer.write_to(out)?),
        }
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            0 => Ok(Self::Disconnected),
            1 => Ok(Self::SubscribedToLeader),
            2 => Ok(Self::SubscribedToPeer {
                peer: MessageEncoding::read_from(read)?,
            }),
            other => Err(unknown_id_err(other, "FollowerStatus")),
        }
    }
}

impl<A: SyncIOAddress> DebugInfo<A> {
    pub fn is_leader(&self) -> bool {
        matches!(self.status, NodeStatus::Leader)
    }

    pub fn is_connected_to_leader(&self) -> bool {
        !matches!(self.status, NodeStatus::Follower(FollowerStatus::Disconnected))
    }

    pub fn is_subscribed_to_leader(&self) -> bool {
        matches!(self.status, NodeStatus::Follower(FollowerStatus::SubscribedToLeader))
    }

    pub fn connected_peer(&self) -> Option<A> {
        match self.status {
            NodeStatus::Leader | NodeStatus::Follower(FollowerStatus::Disconnected) => None,
            NodeStatus::Follower(FollowerStatus::SubscribedToLeader) => Some(self.leader_address),
            NodeStatus::Follower(FollowerStatus::SubscribedToPeer { peer }) => Some(peer),
        }
    }
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
            connected_peer: Mutex::new(None),
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
        if !connected || self.is_leader() {
            *self.connected_peer.lock().unwrap() = None;
        }
        self.connected_to_leader
            .store(connected || self.is_leader(), Ordering::Release);
    }

    pub fn set_connected_peer(&self, peer: Option<A>) {
        *self.connected_peer.lock().unwrap() = peer;
    }

    pub fn connected_peer(&self) -> Option<A> {
        *self.connected_peer.lock().unwrap()
    }

    pub fn debug_info(&self) -> DebugInfo<A> {
        let leader_address = self.leader_address();
        let is_leader = self.my_address == leader_address;
        let is_connected_to_leader = self.is_connected_to_leader();
        let connected_peer = self.connected_peer();
        let status = match (is_leader, is_connected_to_leader, connected_peer) {
            (true, _, _) => NodeStatus::Leader,
            (false, true, Some(peer)) if peer == leader_address => {
                NodeStatus::Follower(FollowerStatus::SubscribedToLeader)
            }
            (false, true, Some(peer)) => NodeStatus::Follower(FollowerStatus::SubscribedToPeer { peer }),
            (false, _, _) => NodeStatus::Follower(FollowerStatus::Disconnected),
        };

        DebugInfo {
            my_address: self.my_address,
            leader_address,
            available_peers: self.available_peers(),
            status,
        }
    }
}
