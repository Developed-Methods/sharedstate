use std::collections::HashSet;

use tokio::sync::Mutex;

use crate::{
    protocol::messages::LeaderWithElectionInfo, state::recoverable_state::RecoverableStateDetails,
    transport::traits::SyncIOAddress,
};

pub struct CurrentLeaderStatus<A: SyncIOAddress> {
    local: A,
    state: Mutex<LeaderMode<A>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderMode<A: SyncIOAddress> {
    NoLeader { term: u64 },
    Electing { term: u64 },
    Leading { term: u64, path: Vec<A> },
    Following { term: u64, leader: A, path: Vec<A>, via: A },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderStatusSnapshot<A: SyncIOAddress> {
    pub mode: LeaderMode<A>,
}

impl<A: SyncIOAddress> CurrentLeaderStatus<A> {
    pub fn new(local: A) -> Self {
        Self {
            local,
            state: Mutex::new(LeaderMode::NoLeader { term: 0 }),
        }
    }

    pub async fn snapshot(&self) -> LeaderStatusSnapshot<A> {
        LeaderStatusSnapshot {
            mode: self.state.lock().await.clone(),
        }
    }

    pub async fn current_term(&self) -> u64 {
        self.state.lock().await.term()
    }

    pub async fn leader(&self) -> Option<A> {
        self.state.lock().await.leader()
    }

    pub async fn path_to_leader(&self) -> Option<Vec<A>> {
        self.state.lock().await.path().cloned()
    }

    pub async fn begin_election(&self, observed_term: u64) -> u64 {
        let mut state = self.state.lock().await;
        let term = state.term().max(observed_term) + 1;
        *state = LeaderMode::Electing { term };
        term
    }

    pub async fn promote_self(&self, term: u64) {
        let mut state = self.state.lock().await;
        if term >= state.term() {
            *state = LeaderMode::Leading {
                term,
                path: vec![self.local],
            };
        }
    }

    pub async fn follow_remote(&self, leader: A, term: u64, path: Vec<A>, via: A) -> bool {
        if !valid_local_path(Some(leader), &path, self.local) {
            return false;
        }

        let mut state = self.state.lock().await;
        if term < state.term() {
            return false;
        }

        *state = LeaderMode::Following {
            term,
            leader,
            path,
            via,
        };
        true
    }

    pub async fn clear_if_leader(&self, leader: A) -> bool {
        let mut state = self.state.lock().await;
        if state.leader() != Some(leader) {
            return false;
        }

        let term = state.term();
        *state = LeaderMode::NoLeader { term };
        true
    }

    pub async fn clear_if_via(&self, via: A) -> bool {
        let mut state = self.state.lock().await;
        let LeaderMode::Following { via: current_via, .. } = *state else {
            return false;
        };
        if current_via != via {
            return false;
        }

        let term = state.term();
        *state = LeaderMode::NoLeader { term };
        true
    }

    pub async fn local_observation(
        &self,
        can_lead: bool,
        reachable_can_lead: Vec<A>,
        recover_details: RecoverableStateDetails,
    ) -> LeaderWithElectionInfo<A> {
        let state = self.state.lock().await;
        LeaderWithElectionInfo {
            observer: self.local,
            term: state.term(),
            leader: state.leader(),
            leader_path: state.path().cloned(),
            can_lead,
            reachable_can_lead,
            recover_details,
        }
    }
}

impl<A: SyncIOAddress> LeaderMode<A> {
    pub fn term(&self) -> u64 {
        match self {
            LeaderMode::NoLeader { term }
            | LeaderMode::Electing { term }
            | LeaderMode::Leading { term, .. }
            | LeaderMode::Following { term, .. } => *term,
        }
    }

    pub fn leader(&self) -> Option<A> {
        match self {
            LeaderMode::Leading { path, .. } => path.first().copied(),
            LeaderMode::Following { leader, .. } => Some(*leader),
            LeaderMode::NoLeader { .. } | LeaderMode::Electing { .. } => None,
        }
    }

    pub fn path(&self) -> Option<&Vec<A>> {
        match self {
            LeaderMode::Leading { path, .. } | LeaderMode::Following { path, .. } => Some(path),
            LeaderMode::NoLeader { .. } | LeaderMode::Electing { .. } => None,
        }
    }
}

fn valid_local_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader || path.last().copied() != Some(local) {
        return false;
    }

    let mut seen = HashSet::new();
    path.iter().all(|item| seen.insert(*item))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_without_leader() {
        let status = CurrentLeaderStatus::new(1);

        assert_eq!(status.snapshot().await.mode, LeaderMode::NoLeader { term: 0 });
        assert_eq!(status.leader().await, None);
    }

    #[tokio::test]
    async fn promote_self_sets_leading_path() {
        let status = CurrentLeaderStatus::new(1);

        status.promote_self(2).await;

        assert_eq!(status.snapshot().await.mode, LeaderMode::Leading { term: 2, path: vec![1] });
    }

    #[tokio::test]
    async fn follow_remote_sets_following_path() {
        let status = CurrentLeaderStatus::new(3);

        assert!(status.follow_remote(1, 2, vec![1, 2, 3], 2).await);

        assert_eq!(
            status.snapshot().await.mode,
            LeaderMode::Following {
                term: 2,
                leader: 1,
                path: vec![1, 2, 3],
                via: 2,
            }
        );
    }

    #[tokio::test]
    async fn lower_term_follow_does_not_override_leading() {
        let status = CurrentLeaderStatus::new(1);
        status.promote_self(3).await;

        assert!(!status.follow_remote(2, 2, vec![2, 1], 2).await);

        assert_eq!(status.snapshot().await.mode, LeaderMode::Leading { term: 3, path: vec![1] });
    }

    #[tokio::test]
    async fn clear_if_via_removes_following_status() {
        let status = CurrentLeaderStatus::new(3);
        status.follow_remote(1, 2, vec![1, 2, 3], 2).await;

        assert!(status.clear_if_via(2).await);

        assert_eq!(status.snapshot().await.mode, LeaderMode::NoLeader { term: 2 });
    }
}
