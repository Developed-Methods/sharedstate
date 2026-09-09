use std::{
    io,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use message_encoding::MessageEncoding;
use tokio::time::Instant;

use crate::{
    cluster::{
        election::{EtcdElection, EtcdElectionConfig, Observation, Session},
        node_state::{ConnectStatus, ElectionStatus, NodeState},
        peer_connections::PeerConnections,
    },
    protocol::messages::{LeadershipEpoch, SyncRequest, SyncResponse},
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableStateAction},
    transport::traits::SyncIO,
    utils::unique_state_id,
};

pub use crate::protocol::messages::LeaderMode;

pub struct EtcdLeaderTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peers: Arc<PeerConnections<I, D>>,
    config: EtcdElectionConfig,
}

impl<I, D> EtcdLeaderTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        peers: Arc<PeerConnections<I, D>>,
        config: EtcdElectionConfig,
    ) -> Self {
        Self { state, peers, config }
    }

    pub async fn run(self) {
        loop {
            let result = match EtcdElection::connect(self.config.clone()).await {
                Ok(mut election) => self.participate(&mut election).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                self.state.revoke_authority(Some(error.to_string()));
                tracing::warn!(%error, "etcd election unavailable");
            }
            tokio::time::sleep(self.retry_delay()).await;
        }
    }

    fn retry_delay(&self) -> Duration {
        let mut random = [0; 2];
        let _ = getrandom::fill(&mut random);
        Duration::from_millis(100 + u16::from_le_bytes(random) as u64 % 400)
    }

    async fn participate(&self, election: &mut EtcdElection<I::Address>) -> io::Result<()> {
        loop {
            let observation = election.observe().await?;
            let vacant = observation.owner.is_none();
            self.observe(observation).await;
            if vacant && self.state.can_lead && self.state.eligible.load(Ordering::Acquire) {
                tokio::time::sleep(self.retry_delay()).await;
                if let Some(mut session) = election.acquire(self.state.my_address).await? {
                    self.publish_local(&session);
                    self.own(election, &mut session).await?;
                }
            }
        }
    }

    async fn observe(&self, observation: Observation<I::Address>) {
        let status = if let Some(owner) = observation.owner {
            if owner.address != self.state.my_address {
                self.state.note_known_peer_activity(owner.address).await;
            }
            ElectionStatus {
                owner: Some(owner.address),
                epoch: owner.epoch,
                ready: owner.ready,
                valid_until: Some(observation.valid_until),
                permit: None,
                error: None,
            }
        } else {
            ElectionStatus::default()
        };
        self.state.set_election_status(status);
    }

    fn publish_local(&self, session: &Session<I::Address>) {
        self.state.set_election_status(ElectionStatus {
            owner: Some(session.owner.address),
            epoch: session.owner.epoch,
            ready: session.owner.ready,
            valid_until: Some(session.permit.deadline()),
            permit: Some(session.permit.clone()),
            error: None,
        });
    }

    async fn own(&self, election: &mut EtcdElection<I::Address>, session: &mut Session<I::Address>) -> io::Result<()> {
        let preparation = self.prepare(session.owner.epoch);
        tokio::pin!(preparation);
        let promotion_deadline = Instant::now() + self.config.promotion_timeout;
        let mut prepared = false;
        let mut preparation_done = false;
        let mut next_renewal = Instant::now() + self.config.renewal_interval;
        let result = async {
        loop {
            if !session.permit.valid() { return Err(io::Error::other("leadership permit expired")); }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(session.permit.deadline()) => {
                    return Err(io::Error::other("leadership permit expired"));
                }
                _ = tokio::time::sleep_until(next_renewal) => {
                    election.renew(session).await?;
                    self.publish_local(session);
                    next_renewal = Instant::now() + self.config.renewal_interval;
                }
                _ = tokio::time::sleep_until(promotion_deadline), if !prepared => {
                    return Err(io::Error::other("promotion timed out; recover state before retrying"));
                }
                result = &mut preparation, if !preparation_done => {
                    preparation_done = true;
                    result?;
                    election.publish_ready(session).await?;
                    if !session.permit.valid() { return Err(io::Error::other("permit expired while publishing readiness")); }
                    prepared = true;
                    self.publish_local(session);
                }
                observation = election.observe() => {
                    let observation = observation?;
                    if observation.owner.as_ref() != Some(&session.owner) {
                        return Err(io::Error::other("etcd ownership changed"));
                    }
                }
            }
        }
        }.await;
        // Finish admitted state mutations after revocation to preserve state and broadcast consistency.
        self.state
            .revoke_authority(result.as_ref().err().map(ToString::to_string));
        session.permit.revoke();
        let _ = election.revoke(session).await;
        if !preparation_done {
            let _ = preparation.await;
        }
        result
    }

    async fn prepare(&self, epoch: LeadershipEpoch) -> io::Result<()> {
        // Resolve available peer metadata before comparing histories; the promotion deadline bounds this wait.
        loop {
            if !self.state.election_status().valid() {
                return Err(io::Error::other("permit expired while awaiting recovery metadata"));
            }
            let pending = self.state.peers.lock().await.values().any(|peer| {
                peer.leader_info.is_none() && !matches!(peer.connect_status, ConnectStatus::FailedToConnect { .. })
            });
            if !pending {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut local = self.state.state.settled_recovery_details().await;
        let peers = self
            .state
            .peers
            .lock()
            .await
            .values()
            .filter_map(|peer| {
                peer.leader_info
                    .as_ref()
                    .filter(|info| info.recovery_initialized)
                    .map(|info| (peer.addr, info.recovery_details.clone()))
            })
            .collect::<Vec<_>>();
        let mut required = Vec::new();
        for (address, details) in &peers {
            if local.can_recover_follower(details) {
                continue;
            }
            if !details.can_recover_follower(&local) {
                return Err(io::Error::other("promotion blocked by conflicting recovery histories"));
            }
            required.push(*address);
        }
        for address in required {
            let response = self
                .peers
                .send_rpc(address, SyncRequest::RecoverySnapshot)
                .await
                .map_err(|error| io::Error::other(format!("promotion recovery failed: {error:?}")))?;
            let SyncResponse::RecoverySnapshot(snapshot) = response else {
                return Err(io::Error::other("promotion source rejected snapshot request"));
            };
            if local.can_recover_follower(snapshot.details()) {
                continue;
            }
            if !snapshot.details().can_recover_follower(&local) {
                return Err(io::Error::other("promotion snapshot conflicts with local history"));
            }
            local = snapshot.details().clone();
            if !self.state.reset_preparing(epoch, snapshot).await {
                return Err(io::Error::other("permit expired during promotion recovery"));
            }
        }
        let latest_peers = self
            .state
            .peers
            .lock()
            .await
            .values()
            .filter_map(|peer| peer.leader_info.as_ref())
            .filter(|info| info.recovery_initialized)
            .map(|info| info.recovery_details.clone())
            .collect::<Vec<_>>();
        if latest_peers.iter().any(|details| !local.can_recover_follower(details)) {
            return Err(io::Error::other("promotion cannot recover known peer histories"));
        }
        if !self
            .state
            .update_preparing(
                epoch,
                RecoverableStateAction::BumpGeneration {
                    new_id: unique_state_id(&self.state.my_address),
                },
            )
            .await
        {
            return Err(io::Error::other("permit expired before promotion generation bump"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cluster::{node_state::PeerState, rpc_server::RpcServer},
        protocol::messages::{LeaderInfo, LeaderState},
        state::{recoverable_state::RecoverableState, subscribable_state::SubscribableState},
        transport::{
            channels::NetIoSettings,
            simulated::{SimulatedIo, SimulatedNet},
        },
    };
    use std::{
        collections::HashMap,
        sync::{RwLock, atomic::AtomicBool},
    };
    use tokio::sync::{Mutex, Notify, mpsc};

    #[derive(Clone)]
    struct Counter(u64);
    impl DeterministicState for Counter {
        type Action = u64;
        type AuthorityAction = u64;
        fn accept_seq(&self) -> u64 {
            self.0
        }
        fn authority(&self, action: u64) -> u64 {
            action
        }
        fn update(&mut self, _: &u64) {
            self.0 += 1;
        }
    }
    impl MessageEncoding for Counter {
        fn write_to<W: io::Write>(&self, out: &mut W) -> io::Result<usize> {
            self.0.write_to(out)
        }
        fn read_from<R: io::Read>(read: &mut R) -> io::Result<Self> {
            Ok(Self(u64::read_from(read)?))
        }
    }
    fn node(address: u64, state: RecoverableState<Counter>) -> Arc<NodeState<u64, Counter>> {
        Arc::new(NodeState {
            my_address: address,
            can_lead: true,
            peers: Mutex::new(HashMap::new()),
            state: SubscribableState::new(state, Default::default()).unwrap(),
            election: RwLock::new(Default::default()),
            leadership_changed: Notify::new(),
            eligible: AtomicBool::new(true),
            synced_epoch: RwLock::new(None),
        })
    }
    async fn add_peer(node: &NodeState<u64, Counter>, peer: &NodeState<u64, Counter>) {
        let mut entry = PeerState::empty(peer.my_address);
        entry.leader_info = Some(LeaderInfo {
            leader_state: LeaderState {
                epoch: Default::default(),
                mode: LeaderMode::NoLeader,
            },
            can_lead: true,
            recovery_initialized: true,
            recovery_details: peer.state.recovery_details().await,
        });
        node.peers.lock().await.insert(peer.my_address, entry);
    }

    #[tokio::test]
    async fn lagging_owner_recovers_and_bumps_once_before_ready() {
        let etcd = crate::test_support::TestEtcd::start().await;
        let net = SimulatedNet::new();
        let io1 = net.start_io(1).await;
        let io2 = net.start_io(2).await;
        let initial = RecoverableState::new(123, Counter(0));
        let candidate = node(1, initial.clone());
        let mut advanced = initial.clone();
        advanced.update(&RecoverableStateAction::StateAction { action: 1 });
        let source = node(2, advanced);
        add_peer(&candidate, &source).await;
        let settings = NetIoSettings::default();
        let (tx, _rx) = mpsc::channel(16);
        let server = Arc::new(RpcServer::new(source, tx)).start_listener(io2, settings.clone());
        let connections = Arc::new(PeerConnections::<SimulatedIo, Counter>::new(io1, settings, candidate.clone()));
        let task = EtcdLeaderTask::new(candidate.clone(), connections, etcd.config(false));
        let mut election = EtcdElection::connect(etcd.config(false)).await.unwrap();
        let mut session = election.acquire(1).await.unwrap().unwrap();
        task.publish_local(&session);
        assert!(!candidate.valid_authority(session.owner.epoch));
        task.prepare(session.owner.epoch).await.unwrap();
        assert!(!election.read().await.unwrap().owner.unwrap().ready);
        let settled = candidate.state.settled_recovery_details().await;
        assert_eq!(settled.next_seq(), initial.details().next_seq() + 2);
        assert_eq!(candidate.state.create_handle().read_with(|v| v.state().0), 1);
        election.publish_ready(&mut session).await.unwrap();
        task.publish_local(&session);
        assert!(candidate.valid_authority(session.owner.epoch));
        election.revoke(&session).await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn conflicting_known_histories_block_promotion() {
        let etcd = crate::test_support::TestEtcd::start().await;
        let net = SimulatedNet::new();
        let io = net.start_io(1).await;
        let candidate = node(1, RecoverableState::new(123, Counter(0)));
        let conflict = node(2, RecoverableState::new(456, Counter(0)));
        add_peer(&candidate, &conflict).await;
        let peers = Arc::new(PeerConnections::new(io, Default::default(), candidate.clone()));
        let task = EtcdLeaderTask::new(candidate.clone(), peers, etcd.config(false));
        let mut election = EtcdElection::connect(etcd.config(false)).await.unwrap();
        let session = election.acquire(1).await.unwrap().unwrap();
        task.publish_local(&session);
        assert!(task.prepare(session.owner.epoch).await.is_err());
        assert!(!candidate.valid_authority(session.owner.epoch));
        assert!(!election.read().await.unwrap().owner.unwrap().ready);
        election.revoke(&session).await.unwrap();
    }
}
