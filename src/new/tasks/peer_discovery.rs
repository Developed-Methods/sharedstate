use std::{num::NonZeroU64, sync::Arc, time::Duration};

use futures_util::{stream, StreamExt};
use message_encoding::MessageEncoding;
use tokio::sync::Mutex;

use crate::{
    new::{
        node_state::{NodeState, PeerState},
        subscribable_state::StateHandle,
        tasks::{current_leader::local_leader_observation, peer_connections::PeerConnections},
    },
    protocol::messages::{SharePeerDetails, SyncRequest, SyncResponse},
    state::determinstic_state::DeterministicState,
    transport::traits::{SyncIO, SyncIOAddress},
    utils::now_ms,
};

pub struct PeerDiscoveryTask<I: SyncIO, D: DeterministicState> {
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    timing: PeerDiscoveryTiming,
    state_handle: Mutex<StateHandle<D>>,
}

#[derive(Clone, Debug)]
pub struct PeerDiscoveryTiming {
    pub observation_interval: Duration,
    pub max_concurrent_observations: usize,
}

impl Default for PeerDiscoveryTiming {
    fn default() -> Self {
        Self {
            observation_interval: Duration::from_secs(3),
            max_concurrent_observations: 8,
        }
    }
}

impl<I, D> PeerDiscoveryTask<I, D>
where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(
        state: Arc<NodeState<I::Address, D>>,
        peer_connections: Arc<PeerConnections<I, D>>,
        timing: PeerDiscoveryTiming,
    ) -> Self {
        let state_handle = Mutex::new(state.state.create_handle());
        Self {
            state,
            peer_connections,
            timing,
            state_handle,
        }
    }

    pub async fn run(self) {
        tracing::debug!(
            local = ?self.state.my_address,
            interval_ms = self.timing.observation_interval.as_millis(),
            max_concurrent = self.timing.max_concurrent_observations,
            "starting peer discovery task",
        );

        loop {
            self.tick().await;
            tokio::time::sleep(self.timing.observation_interval).await;
        }
    }

    pub async fn tick(&self) {
        tracing::debug!(local = ?self.state.my_address, "starting peer discovery tick");
        self.observe_peers().await;
        tracing::debug!(local = ?self.state.my_address, "finished peer discovery tick");
    }

    pub async fn observe_peers(&self) {
        let targets = self.observation_targets().await;
        if targets.is_empty() {
            tracing::debug!(local = ?self.state.my_address, "no peer discovery observation targets");
            return;
        }

        let share_peers = self.local_share_peers().await;
        let local_observation = local_leader_observation(&self.state, &self.state_handle).await;
        let max_concurrent = self.timing.max_concurrent_observations.max(1);
        tracing::debug!(
            local = ?self.state.my_address,
            ?targets,
            share_peer_count = share_peers.len(),
            max_concurrent,
            "observing peers",
        );

        stream::iter(targets.into_iter().map(|peer| {
            let state = self.state.clone();
            let peer_connections = self.peer_connections.clone();
            let share_peers = share_peers.clone();
            let local_observation = local_observation.clone();

            async move {
                observe_peer(state, peer_connections, peer, share_peers, local_observation).await;
            }
        }))
        .buffer_unordered(max_concurrent)
        .collect::<Vec<_>>()
        .await;
    }

    async fn observation_targets(&self) -> Vec<I::Address> {
        let leader_path = self.state.leader_status.path_to_leader().await;
        let peers = self.state.peers.lock().await;

        let mut targets = if self.state.can_lead || leader_path.is_none() {
            peers.keys().copied().collect::<Vec<_>>()
        } else {
            let mut targets = leader_path.clone().unwrap_or_default();
            targets.extend(
                peers
                    .values()
                    .filter_map(|peer| (peer.can_lead == Some(true)).then_some(peer.addr)),
            );
            targets
        };

        targets.retain(|addr| *addr != self.state.my_address);
        targets.sort();
        targets.dedup();
        tracing::debug!(
            local = ?self.state.my_address,
            can_lead = self.state.can_lead,
            leader_path = ?leader_path,
            ?targets,
            "selected peer discovery observation targets",
        );
        targets
    }

    async fn local_share_peers(&self) -> Vec<SharePeerDetails<I::Address>> {
        let peers = self.state.peers.lock().await;
        let mut share = Vec::with_capacity(peers.len() + 1);
        share.push(SharePeerDetails {
            address: self.state.my_address,
            can_be_leader: Some(self.state.can_lead),
            last_global_activity: NonZeroU64::new(now_ms()),
        });
        share.extend(peers.values().map(|peer| SharePeerDetails {
            address: peer.addr,
            can_be_leader: peer.can_lead,
            last_global_activity: peer.last_global_connectivity,
        }));
        tracing::debug!(
            local = ?self.state.my_address,
            share_peer_count = share.len(),
            "built local peer discovery share list",
        );
        share
    }
}

async fn observe_peer<I, D>(
    state: Arc<NodeState<I::Address, D>>,
    peer_connections: Arc<PeerConnections<I, D>>,
    peer: I::Address,
    share_peers: Vec<SharePeerDetails<I::Address>>,
    local_observation: crate::protocol::messages::LeaderWithElectionInfo<I::Address>,
) where
    I: SyncIO,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    tracing::debug!(local = ?state.my_address, ?peer, "observing peer");
    let started = now_ms();
    match peer_connections.send_rpc(peer, SyncRequest::Ping(started)).await {
        Ok(SyncResponse::Pong(id)) if id == started => {
            let latency = NonZeroU64::new(now_ms().saturating_sub(started).max(1));
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                latency_ms = latency.map(NonZeroU64::get),
                "peer ping succeeded",
            );
            mark_peer_observed(&state, peer, latency).await;
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "peer ping returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
            return;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "peer ping failed",
            );
            clear_failed_observation(&state, peer).await;
            return;
        }
    }

    match peer_connections
        .send_rpc(peer, SyncRequest::SharePeers(share_peers))
        .await
    {
        Ok(SyncResponse::Peers(peers)) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                peer_count = peers.len(),
                "peer shared peer details",
            );
            merge_peer_details(&state, peers).await;
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "share peers returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
            return;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "share peers request failed",
            );
            clear_failed_observation(&state, peer).await;
            return;
        }
    }

    match peer_connections
        .send_rpc(
            peer,
            SyncRequest::LeaderInformation {
                source: state.my_address,
                info: local_observation,
            },
        )
        .await
    {
        Ok(SyncResponse::Ok) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                "peer accepted local leader information",
            );
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "leader information push returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
            return;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "leader information push failed",
            );
            clear_failed_observation(&state, peer).await;
            return;
        }
    }

    match peer_connections.send_rpc(peer, SyncRequest::ShareLeaderInfo).await {
        Ok(SyncResponse::LeaderInfo(info)) if info.observer == peer => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                observer = ?info.observer,
                leader = ?info.leader,
                leader_path = ?info.leader_path,
                term = info.term,
                can_lead = info.can_lead,
                reachable_can_lead = ?info.reachable_can_lead,
                "peer shared leader info",
            );
            record_leader_observation(&state, peer, info).await;
        }
        Ok(response) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                response = response_name(&response),
                "share leader info returned unexpected response",
            );
            peer_connections.kill_connection(peer).await;
            clear_failed_observation(&state, peer).await;
        }
        Err(error) => {
            tracing::debug!(
                local = ?state.my_address,
                ?peer,
                ?error,
                "share leader info request failed",
            );
            clear_failed_observation(&state, peer).await;
        }
    }
}

async fn record_leader_observation<A, D>(
    state: &NodeState<A, D>,
    peer: A,
    info: crate::protocol::messages::LeaderWithElectionInfo<A>,
) where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let mut peers = state.peers.lock().await;
    let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
    peer_state.can_lead = Some(info.can_lead);
    peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
    peer_state.leader_observation = Some(info);
}

async fn mark_peer_observed<A, D>(state: &NodeState<A, D>, peer: A, latency: Option<NonZeroU64>)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let mut peers = state.peers.lock().await;
    let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
    peer_state.latency = latency;
    peer_state.last_global_connectivity = NonZeroU64::new(now_ms());
    tracing::debug!(
        local = ?state.my_address,
        ?peer,
        latency_ms = latency.map(NonZeroU64::get),
        "marked peer connected",
    );
}

async fn clear_failed_observation<A, D>(state: &NodeState<A, D>, peer: A)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    {
        let mut peers = state.peers.lock().await;
        let peer_state = peers.entry(peer).or_insert_with(|| empty_peer_state(peer));
        peer_state.leader_observation = None;
    }
    let cleared_via = state.leader_status.clear_if_via(peer).await;
    let cleared_leader = state.leader_status.clear_if_leader(peer).await;
    tracing::debug!(
        local = ?state.my_address,
        ?peer,
        cleared_via,
        cleared_leader,
        "cleared failed peer observation",
    );
}

async fn merge_peer_details<A, D>(state: &NodeState<A, D>, shared_peers: Vec<SharePeerDetails<A>>)
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    let mut peers = state.peers.lock().await;
    let shared_count = shared_peers.len();
    let mut inserted = 0usize;
    let mut updated = 0usize;
    for shared in shared_peers {
        if shared.address == state.my_address {
            continue;
        }

        let peer_state = peers.entry(shared.address).or_insert_with(|| {
            inserted += 1;
            empty_peer_state(shared.address)
        });
        updated += 1;
        if let Some(can_lead) = shared.can_be_leader {
            peer_state.can_lead = Some(can_lead);
        }
        peer_state.last_global_connectivity = match (peer_state.last_global_connectivity, shared.last_global_activity) {
            (None, Some(activity)) | (Some(activity), None) => Some(activity),
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, None) => None,
        };
    }
    tracing::debug!(
        local = ?state.my_address,
        shared_count,
        inserted,
        updated,
        "merged shared peer details",
    );
}

fn response_name<A: SyncIOAddress, D: DeterministicState>(response: &SyncResponse<A, D>) -> &'static str {
    match response {
        SyncResponse::Pong(_) => "Pong",
        SyncResponse::Ok => "Ok",
        SyncResponse::FailedToQueueAction { .. } => "FailedToQueueAction",
        SyncResponse::Peers(_) => "Peers",
        SyncResponse::LeaderInfo(_) => "LeaderInfo",
        SyncResponse::LeaderPath(_) => "LeaderPath",
        SyncResponse::NoPathToLeader => "NoPathToLeader",
        SyncResponse::Accepted(_) => "Accepted",
        SyncResponse::RecoveryFailed => "RecoveryFailed",
        SyncResponse::FreshState(_) => "FreshState",
        SyncResponse::AuthorityAction(_, _) => "AuthorityAction",
        SyncResponse::ActionStreamClosed => "ActionStreamClosed",
        SyncResponse::UnexpectedRequest => "UnexpectedRequest",
    }
}

fn empty_peer_state<A: SyncIOAddress>(addr: A) -> PeerState<A> {
    PeerState {
        addr,
        latency: None,
        can_lead: None,
        is_connected: false,
        last_global_connectivity: None,
        leader_observation: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Result, sync::Arc, time::Duration};

    use message_encoding::MessageEncoding;
    use sequenced_broadcast::SequencedBroadcastSettings;
    use tokio::sync::{mpsc::Receiver, mpsc::Sender, Mutex};

    use super::*;
    use crate::{
        new::{
            subscribable_state::SubscribableState,
            tasks::{
                current_leader::{CurrentLeaderStatus, LeaderMode},
                peer_connections::PeerConnections,
            },
        },
        protocol::messages::{LeaderWithElectionInfo, PROTOCOL_VERSION},
        state::recoverable_state::{RecoverableState, RecoverableStateDetails},
        transport::{
            channels::NetIoSettings,
            simulated::{SimulatedIo, SimulatedNet},
            traits::SyncIOListener,
        },
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestState(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAction(u64);

    impl DeterministicState for TestState {
        type Action = TestAction;
        type AuthorityAction = TestAction;

        fn accept_seq(&self) -> u64 {
            self.0
        }

        fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
            action
        }

        fn update(&mut self, _action: &Self::AuthorityAction) {
            self.0 += 1;
        }
    }

    impl MessageEncoding for TestState {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    impl MessageEncoding for TestAction {
        fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    fn settings() -> NetIoSettings {
        NetIoSettings {
            process_timeout: Duration::from_millis(100),
            message_timeout: Duration::from_millis(100),
        }
    }

    fn node_state(address: u64, can_lead: bool, peers: HashMap<u64, PeerState<u64>>) -> Arc<NodeState<u64, TestState>> {
        Arc::new(NodeState {
            my_address: address,
            can_lead,
            peers: Mutex::new(peers),
            state: SubscribableState::new(
                RecoverableState::new(address, TestState(1)),
                SequencedBroadcastSettings::default(),
            )
            .unwrap(),
            leader_status: Arc::new(CurrentLeaderStatus::new(address)),
        })
    }

    async fn simulated_task(
        state: Arc<NodeState<u64, TestState>>,
        remote_address: u64,
    ) -> (PeerDiscoveryTask<SimulatedIo, TestState>, Arc<PeerConnections<SimulatedIo, TestState>>, Arc<SimulatedIo>)
    {
        let net = SimulatedNet::new();
        let local_io = net.start_io(state.my_address).await;
        let remote_io = net.start_io(remote_address).await;
        let connections = Arc::new(PeerConnections::new(local_io, settings(), state.clone()));
        let task = PeerDiscoveryTask::new(state, connections.clone(), PeerDiscoveryTiming::default());
        (task, connections, remote_io)
    }

    async fn accept_server(
        remote_io: &SimulatedIo,
    ) -> (Sender<SyncResponse<u64, TestState>>, Receiver<SyncRequest<u64, TestState>>) {
        let conn = tokio::time::timeout(Duration::from_secs(1), remote_io.next_client())
            .await
            .unwrap()
            .unwrap();
        let (_remote, write, read) = conn.server_channels::<TestState>(settings());
        (write, read)
    }

    async fn recv_request(read: &mut Receiver<SyncRequest<u64, TestState>>) -> SyncRequest<u64, TestState> {
        tokio::time::timeout(Duration::from_secs(1), read.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn expect_handshake(
        write: &Sender<SyncResponse<u64, TestState>>,
        read: &mut Receiver<SyncRequest<u64, TestState>>,
        expected_local: u64,
    ) {
        match recv_request(read).await {
            SyncRequest::ProtocolVersion(version) => assert_eq!(version, PROTOCOL_VERSION),
            other => panic!("expected protocol version request, got {other:?}"),
        }
        write.send(SyncResponse::Ok).await.unwrap();

        match recv_request(read).await {
            SyncRequest::MyAddress(address) => assert_eq!(address, expected_local),
            other => panic!("expected my-address request, got {other:?}"),
        }
        write.send(SyncResponse::Ok).await.unwrap();
    }

    fn one_peer_task_state() -> Arc<NodeState<u64, TestState>> {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), false));
        node_state(1, true, peers)
    }

    async fn expect_ping_share_and_leader_push(
        write: &Sender<SyncResponse<u64, TestState>>,
        read: &mut Receiver<SyncRequest<u64, TestState>>,
    ) {
        match recv_request(read).await {
            SyncRequest::Ping(id) => write.send(SyncResponse::Pong(id)).await.unwrap(),
            other => panic!("expected ping request, got {other:?}"),
        }

        match recv_request(read).await {
            SyncRequest::SharePeers(_) => write.send(SyncResponse::Peers(Vec::new())).await.unwrap(),
            other => panic!("expected share-peers request, got {other:?}"),
        }

        match recv_request(read).await {
            SyncRequest::LeaderInformation { source, info } => {
                assert_eq!(source, 1);
                assert_eq!(info.observer, 1);
                write.send(SyncResponse::Ok).await.unwrap();
            }
            other => panic!("expected leader-information request, got {other:?}"),
        }
    }

    fn peer(addr: u64, can_lead: Option<bool>, is_connected: bool) -> PeerState<u64> {
        PeerState {
            addr,
            latency: None,
            can_lead,
            is_connected,
            last_global_connectivity: is_connected.then(|| NonZeroU64::new(now_ms()).unwrap()),
            leader_observation: None,
        }
    }

    fn leader_observation(observer: u64, term: u64, leader: u64, path: Vec<u64>) -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer,
            term,
            leader: Some(leader),
            leader_path: Some(path),
            can_lead: true,
            reachable_can_lead: vec![observer],
            recover_details: RecoverableStateDetails::new(observer, 1),
        }
    }

    #[tokio::test]
    async fn clear_failed_observation_clears_path_state_without_changing_connected() {
        let mut peers = HashMap::new();
        peers.insert(2, peer(2, Some(true), true));
        let state = node_state(1, false, peers);
        assert!(state.leader_status.follow_remote(2, 3, vec![2, 1], 2).await);

        clear_failed_observation(&state, 2).await;

        assert_eq!(state.leader_status.snapshot().await.mode, LeaderMode::NoLeader { term: 3 });
        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| peer.is_connected));
    }

    #[tokio::test]
    async fn discovers_peers_from_share_peers() {
        let state = node_state(1, true, HashMap::new());

        merge_peer_details(
            &state,
            vec![SharePeerDetails {
                address: 2,
                can_be_leader: Some(true),
                last_global_activity: NonZeroU64::new(10),
            }],
        )
        .await;

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.can_lead, Some(true));
        assert_eq!(peer.last_global_connectivity, NonZeroU64::new(10));
    }

    #[tokio::test]
    async fn records_leader_observation() {
        let state = node_state(1, true, HashMap::new());

        record_leader_observation(&state, 2, leader_observation(2, 3, 2, vec![2])).await;

        let peers = state.peers.lock().await;
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.can_lead, Some(true));
        assert_eq!(peer.leader_observation.as_ref().unwrap().leader, Some(2));
    }

    #[tokio::test]
    async fn pushes_local_leader_information_to_peer() {
        let state = one_peer_task_state();
        let (task, _connections, remote_io) = simulated_task(state.clone(), 2).await;
        let task_handle = tokio::spawn(async move {
            task.observe_peers().await;
        });

        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        expect_ping_share_and_leader_push(&write, &mut read).await;

        match recv_request(&mut read).await {
            SyncRequest::ShareLeaderInfo => {
                write
                    .send(SyncResponse::LeaderInfo(leader_observation(2, 1, 2, vec![2])))
                    .await
                    .unwrap();
            }
            other => panic!("expected share-leader-info request, got {other:?}"),
        }

        task_handle.await.unwrap();
        let peers = state.peers.lock().await;
        assert_eq!(peers.get(&2).unwrap().leader_observation.as_ref().unwrap().observer, 2);
    }

    #[tokio::test]
    async fn unexpected_response_kills_connection() {
        let state = one_peer_task_state();
        let (task, connections, remote_io) = simulated_task(state, 2).await;
        let task_handle = tokio::spawn(async move {
            task.observe_peers().await;
        });

        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;

        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => write.send(SyncResponse::Pong(id)).await.unwrap(),
            other => panic!("expected ping request, got {other:?}"),
        }
        match recv_request(&mut read).await {
            SyncRequest::SharePeers(_) => write.send(SyncResponse::UnexpectedRequest).await.unwrap(),
            other => panic!("expected share-peers request, got {other:?}"),
        }
        task_handle.await.unwrap();

        let rpc = tokio::spawn({
            let connections = connections.clone();
            async move { connections.send_rpc(2, SyncRequest::Ping(9)).await }
        });
        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        match recv_request(&mut read).await {
            SyncRequest::Ping(id) => {
                assert_eq!(id, 9);
                write.send(SyncResponse::Pong(9)).await.unwrap();
            }
            other => panic!("expected ping request after reconnect, got {other:?}"),
        }
        assert!(matches!(rpc.await.unwrap().unwrap(), SyncResponse::Pong(9)));
    }

    #[tokio::test]
    async fn rpc_error_clears_observation_without_killing_explicitly() {
        let mut peers = HashMap::new();
        let mut peer_two = peer(2, Some(true), true);
        peer_two.leader_observation = Some(leader_observation(2, 1, 2, vec![2]));
        peers.insert(2, peer_two);
        let state = node_state(1, true, peers);
        let (task, _connections, remote_io) = simulated_task(state.clone(), 2).await;
        let task_handle = tokio::spawn(async move {
            task.observe_peers().await;
        });

        let (write, mut read) = accept_server(&remote_io).await;
        expect_handshake(&write, &mut read, 1).await;
        expect_ping_share_and_leader_push(&write, &mut read).await;

        match recv_request(&mut read).await {
            SyncRequest::ShareLeaderInfo => {}
            other => panic!("expected share-leader-info request, got {other:?}"),
        }

        task_handle.await.unwrap();
        let peers = state.peers.lock().await;
        assert!(peers.get(&2).is_some_and(|peer| peer.leader_observation.is_none()));
    }
}
