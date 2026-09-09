use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{
    Command, Peer, Raft,
    machine::{Machine, ReplicatedState, Revision, Session, SnapshotHandle},
    network::Network,
    protocol::{self, Request, Response as WireResponse},
    storage::{Database, LogStore},
};
use crate::transport::traits::{SyncIO, SyncIOListener};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct OperationId {
    pub client_id: uuid::Uuid,
    pub sequence: u64,
}

pub struct Operation<C> {
    pub id: OperationId,
    pub command: C,
}

#[derive(Clone, Debug)]
pub struct CommitReceipt<R> {
    pub operation_id: OperationId,
    pub revision: Revision,
    pub result: R,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitError {
    NotAdmitted { reason: String },
    OutcomeUnknown { operation_id: OperationId },
    SessionRejected { reason: String },
}

#[derive(Clone, Debug)]
pub enum OperationStatus<R> {
    Committed(CommitReceipt<R>),
    PendingOrUnknown,
    Retired,
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: u64,
    pub cluster_id: uuid::Uuid,
    pub storage_path: PathBuf,
    pub max_pending: usize,
    pub max_command_bytes: usize,
    pub max_sessions: usize,
    pub publication_interval: Duration,
    pub heartbeat: Duration,
    pub election_min: Duration,
    pub election_max: Duration,
}

impl NodeConfig {
    pub async fn persistent(storage_path: PathBuf, cluster_id: uuid::Uuid) -> Result<Self, String> {
        let path = storage_path.clone();
        let node_id = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut identity_name = path.as_os_str().to_os_string();
            identity_name.push(".identity");
            let identity_path = PathBuf::from(identity_name);
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&identity_path)
            {
                Ok(mut file) => {
                    let random = uuid::Uuid::new_v4();
                    let node_id = u64::from_be_bytes(random.as_bytes()[..8].try_into().unwrap());
                    let bytes = serde_json::to_vec(&(1u32, cluster_id, node_id)).map_err(|e| e.to_string())?;
                    file.write_all(&bytes).map_err(|e| e.to_string())?;
                    file.sync_all().map_err(|e| e.to_string())?;
                    let parent = identity_path
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or_else(|| std::path::Path::new("."));
                    std::fs::File::open(parent)
                        .and_then(|f| f.sync_all())
                        .map_err(|e| e.to_string())?;
                    Ok(node_id)
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let bytes = std::fs::read(&identity_path).map_err(|e| e.to_string())?;
                    let (format, stored_cluster, node_id): (u32, uuid::Uuid, u64) =
                        serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                    if format != 1 || stored_cluster != cluster_id {
                        return Err("node identity format or cluster mismatch".into());
                    }
                    Ok(node_id)
                }
                Err(error) => Err(error.to_string()),
            }
        })
        .await
        .map_err(|e| e.to_string())??;
        Ok(Self::new(storage_path, cluster_id, node_id))
    }

    pub fn new(storage_path: PathBuf, cluster_id: uuid::Uuid, node_id: u64) -> Self {
        Self {
            node_id,
            cluster_id,
            storage_path,
            max_pending: 128,
            max_command_bytes: 64 * 1024,
            max_sessions: 10_000,
            publication_interval: Duration::from_millis(100),
            heartbeat: Duration::from_millis(500),
            election_min: Duration::from_secs(3),
            election_max: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeStatus {
    pub node_id: u64,
    pub ready: bool,
    pub leader: Option<u64>,
    pub revision: Option<Revision>,
    pub voters: BTreeSet<u64>,
    pub failure: Option<String>,
}

#[derive(Clone)]
struct StatusContext<S: ReplicatedState> {
    local: u64,
    cancel: CancellationToken,
    metrics: watch::Receiver<openraft::RaftMetrics<u64, Peer>>,
    snapshot: watch::Receiver<SnapshotHandle<S>>,
    listener: tokio::task::AbortHandle,
    publisher: tokio::task::AbortHandle,
    critical: watch::Receiver<Option<String>>,
}

impl<S: ReplicatedState> StatusContext<S> {
    fn current(&self) -> NodeStatus {
        let metrics = self.metrics.borrow().clone();
        let revision = self.snapshot.borrow().revision;
        let failure = self
            .critical
            .borrow()
            .clone()
            .or_else(|| metrics.running_state.err().map(|e| e.to_string()))
            .or_else(|| {
                (self.listener.is_finished() && !self.cancel.is_cancelled()).then(|| "RPC listener stopped".into())
            })
            .or_else(|| {
                self.publisher
                    .is_finished()
                    .then(|| "publication worker stopped".into())
            });
        NodeStatus {
            node_id: self.local,
            ready: failure.is_none()
                && !self.cancel.is_cancelled()
                && metrics.last_applied.is_some()
                && revision.is_some(),
            leader: metrics.current_leader,
            revision,
            voters: metrics.membership_config.membership().voter_ids().collect(),
            failure,
        }
    }
}

pub struct Node<S: ReplicatedState, I: SyncIO<Address = u64>> {
    raft: Option<Raft>,
    database: Option<Database>,
    network: Network<I>,
    config: NodeConfig,
    snapshot: watch::Receiver<SnapshotHandle<S>>,
    sessions: watch::Receiver<BTreeMap<uuid::Uuid, Session>>,
    admission: Arc<Semaphore>,
    membership: tokio::sync::Mutex<()>,
    cancel: CancellationToken,
    listener: Option<JoinHandle<Result<(), String>>>,
    publisher: Option<JoinHandle<Result<(), String>>>,
    critical_failure: watch::Sender<Option<String>>,
    status_context: StatusContext<S>,
    status_watch: watch::Receiver<NodeStatus>,
    status_task: Option<JoinHandle<()>>,
}

impl<S: ReplicatedState, I: SyncIOListener<Address = u64>> Node<S, I> {
    pub async fn open(config: NodeConfig, io: Arc<I>, state: S) -> Result<Self, String> {
        if config.max_pending == 0
            || config.max_pending > 128
            || config.publication_interval.is_zero()
            || config.max_command_bytes == 0
            || config.max_command_bytes > 64 * 1024
            || config.max_sessions == 0
        {
            return Err("invalid admission limits".into());
        }
        let engine = openraft::Config {
            heartbeat_interval: config
                .heartbeat
                .as_millis()
                .try_into()
                .map_err(|_| "heartbeat overflow")?,
            election_timeout_min: config
                .election_min
                .as_millis()
                .try_into()
                .map_err(|_| "election timeout overflow")?,
            election_timeout_max: config
                .election_max
                .as_millis()
                .try_into()
                .map_err(|_| "election timeout overflow")?,
            max_payload_entries: 2,
            snapshot_max_chunk_size: 64 * 1024,
            install_snapshot_timeout: 5_000,
            ..Default::default()
        }
        .validate()
        .map_err(|e| e.to_string())?;
        let database = Database::open(&config.storage_path, config.cluster_id, config.node_id)
            .await
            .map_err(|e| e.to_string())?;
        let machine = Machine::open(state, database.clone(), config.cluster_id, config.max_sessions)
            .await
            .map_err(|e| e.to_string())?;
        let snapshot = machine.publisher.subscribe();
        let sessions = machine.sessions.subscribe();
        let cancel = CancellationToken::new();
        let network = Network {
            io: io.clone(),
            cluster: config.cluster_id,
            local: config.node_id,
            schema: S::SCHEMA_VERSION,
            session_limit: config.max_sessions as u64,
            cancel: cancel.clone(),
        };
        let publication = machine.publication(cancel.clone(), config.publication_interval);
        let raft = Raft::new(config.node_id, Arc::new(engine), network.clone(), LogStore(database.clone()), machine)
            .await
            .map_err(|e| e.to_string())?;
        let publisher = tokio::spawn(publication);
        let listener_raft = raft.clone();
        let listener_cancel = cancel.clone();
        let cluster = config.cluster_id;
        let session_limit = config.max_sessions as u64;
        let local = config.node_id;
        let listener_database = database.clone();
        let admission = Arc::new(Semaphore::new(config.max_pending));
        let ingress_admission = admission.clone();
        let command_limit = config.max_command_bytes;
        let listener = tokio::spawn(async move {
            let mut children = JoinSet::new();
            let writes = ingress_admission;
            let snapshots = Arc::new(Semaphore::new(16));
            let control = Arc::new(Semaphore::new(64));
            let installs = Arc::new(Semaphore::new(1));
            loop {
                tokio::select! {
                    _ = listener_cancel.cancelled() => break,
                    child = children.join_next(), if !children.is_empty() => {
                        if let Some(Err(error)) = child {
                            children.abort_all();
                            while children.join_next().await.is_some() {}
                            return Err(error.to_string());
                        }
                    }
                    incoming = io.next_client(), if children.len() < 256 => {
                        let mut connection = match incoming {
                            Ok(connection) => connection,
                            Err(error) => { children.abort_all(); while children.join_next().await.is_some() {} return Err(error.to_string()); }
                        };
                        let raft = listener_raft.clone();
                        let database = listener_database.clone();
                        let cancel = listener_cancel.clone();
                        let writes = writes.clone();
                        let snapshots = snapshots.clone();
                        let control = control.clone();
                        let installs = installs.clone();
                        children.spawn(async move {
                            let serve = async {
                                let source = protocol::verify(&mut connection.read, cluster, local, S::SCHEMA_VERSION, session_limit).await?;
                                protocol::hello(&mut connection.write, cluster, local, source, S::SCHEMA_VERSION, session_limit).await?;
                                use tokio::io::AsyncReadExt;
                                let lane = connection.read.read_u8().await?;
                                let _permit = match lane {
                                    0 => control.try_acquire(),
                                    1 => writes.try_acquire(),
                                    2 => snapshots.try_acquire(),
                                    _ => return Err(std::io::Error::other("invalid RPC lane")),
                                }.map_err(|_| std::io::Error::other("RPC lane full"))?;
                                let request: Request = protocol::read(&mut connection.read).await?;
                                if request.lane() != lane { return Err(std::io::Error::other("RPC lane mismatch")); }
                                let response = match request {
                                    Request::Append(rpc) => WireResponse::Append(raft.append_entries(rpc).await),
                                    Request::Vote(rpc) => WireResponse::Vote(raft.vote(rpc).await),
                                    Request::Snapshot(rpc) => {
                                        if rpc.offset.saturating_add(rpc.data.len() as u64) > 256 * 1024 * 1024 || rpc.data.len() > 64 * 1024 {
                                            WireResponse::Rejected("snapshot limit exceeded".into())
                                        } else { WireResponse::Snapshot(raft.install_snapshot(rpc).await) }
                                    }
                                    Request::SnapshotBegin { meta, bytes } => {
                                        let existing = database.snapshot_manifest().await.map_err(std::io::Error::other)?;
                                        let offset = if existing.as_ref().is_some_and(|(current, size)| *current == meta && *size == bytes) {
                                            bytes
                                        } else {
                                            database.resume_snapshot(meta, bytes).await.map_err(std::io::Error::other)?
                                        };
                                        WireResponse::SnapshotOffset(offset)
                                    }
                                    Request::SnapshotPart { snapshot_id, offset, data, digest } => {
                                        use sha2::{Digest, Sha256};
                                        if Sha256::digest(&data).as_slice() != digest.as_slice() {
                                            WireResponse::Rejected("snapshot chunk digest mismatch".into())
                                        } else {
                                            WireResponse::SnapshotOffset(database.receive_chunk(snapshot_id, offset, data).await.map_err(std::io::Error::other)?)
                                        }
                                    }
                                    Request::SnapshotEnd { vote, meta } => {
                                        use sha2::{Digest, Sha256};
                                        let _install = installs.try_acquire().map_err(|_| std::io::Error::other("snapshot install busy"))?;
                                        let current = database.snapshot_manifest().await.map_err(std::io::Error::other)?;
                                        let bytes = if current.as_ref().is_some_and(|(current, _)| *current == meta) {
                                            database.snapshot().await.map_err(std::io::Error::other)?.ok_or_else(|| std::io::Error::other("snapshot missing"))?.1
                                        } else {
                                            database.assembled_snapshot(meta.snapshot_id.clone()).await.map_err(std::io::Error::other)?
                                        };
                                        if format!("{:x}", Sha256::digest(&bytes)) != meta.snapshot_id {
                                            WireResponse::Rejected("snapshot digest mismatch".into())
                                        } else {
                                            WireResponse::SnapshotInstalled(raft.install_full_snapshot(vote, openraft::Snapshot { meta, snapshot: Box::new(std::io::Cursor::new(bytes)) }).await)
                                        }
                                    }
                                    Request::Feed { after } => {
                                        use openraft::{RaftLogReader, storage::RaftLogStorage};
                                        let mut log = LogStore(database.clone());
                                        let metrics = raft.metrics().borrow().clone();
                                        let state = log.get_log_state().await.map_err(std::io::Error::other)?;
                                        let compatible = match after {
                                            None => false,
                                            Some(after) if metrics.last_applied.is_some_and(|x| after.index <= x.index) => {
                                                let entry = log.try_get_log_entries(after.index..=after.index).await.map_err(std::io::Error::other)?;
                                                entry.first().is_some_and(|e| e.log_id == after) || state.last_purged_log_id == Some(after)
                                            }
                                            _ => false,
                                        };
                                        if !compatible { WireResponse::SnapshotRequired }
                                        else {
                                            let start = after.unwrap().index.checked_add(1).ok_or_else(|| std::io::Error::other("revision overflow"))?;
                                            let checkpoint = database.snapshot_manifest().await.map_err(std::io::Error::other)?.map(|(meta, _)| meta);
                                            let mut end = metrics.last_applied.unwrap().index.saturating_add(1).min(start.saturating_add(2));
                                            if let Some(base) = checkpoint.as_ref().and_then(|meta| meta.last_log_id)
                                                && base.index >= start { end = end.min(base.index.saturating_add(1)); }
                                            let entries = log.try_get_log_entries(start..end).await.map_err(std::io::Error::other)?;
                                            if entries.len() as u64 != end.saturating_sub(start) { WireResponse::SnapshotRequired }
                                            else { WireResponse::Feed { entries, applied: metrics.last_applied, leader: metrics.current_leader, checkpoint } }
                                        }
                                    }
                                    Request::Checkpoint => match database.snapshot_manifest().await.map_err(std::io::Error::other)? {
                                        Some((meta, bytes)) => WireResponse::Manifest { meta, bytes },
                                        None => WireResponse::Rejected("checkpoint unavailable".into()),
                                    },
                                    Request::Manifest { preferred } => {
                                        use openraft::storage::RaftLogStorage;
                                        let purged = LogStore(database.clone()).get_log_state().await.map_err(std::io::Error::other)?.last_purged_log_id;
                                        match database.lease_snapshot(preferred.clone()).await.map_err(std::io::Error::other)? {
                                            Some((meta, bytes)) if meta.last_log_id.is_some() && (preferred.is_some() || meta.last_log_id >= purged) => WireResponse::Manifest { meta, bytes },
                                            _ => { let _ = raft.trigger().snapshot().await; WireResponse::Rejected("snapshot pending".into()) }
                                        }
                                    }
                                    Request::Chunk { snapshot_id, offset } => {
                                        use sha2::{Digest, Sha256};
                                        match database.snapshot_chunk(snapshot_id.clone(), offset, 256 * 1024).await {
                                            Ok(data) => {
                                                let digest = Sha256::digest(&data).to_vec();
                                                WireResponse::Chunk { snapshot_id, offset, data, digest }
                                            }
                                            Err(_) => WireResponse::Rejected("snapshot expired".into()),
                                        }
                                    }
                                    Request::Write(command) => {
                                        if command.payload.len() > command_limit || (!command.retire && serde_json::from_slice::<S::Command>(&command.payload).is_err()) {
                                            WireResponse::Rejected("invalid command".into())
                                        } else { WireResponse::Write(raft.client_write(command).await) }
                                    }
                                };
                                protocol::write(&mut connection.write, &response).await
                            };
                            tokio::select! {
                                _ = cancel.cancelled() => {},
                                result = tokio::time::timeout(Duration::from_secs(5), serve) => {
                                    if let Ok(Err(error)) = result { tracing::debug!(%error, "v4 RPC failed"); }
                                }
                            }
                        });
                    }
                }
            }
            children.abort_all();
            while children.join_next().await.is_some() {}
            Ok(())
        });
        let (critical_failure, critical) = watch::channel(None);
        let status_context = StatusContext {
            local: config.node_id,
            cancel: cancel.clone(),
            metrics: raft.metrics(),
            snapshot: snapshot.clone(),
            listener: listener.abort_handle(),
            publisher: publisher.abort_handle(),
            critical,
        };
        let (status_tx, status_watch) = watch::channel(status_context.current());
        let mut monitor = status_context.clone();
        let status_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = monitor.cancel.cancelled() => break,
                    result = monitor.metrics.changed() => if result.is_err() { break; },
                    _ = interval.tick() => {},
                }
                let next = monitor.current();
                let previous = status_tx.borrow().leader;
                if previous != next.leader {
                    tracing::info!(node = monitor.local, previous = ?previous, leader = ?next.leader, "leadership changed");
                }
                status_tx.send_replace(next);
            }
            let mut final_status = monitor.current();
            final_status.ready = false;
            status_tx.send_replace(final_status);
        });
        Ok(Self {
            raft: Some(raft),
            database: Some(database),
            network,
            admission,
            membership: tokio::sync::Mutex::new(()),
            config,
            snapshot,
            sessions,
            cancel,
            listener: Some(listener),
            publisher: Some(publisher),
            critical_failure,
            status_context,
            status_watch,
            status_task: Some(status_task),
        })
    }
}

impl<S: ReplicatedState, I: SyncIO<Address = u64>> Node<S, I> {
    fn raft(&self) -> &Raft {
        self.raft.as_ref().expect("node is running")
    }

    pub async fn bootstrap(&self, voters: BTreeMap<u64, u64>) -> Result<(), String> {
        let _membership = self.membership.lock().await;
        if voters.len() != 3 && voters.len() != 5 {
            return Err("bootstrap requires three or five voters".into());
        }
        if voters.values().copied().collect::<BTreeSet<_>>().len() != voters.len() {
            return Err("voter addresses must be distinct".into());
        }
        if !voters.contains_key(&self.config.node_id) {
            return Err("bootstrap node must be an initial voter".into());
        }
        self.raft()
            .initialize(
                voters
                    .into_iter()
                    .map(|(id, address)| (id, Peer { address }))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .map_err(|e| e.to_string())
    }

    pub fn read_snapshot(&self) -> SnapshotHandle<S> {
        self.snapshot.borrow().clone()
    }

    pub fn status(&self) -> NodeStatus {
        let mut status = self.status_context.current();
        if self.status_task.as_ref().is_some_and(|task| task.is_finished()) && !self.cancel.is_cancelled() {
            status.ready = false;
            status.failure = Some("status worker stopped".into());
        }
        status
    }

    pub fn watch_status(&self) -> watch::Receiver<NodeStatus> {
        self.status_watch.clone()
    }

    pub async fn submit(
        &self,
        operation: Operation<S::Command>,
        deadline: Instant,
    ) -> Result<CommitReceipt<S::Result>, SubmitError> {
        let id = operation.id;
        let reject = |reason: &str| SubmitError::NotAdmitted { reason: reason.into() };
        if self.cancel.is_cancelled() || self.status().failure.is_some() {
            return Err(reject("node unavailable"));
        }
        if deadline <= Instant::now() {
            return Err(reject("deadline exceeded"));
        }
        let _permit = self.admission.try_acquire().map_err(|_| reject("admission full"))?;
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let bytes = protocol::canonical(&operation.command, self.config.max_command_bytes)?;
            let _: S::Command = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
            Ok::<_, std::io::Error>(bytes)
        }))
        .map_err(|_| reject("command codec panicked"))?
        .map_err(|_| reject("command encoding failed or size limit exceeded"))?;
        if payload.len() > self.config.max_command_bytes {
            return Err(reject("command limit exceeded"));
        }
        let response = self
            .send_command(
                Command {
                    operation: id,
                    payload,
                    retire: false,
                },
                deadline,
            )
            .await?;
        let result =
            serde_json::from_slice(&response.result).map_err(|_| SubmitError::OutcomeUnknown { operation_id: id })?;
        Ok(CommitReceipt {
            operation_id: id,
            revision: Revision::new(response.revision.expect("applied command has revision"), self.config.cluster_id),
            result,
        })
    }

    pub async fn retire_session(&self, id: OperationId, deadline: Instant) -> Result<CommitReceipt<()>, SubmitError> {
        let _permit = self.admission.try_acquire().map_err(|_| SubmitError::NotAdmitted {
            reason: "admission full".into(),
        })?;
        let response = self
            .send_command(
                Command {
                    operation: id,
                    payload: Vec::new(),
                    retire: true,
                },
                deadline,
            )
            .await?;
        Ok(CommitReceipt {
            operation_id: id,
            revision: Revision::new(response.revision.expect("retirement has revision"), self.config.cluster_id),
            result: (),
        })
    }

    async fn send_command(&self, command: Command, deadline: Instant) -> Result<super::Response, SubmitError> {
        let id = command.operation;
        let reject = |reason: &str| SubmitError::NotAdmitted { reason: reason.into() };
        if deadline <= Instant::now() || self.cancel.is_cancelled() || self.status().failure.is_some() {
            return Err(reject("node unavailable or deadline exceeded"));
        }
        let metrics = self.raft().metrics().borrow().clone();
        let leader = metrics.current_leader.ok_or_else(|| reject("no known leader"))?;
        let peer = metrics
            .membership_config
            .membership()
            .get_node(&leader)
            .cloned()
            .ok_or_else(|| reject("leader has no route"))?;
        let result = tokio::select! {
            _ = self.cancel.cancelled() => return Err(SubmitError::OutcomeUnknown { operation_id: id }),
            result = timeout_at(deadline, async {
                if leader == self.config.node_id { self.raft().client_write(command).await.map_err(|_| ()) }
                else {
                    match self.network.request(leader, &peer, Request::Write(command), deadline).await {
                        Ok(WireResponse::Write(Ok(response))) => Ok(response),
                        _ => Err(()),
                    }
                }
            }) => result,
        };
        let response = result
            .map_err(|_| SubmitError::OutcomeUnknown { operation_id: id })?
            .map_err(|_| SubmitError::OutcomeUnknown { operation_id: id })?
            .data;
        if let Some(reason) = &response.error {
            return Err(SubmitError::SessionRejected { reason: reason.clone() });
        }
        if self.status().failure.is_some() || self.cancel.is_cancelled() {
            return Err(SubmitError::OutcomeUnknown { operation_id: id });
        }
        tracing::debug!(client = %id.client_id, sequence = id.sequence, revision = ?response.revision, "operation committed");
        Ok(response)
    }

    pub fn operation_status(&self, id: OperationId) -> OperationStatus<S::Result> {
        let sessions = self.sessions.borrow();
        let Some(session) = sessions.get(&id.client_id) else {
            return OperationStatus::PendingOrUnknown;
        };
        if session.retired || id.sequence < session.sequence {
            return OperationStatus::Retired;
        }
        if session.sequence != id.sequence {
            return OperationStatus::PendingOrUnknown;
        }
        match serde_json::from_slice(&session.response.result) {
            Ok(result) => OperationStatus::Committed(CommitReceipt {
                operation_id: id,
                revision: Revision::new(
                    session.response.revision.expect("session has revision"),
                    self.config.cluster_id,
                ),
                result,
            }),
            Err(_) => OperationStatus::PendingOrUnknown,
        }
    }

    pub async fn wait_for_revision(&self, revision: Revision, deadline: Instant) -> Result<(), String> {
        if revision.cluster != self.config.cluster_id {
            return Err("revision belongs to another cluster".into());
        }
        let mut snapshot = self.snapshot.clone();
        tokio::select! {
            _ = self.cancel.cancelled() => Err("node shutting down".into()),
            result = timeout_at(deadline, async {
                loop {
                    if snapshot.borrow().revision.is_some_and(|r| r.index >= revision.index) { return Ok(()); }
                    snapshot.changed().await.map_err(|_| "state owner stopped".to_owned())?;
                }
            }) => result.map_err(|_| "revision deadline exceeded".to_owned())?,
        }
    }

    pub async fn checkpoint(&self, deadline: Instant) -> Result<(), String> {
        let target = self
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .ok_or("no committed state")?;
        self.raft().trigger().snapshot().await.map_err(|e| e.to_string())?;
        let mut metrics = self.raft().metrics();
        timeout_at(deadline, async {
            loop {
                if metrics.borrow().snapshot.is_some_and(|log| log.index >= target.index) {
                    return Ok(());
                }
                metrics.changed().await.map_err(|_| "consensus stopped".to_owned())?;
            }
        })
        .await
        .map_err(|_| "snapshot deadline exceeded".to_owned())?
    }

    /// Compare retained voter checkpoints after all voters reach the same quiesced barrier.
    /// A content mismatch stops this node; differing checkpoint revisions require another comparison.
    pub async fn verify_voter_checkpoints(&self, deadline: Instant) -> Result<(), String> {
        let database = self.database.as_ref().ok_or("node stopped")?;
        let (local, _) = database
            .snapshot_manifest()
            .await
            .map_err(|e| e.to_string())?
            .ok_or("no local checkpoint")?;
        let metrics = self.raft().metrics().borrow().clone();
        for id in metrics.membership_config.membership().voter_ids() {
            if id == self.config.node_id {
                continue;
            }
            let peer = metrics
                .membership_config
                .membership()
                .get_node(&id)
                .ok_or("voter route missing")?;
            let response = self
                .network
                .request(id, peer, Request::Checkpoint, deadline)
                .await
                .map_err(|e| e.to_string())?;
            let WireResponse::Manifest { meta, .. } = response else {
                return Err("voter checkpoint unavailable".into());
            };
            if meta.last_log_id != local.last_log_id {
                return Err("voter checkpoint revisions differ".into());
            }
            if meta.snapshot_id != local.snapshot_id {
                let error = "voting state machines disagree at the same committed revision".to_owned();
                self.critical_failure.send_replace(Some(error.clone()));
                self.cancel.cancel();
                tracing::error!(peer = id, revision = ?local.last_log_id, "voter checkpoint mismatch");
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn replace_voter(&self, old: u64, new: u64, address: u64, deadline: Instant) -> Result<(), String> {
        timeout_at(deadline, async {
            let _membership = self.membership.lock().await;
            let mut voters = self.status().voters;
            if !voters.remove(&old) || voters.contains(&new) || new == old {
                return Err("invalid replacement voter identities".into());
            }
            if self.status().leader != Some(self.config.node_id) {
                return Err("membership changes require the leader".into());
            }
            let metrics = self.raft().metrics().borrow().clone();
            if metrics
                .membership_config
                .membership()
                .nodes()
                .any(|(id, peer)| *id != new && peer.address == address)
            {
                return Err("replacement address already belongs to a member".into());
            }
            // Initial application state is outside the log. Force learners to install its complete checkpoint.
            self.checkpoint(deadline).await?;
            let base = self.raft().metrics().borrow().snapshot.ok_or("checkpoint missing")?;
            self.raft()
                .trigger()
                .purge_log(base.index)
                .await
                .map_err(|e| e.to_string())?;
            let mut metrics = self.raft().metrics();
            loop {
                if metrics.borrow().purged.is_some_and(|log| log.index >= base.index) {
                    break;
                }
                metrics.changed().await.map_err(|_| "consensus stopped".to_owned())?;
            }
            self.raft()
                .add_learner(new, Peer { address }, true)
                .await
                .map_err(|e| e.to_string())?;
            voters.insert(new);
            self.raft()
                .change_membership(voters, false)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
        .map_err(|_| "membership outcome unknown".to_owned())?
    }

    pub async fn shutdown(mut self, deadline: Instant) -> Result<(), String> {
        self.cancel.cancel();
        self.admission.close();
        timeout_at(deadline, async {
            let mut failures = Vec::new();
            if let Some(listener) = self.listener.take() {
                match listener.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(error),
                    Err(error) => failures.push(error.to_string()),
                }
            }
            if let Some(publisher) = self.publisher.take() {
                match publisher.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(error),
                    Err(error) => failures.push(error.to_string()),
                }
            }
            if let Some(status_task) = self.status_task.take()
                && let Err(error) = status_task.await
            {
                failures.push(error.to_string());
            }
            if let Some(raft) = self.raft.take()
                && let Err(error) = raft.shutdown().await
            {
                failures.push(error.to_string());
            }
            if let Some(database) = self.database.take()
                && let Err(error) = database.shutdown().await
            {
                failures.push(error.to_string());
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            }
        })
        .await
        .map_err(|_| "shutdown deadline exceeded".to_owned())?
    }
}

impl<S: ReplicatedState, I: SyncIO<Address = u64>> Drop for Node<S, I> {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.admission.close();
    }
}
