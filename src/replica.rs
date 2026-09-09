use super::{
    NodeConfig, Peer, ReplicatedState, Revision, SnapshotHandle,
    machine::Machine,
    network::Network,
    protocol::{Request, Response},
    storage::Database,
};
use crate::transport::traits::SyncIO;
use openraft::storage::RaftStateMachine;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Cursor, sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct ReplicaStatus {
    pub ready: bool,
    pub source: Option<u64>,
    pub session: uuid::Uuid,
    pub phase: &'static str,
    pub error: Option<String>,
}

struct ReplicaExit(watch::Sender<ReplicaStatus>);
impl Drop for ReplicaExit {
    fn drop(&mut self) {
        let mut status = self.0.borrow().clone();
        status.ready = false;
        if status.error.is_none() {
            status.phase = "stopped";
        }
        self.0.send_replace(status);
    }
}

pub struct ReadReplica<S: ReplicatedState> {
    snapshot: watch::Receiver<SnapshotHandle<S>>,
    status: watch::Receiver<ReplicaStatus>,
    cancel: CancellationToken,
    task: Option<JoinHandle<Result<(), String>>>,
    publisher: Option<JoinHandle<Result<(), String>>>,
    database: Option<Database>,
    verification_failures: Arc<std::sync::atomic::AtomicU64>,
}

impl<S: ReplicatedState> ReadReplica<S> {
    pub async fn open<I: SyncIO<Address = u64>>(
        config: NodeConfig,
        io: Arc<I>,
        initial: S,
        sources: BTreeMap<u64, u64>,
    ) -> Result<Self, String> {
        if config.publication_interval.is_zero() || config.max_sessions == 0 {
            return Err("invalid replica configuration".into());
        }
        if sources.is_empty() || sources.len() > 64 || sources.contains_key(&config.node_id) {
            return Err("invalid replica source directory".into());
        }
        let database = Database::open(&config.storage_path, config.cluster_id, config.node_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut machine = Machine::open(initial, database.clone(), config.cluster_id, config.max_sessions)
            .await
            .map_err(|e| e.to_string())?;
        let snapshot = machine.publisher.subscribe();
        let cancel = CancellationToken::new();
        let network = Network {
            io,
            cluster: config.cluster_id,
            local: config.node_id,
            schema: S::SCHEMA_VERSION,
            session_limit: config.max_sessions as u64,
            cancel: cancel.clone(),
        };
        let (status_tx, status) = watch::channel(ReplicaStatus {
            ready: false,
            source: None,
            session: uuid::Uuid::new_v4(),
            phase: "idle",
            error: None,
        });
        let publication = machine.publication(cancel.clone(), config.publication_interval);
        let publisher = tokio::spawn(publication);
        let task_cancel = cancel.clone();
        let verification_failures = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let task_failures = verification_failures.clone();
        let task = tokio::spawn(async move {
            let _exit = ReplicaExit(status_tx.clone());
            let mut sources: Vec<_> = sources.into_iter().collect();
            let mut next = network.local as usize;
            loop {
                if task_cancel.is_cancelled() {
                    break;
                }
                let committed_sources = machine.sources();
                if !committed_sources.is_empty() {
                    sources = committed_sources.into_iter().collect();
                }
                let (source, address) = sources[next % sources.len()];
                next = next.wrapping_add(1);
                let session = uuid::Uuid::new_v4();
                status_tx.send_replace(ReplicaStatus {
                    ready: false,
                    source: Some(source),
                    session,
                    phase: "connecting",
                    error: None,
                });
                let work =
                    follow(&mut machine, &network, source, Peer { address }, &status_tx, session, &task_failures);
                let result = tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    result = work => result,
                };
                if let Err(error) = result {
                    status_tx.send_replace(ReplicaStatus {
                        ready: false,
                        source: Some(source),
                        session,
                        phase: "backoff",
                        error: Some(match error {
                            FollowError::Retry(error) => error,
                            FollowError::Fatal(error) => {
                                status_tx.send_replace(ReplicaStatus {
                                    ready: false,
                                    source: Some(source),
                                    session,
                                    phase: "unhealthy",
                                    error: Some(error.clone()),
                                });
                                return Err(error);
                            }
                        }),
                    });
                }
                tokio::select! { _ = task_cancel.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
            }
            Ok(())
        });
        Ok(Self {
            snapshot,
            status,
            cancel,
            task: Some(task),
            publisher: Some(publisher),
            database: Some(database),
            verification_failures,
        })
    }
    pub fn verification_failures(&self) -> u64 {
        self.verification_failures.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn read_snapshot(&self) -> SnapshotHandle<S> {
        self.snapshot.borrow().clone()
    }
    pub fn watch_status(&self) -> watch::Receiver<ReplicaStatus> {
        self.status.clone()
    }

    pub fn status(&self) -> ReplicaStatus {
        let mut status = self.status.borrow().clone();
        if self.cancel.is_cancelled()
            || (self.task.as_ref().is_some_and(|task| task.is_finished())
                || self.publisher.as_ref().is_some_and(|task| task.is_finished()))
        {
            status.ready = false;
            status.phase = "stopped";
        }
        status
    }
    pub async fn wait_for_revision(&self, revision: Revision, deadline: Instant) -> Result<(), String> {
        let mut snapshot = self.snapshot.clone();
        timeout_at(deadline, async {
            loop {
                if self.status().phase == "stopped" {
                    return Err("replica stopped".into());
                }
                if let Some(local) = snapshot.borrow().revision {
                    if local.cluster != revision.cluster {
                        return Err("revision belongs to another cluster".into());
                    }
                    if local.index >= revision.index {
                        return Ok(());
                    }
                }
                tokio::select! {
                    _ = self.cancel.cancelled() => return Err("replica stopped".into()),
                    changed = snapshot.changed() => changed.map_err(|_| "replica state owner stopped")?,
                }
            }
        })
        .await
        .map_err(|_| "replica deadline exceeded".to_owned())?
    }
    pub async fn shutdown(mut self, deadline: Instant) -> Result<(), String> {
        self.cancel.cancel();
        timeout_at(deadline, async {
            let mut failures = Vec::new();
            if let Some(task) = self.task.take() {
                match task.await {
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
        .map_err(|_| "replica shutdown deadline exceeded".to_owned())?
    }
}
impl<S: ReplicatedState> Drop for ReadReplica<S> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

enum FollowError {
    Retry(String),
    Fatal(String),
}
impl From<String> for FollowError {
    fn from(value: String) -> Self {
        Self::Retry(value)
    }
}
impl From<&str> for FollowError {
    fn from(value: &str) -> Self {
        Self::Retry(value.into())
    }
}

async fn follow<S: ReplicatedState, I: SyncIO<Address = u64>>(
    machine: &mut Machine<S>,
    network: &Network<I>,
    source: u64,
    peer: Peer,
    status: &watch::Sender<ReplicaStatus>,
    session: uuid::Uuid,
    verification_failures: &std::sync::atomic::AtomicU64,
) -> Result<(), FollowError> {
    let started = Instant::now();
    let mut needs_snapshot = false;
    let mut verified = machine
        .database
        .snapshot_manifest()
        .await
        .map_err(|e| FollowError::Fatal(e.to_string()))?
        .map(|(meta, _)| meta.snapshot_id);
    loop {
        let response = if needs_snapshot {
            Response::SnapshotRequired
        } else {
            network
                .request(
                    source,
                    &peer,
                    Request::Feed {
                        after: machine.applied(),
                    },
                    Instant::now() + Duration::from_secs(2),
                )
                .await
                .map_err(|e| e.to_string())?
        };
        // I06: one session owns all I/O and application; no detached completion can mutate this owner.
        if status.borrow().session != session {
            return Err("obsolete session".into());
        }
        match response {
            Response::Feed {
                entries,
                applied,
                leader: _,
                checkpoint,
            } => {
                let mut expected = machine
                    .applied()
                    .and_then(|r| r.index.checked_add(1))
                    .ok_or("missing replica base")?;
                for entry in &entries {
                    if entry.log_id.index != expected
                        || applied.is_none_or(|barrier| entry.log_id.index > barrier.index)
                    {
                        return Err("incompatible committed suffix".into());
                    }
                    expected = expected.checked_add(1).ok_or("revision overflow")?;
                }
                let empty = entries.is_empty();
                if !empty {
                    machine
                        .apply(entries)
                        .await
                        .map_err(|e| FollowError::Fatal(e.to_string()))?;
                }
                if let Some(meta) = checkpoint
                    && meta.last_log_id == machine.applied()
                    && verified.as_ref() != Some(&meta.snapshot_id)
                {
                    if !machine
                        .verify_checkpoint(&meta)
                        .await
                        .map_err(|e| FollowError::Fatal(e.to_string()))?
                    {
                        verification_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        needs_snapshot = true;
                        status.send_replace(ReplicaStatus {
                            ready: false,
                            source: Some(source),
                            session,
                            phase: "divergent",
                            error: Some("checkpoint digest mismatch".into()),
                        });
                        continue;
                    }
                    verified = Some(meta.snapshot_id);
                }
                let caught_up = machine.applied() == applied;
                status.send_replace(ReplicaStatus {
                    ready: caught_up,
                    source: Some(source),
                    session,
                    phase: if caught_up { "streaming" } else { "replaying" },
                    error: None,
                });
                if empty {
                    if started.elapsed() >= Duration::from_secs(5) {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Response::SnapshotRequired => {
                status.send_replace(ReplicaStatus {
                    ready: false,
                    source: Some(source),
                    session,
                    phase: "snapshot",
                    error: None,
                });
                let response = network
                    .request(
                        source,
                        &peer,
                        Request::Manifest {
                            preferred: machine
                                .database
                                .pending_snapshot_id()
                                .await
                                .map_err(|e| FollowError::Fatal(e.to_string()))?,
                        },
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                let Response::Manifest { meta, bytes } = response else {
                    return Err("snapshot not available".into());
                };
                if meta.last_log_id < machine.applied() {
                    return Err("source snapshot regresses committed state".into());
                }
                let mut offset = machine
                    .database
                    .resume_snapshot(meta.clone(), bytes)
                    .await
                    .map_err(|e| e.to_string())?;
                let recovery_deadline = Instant::now() + Duration::from_secs(60);
                while offset < bytes {
                    if Instant::now() >= recovery_deadline {
                        return Err("snapshot recovery deadline exceeded".into());
                    }
                    let response = network
                        .request(
                            source,
                            &peer,
                            Request::Chunk {
                                snapshot_id: meta.snapshot_id.clone(),
                                offset,
                            },
                            (Instant::now() + Duration::from_secs(5)).min(recovery_deadline),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    let Response::Chunk {
                        snapshot_id,
                        offset: received_offset,
                        data,
                        digest,
                    } = response
                    else {
                        return Err("snapshot transfer interrupted".into());
                    };
                    if snapshot_id != meta.snapshot_id
                        || received_offset != offset
                        || Sha256::digest(&data).as_slice() != digest
                    {
                        return Err("invalid snapshot chunk".into());
                    }
                    offset = machine
                        .database
                        .receive_chunk(snapshot_id, offset, data)
                        .await
                        .map_err(|e| FollowError::Fatal(e.to_string()))?;
                }
                let data = machine
                    .database
                    .assembled_snapshot(meta.snapshot_id.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                if format!("{:x}", Sha256::digest(&data)) != meta.snapshot_id {
                    return Err("snapshot digest mismatch".into());
                }
                machine
                    .install_snapshot(&meta, Box::new(Cursor::new(data)))
                    .await
                    .map_err(|e| FollowError::Fatal(e.to_string()))?;
                needs_snapshot = false;
                verified = Some(meta.snapshot_id.clone());
            }
            _ => return Err("unexpected feed response".into()),
        }
    }
}
