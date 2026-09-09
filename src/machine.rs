use std::{collections::BTreeMap, fmt::Debug, io::Cursor, sync::Arc};

use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StoredMembership, storage::RaftStateMachine,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use super::{Entry, Response, StorageResult, TypeConfig, storage::Database, storage_error};

pub trait ReplicatedState: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Command: Serialize + DeserializeOwned + Send + Sync;
    type Result: Serialize + DeserializeOwned + Send + Sync;
    const SCHEMA_VERSION: u32;
    /// Apply deterministically. Capture randomness and timestamps in the command before submission.
    fn apply(&mut self, command: Self::Command) -> Self::Result;
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Revision {
    pub cluster: uuid::Uuid,
    pub term: u64,
    pub node: u64,
    pub index: u64,
}

impl Revision {
    pub(crate) fn new(value: LogId<u64>, cluster: uuid::Uuid) -> Self {
        Self {
            cluster,
            term: value.leader_id.term,
            node: value.leader_id.node_id,
            index: value.index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotHandle<S> {
    pub revision: Option<Revision>,
    pub state: Arc<S>,
    /// Local snapshots do not establish linearizable freshness.
    pub may_be_stale: bool,
    pub published_at: tokio::time::Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Session {
    pub sequence: u64,
    pub digest: Vec<u8>,
    pub response: Response,
    pub retired: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, super::Peer>,
    pub sessions: BTreeMap<uuid::Uuid, Session>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Image {
    cluster: uuid::Uuid,
    schema: u32,
    metadata: Metadata,
    state: serde_json::Value,
}

struct Working<S> {
    state: S,
    metadata: Metadata,
    failed: bool,
}

pub(crate) struct Machine<S> {
    working: Arc<std::sync::Mutex<Working<S>>>,
    pub database: Database,
    pub cluster: uuid::Uuid,
    pub publisher: watch::Sender<SnapshotHandle<S>>,
    pub sessions: watch::Sender<BTreeMap<uuid::Uuid, Session>>,
    pub max_sessions: usize,
}

impl<S: ReplicatedState> Machine<S> {
    pub async fn open(state: S, database: Database, cluster: uuid::Uuid, max_sessions: usize) -> StorageResult<Self> {
        match database.get::<usize>("session_limit").await? {
            Some(limit) if limit != max_sessions => return Err(storage_error("persisted session contract mismatch")),
            None => database.put("session_limit", max_sessions).await?,
            _ => {}
        }
        let initial: Option<(u32, Vec<u8>)> = database.get("initial_state").await?;
        let state = match initial {
            Some((schema, bytes)) => {
                if schema != S::SCHEMA_VERSION {
                    return Err(storage_error("state schema mismatch"));
                }
                serde_json::from_slice(&bytes).map_err(storage_error)?
            }
            None => {
                database
                    .put("initial_state", (S::SCHEMA_VERSION, serde_json::to_vec(&state).map_err(storage_error)?))
                    .await?;
                state
            }
        };
        let (publisher, _) = watch::channel(SnapshotHandle {
            revision: None,
            state: Arc::new(state.clone()),
            may_be_stale: true,
            published_at: tokio::time::Instant::now(),
        });
        let (sessions, _) = watch::channel(BTreeMap::new());
        let mut machine = Self {
            working: Arc::new(std::sync::Mutex::new(Working {
                state,
                metadata: Metadata::default(),
                failed: false,
            })),
            database,
            cluster,
            publisher,
            sessions,
            max_sessions,
        };
        if let Some((meta, data)) = machine.database.snapshot().await? {
            machine.restore(&meta, &data)?;
        }
        machine.publish();
        Ok(machine)
    }

    fn restore(&mut self, meta: &SnapshotMeta<u64, super::Peer>, bytes: &[u8]) -> StorageResult<()> {
        let digest = format!("{:x}", Sha256::digest(bytes));
        if digest != meta.snapshot_id {
            return Err(storage_error("snapshot digest mismatch"));
        }
        let image: Image = serde_json::from_slice(bytes).map_err(storage_error)?;
        if image.cluster != self.cluster
            || image.schema != S::SCHEMA_VERSION
            || image.metadata.applied != meta.last_log_id
            || image.metadata.membership != meta.last_membership
        {
            return Err(storage_error("snapshot identity, schema, or metadata mismatch"));
        }
        let state = serde_json::from_value(image.state).map_err(storage_error)?;
        let mut working = self.working.lock().map_err(storage_error)?;
        working.state = state;
        working.metadata = image.metadata;
        Ok(())
    }

    pub fn applied(&self) -> Option<LogId<u64>> {
        self.working.lock().expect("state owner lock").metadata.applied
    }

    pub fn sources(&self) -> BTreeMap<u64, u64> {
        let working = self.working.lock().expect("state owner lock");
        let membership = working.metadata.membership.membership();
        membership
            .voter_ids()
            .filter_map(|id| membership.get_node(&id).map(|node| (id, node.address)))
            .collect()
    }

    fn publish(&self) {
        let working = self.working.lock().expect("state owner lock");
        self.publisher.send_replace(SnapshotHandle {
            revision: working.metadata.applied.map(|log| Revision::new(log, self.cluster)),
            state: Arc::new(working.state.clone()),
            may_be_stale: true,
            published_at: tokio::time::Instant::now(),
        });
        self.sessions.send_replace(working.metadata.sessions.clone());
    }

    pub async fn verify_checkpoint(&self, meta: &SnapshotMeta<u64, super::Peer>) -> StorageResult<bool> {
        let image = {
            let working = self.working.lock().map_err(storage_error)?;
            if working.metadata.applied != meta.last_log_id {
                return Err(storage_error("checkpoint revision mismatch"));
            }
            Image {
                cluster: self.cluster,
                schema: S::SCHEMA_VERSION,
                metadata: working.metadata.clone(),
                state: super::protocol::canonical_value(&working.state).map_err(storage_error)?,
            }
        };
        let data = tokio::task::spawn_blocking(move || super::protocol::encode(&image, 256 * 1024 * 1024))
            .await
            .map_err(storage_error)?
            .map_err(storage_error)?;
        Ok(format!("{:x}", Sha256::digest(&data)) == meta.snapshot_id)
    }

    pub fn publication(
        &self,
        cancel: tokio_util::sync::CancellationToken,
        period: std::time::Duration,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send + 'static {
        let working = self.working.clone();
        let publisher = self.publisher.clone();
        let cluster = self.cluster;
        async move {
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = interval.tick() => {} }
                let working = working.clone();
                let publisher = publisher.clone();
                tokio::task::spawn_blocking(move || {
                    let working = working.lock().map_err(|e| e.to_string())?;
                    if working.failed {
                        return Err("state owner failed".into());
                    }
                    let revision = working.metadata.applied.map(|log| Revision::new(log, cluster));
                    if publisher.borrow().revision == revision {
                        return Ok::<_, String>(());
                    }
                    // I06/I08: clone and swap under the owner lock; old completions cannot replace newer publications.
                    publisher.send_replace(SnapshotHandle {
                        revision,
                        state: Arc::new(working.state.clone()),
                        may_be_stale: true,
                        published_at: tokio::time::Instant::now(),
                    });
                    Ok(())
                })
                .await
                .map_err(|e| e.to_string())??;
            }
        }
    }
}

impl<S: ReplicatedState> Working<S> {
    fn command(&mut self, command: super::Command, log_id: LogId<u64>, max_sessions: usize) -> StorageResult<Response> {
        let id = command.operation;
        let mut hash = Sha256::new();
        hash.update([u8::from(command.retire)]);
        hash.update(&command.payload);
        let digest = hash.finalize().to_vec();
        let reject = |reason: &str| Response {
            error: Some(reason.to_owned()),
            revision: Some(log_id),
            ..Response::default()
        };
        if let Some(session) = self.metadata.sessions.get(&id.client_id) {
            if session.retired {
                return Ok(if command.retire && id.sequence == session.sequence && digest == session.digest {
                    session.response.clone()
                } else {
                    reject("session retired")
                });
            }
            if id.sequence == session.sequence {
                return Ok(if digest == session.digest {
                    session.response.clone()
                } else {
                    reject("operation payload mismatch")
                });
            }
            if session.sequence.checked_add(1) != Some(id.sequence) {
                return Ok(reject("operation sequence is not next"));
            }
        } else {
            if id.sequence != 1 {
                return Ok(reject("new sessions start at sequence one"));
            }
            if self.metadata.sessions.len() >= max_sessions {
                return Ok(reject("session capacity exhausted"));
            }
        }
        let result = if command.retire {
            Vec::new()
        } else {
            let decoded = serde_json::from_slice(&command.payload).map_err(storage_error)?;
            // I02/I04: Openraft calls apply only for committed entries; sessions advance with state.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.state.apply(decoded)))
                .map_err(|_| storage_error("deterministic application panicked"))?;
            let bytes = super::protocol::canonical(&result, 64 * 1024).map_err(storage_error)?;
            let _: S::Result = serde_json::from_slice(&bytes).map_err(storage_error)?;
            bytes
        };
        let response = Response {
            result,
            error: None,
            revision: Some(log_id),
        };
        self.metadata.sessions.insert(
            id.client_id,
            Session {
                sequence: id.sequence,
                digest,
                response: response.clone(),
                retired: command.retire,
            },
        );
        Ok(response)
    }
}

pub(crate) struct Builder<S> {
    state: S,
    metadata: Metadata,
    database: Database,
    cluster: uuid::Uuid,
    failed: bool,
}

impl<S: ReplicatedState> RaftSnapshotBuilder<TypeConfig> for Builder<S> {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TypeConfig>> {
        if self.failed {
            return Err(storage_error("state owner failed"));
        }
        let image = Image {
            cluster: self.cluster,
            schema: S::SCHEMA_VERSION,
            metadata: self.metadata.clone(),
            state: super::protocol::canonical_value(&self.state).map_err(storage_error)?,
        };
        let data = tokio::task::spawn_blocking(move || super::protocol::encode(&image, 256 * 1024 * 1024))
            .await
            .map_err(storage_error)?
            .map_err(storage_error)?;
        let meta = SnapshotMeta {
            last_log_id: self.metadata.applied,
            last_membership: self.metadata.membership.clone(),
            snapshot_id: format!("{:x}", Sha256::digest(&data)),
        };
        self.database.save_snapshot(meta.clone(), data.clone()).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl<S: ReplicatedState> RaftStateMachine<TypeConfig> for Machine<S> {
    type SnapshotBuilder = Builder<S>;
    async fn applied_state(&mut self) -> StorageResult<(Option<LogId<u64>>, StoredMembership<u64, super::Peer>)> {
        let working = self.working.lock().map_err(storage_error)?;
        Ok((working.metadata.applied, working.metadata.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<Response>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut working = self.working.lock().map_err(storage_error)?;
        if working.failed {
            return Err(storage_error("state owner failed"));
        }
        let mut responses = Vec::new();
        for entry in entries {
            let response = match entry.payload {
                EntryPayload::Blank => Response::default(),
                EntryPayload::Membership(membership) => {
                    working.metadata.membership = StoredMembership::new(Some(entry.log_id), membership);
                    Response::default()
                }
                EntryPayload::Normal(command) => match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    working.command(command, entry.log_id, self.max_sessions)
                }))
                .unwrap_or_else(|_| Err(storage_error("state machine command panicked")))
                {
                    Ok(response) => response,
                    Err(error) => {
                        working.failed = true;
                        return Err(error);
                    }
                },
            };
            working.metadata.applied = Some(entry.log_id);
            responses.push(response);
        }
        self.sessions.send_replace(working.metadata.sessions.clone());
        Ok(responses)
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let working = self.working.lock().expect("state owner lock");
        Builder {
            state: working.state.clone(),
            metadata: working.metadata.clone(),
            database: self.database.clone(),
            cluster: self.cluster,
            failed: working.failed,
        }
    }
    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, super::Peer>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StorageResult<()> {
        let bytes = snapshot.into_inner();
        let (state, metadata) = {
            let working = self.working.lock().map_err(storage_error)?;
            (working.state.clone(), working.metadata.clone())
        };
        let mut replacement = Self {
            working: Arc::new(std::sync::Mutex::new(Working {
                state,
                metadata,
                failed: false,
            })),
            database: self.database.clone(),
            cluster: self.cluster,
            publisher: self.publisher.clone(),
            sessions: self.sessions.clone(),
            max_sessions: self.max_sessions,
        };
        replacement.restore(meta, &bytes)?;
        self.database.save_snapshot(meta.clone(), bytes).await?;
        let mut working = self.working.lock().map_err(storage_error)?;
        let mut replaced = replacement.working.lock().map_err(storage_error)?;
        std::mem::swap(&mut *working, &mut *replaced);
        drop(working);
        drop(replaced);
        self.publish();
        Ok(())
    }
    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        Ok(self.database.snapshot().await?.map(|(meta, data)| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Counter(u64);
    impl ReplicatedState for Counter {
        type Command = u64;
        type Result = u64;
        const SCHEMA_VERSION: u32 = 1;
        fn apply(&mut self, amount: u64) -> u64 {
            self.0 += amount;
            self.0
        }
    }

    #[tokio::test]
    async fn t13_corrupt_snapshot_and_failed_install_preserve_publication() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = uuid::Uuid::new_v4();
        let database = Database::open(&dir.path().join("raft.db"), cluster, 1).await.unwrap();
        let mut machine = Machine::open(Counter(10), database.clone(), cluster, 10).await.unwrap();
        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let mut corrupt = snapshot.snapshot.into_inner();
        corrupt.push(0);
        assert!(
            machine
                .install_snapshot(&snapshot.meta, Box::new(Cursor::new(corrupt)))
                .await
                .is_err()
        );
        assert_eq!(machine.publisher.borrow().state.0, 10);
        builder.state = Counter(20);
        let snapshot = builder.build_snapshot().await.unwrap();
        database
            .call(|c| c.pragma_update(None, "query_only", true).map_err(storage_error))
            .await
            .unwrap();
        assert!(
            machine
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
                .is_err()
        );
        assert_eq!(machine.publisher.borrow().state.0, 10);
        drop(machine);
        drop(builder);
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn initial_state_is_persisted_and_not_replaced_by_restart_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = uuid::Uuid::new_v4();
        let path = dir.path().join("raft.db");
        let database = Database::open(&path, cluster, 1).await.unwrap();
        let machine = Machine::open(Counter(10), database.clone(), cluster, 10).await.unwrap();
        drop(machine);
        database.shutdown().await.unwrap();
        let database = Database::open(&path, cluster, 1).await.unwrap();
        let machine = Machine::open(Counter(99), database.clone(), cluster, 10).await.unwrap();
        assert_eq!(machine.publisher.borrow().state.0, 10);
        drop(machine);
        database.shutdown().await.unwrap();
    }
}
