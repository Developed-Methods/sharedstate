use std::{
    ops::{Bound, RangeBounds},
    path::Path,
    sync::Arc,
};

use openraft::{
    LogId, LogState, RaftLogReader, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{mpsc, oneshot};

use super::{Entry, StorageResult, TypeConfig, storage_error};

fn lease_clock() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

enum Job {
    Call(Box<dyn FnOnce(&mut Connection) + Send>),
    Stop,
}

#[derive(Clone)]
pub(crate) struct Database {
    sender: mpsc::Sender<Job>,
    worker: Arc<tokio::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl Database {
    pub async fn open(path: &Path, cluster: uuid::Uuid, node: u64) -> StorageResult<Self> {
        let path = path.to_owned();
        let (sender, mut receiver) = mpsc::channel::<Job>(64);
        let (ready, opened) = oneshot::channel();
        let worker = std::thread::Builder::new()
            .name(format!("raft-sqlite-{node}"))
            .spawn(move || {
                let connection = (|| -> Result<Connection, Box<dyn std::error::Error>> {
                    let mut connection = Connection::open(path)?;
                    connection.busy_timeout(std::time::Duration::ZERO)?;
                    connection.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
                    connection.pragma_update(None, "journal_mode", "WAL")?;
                    connection.pragma_update(None, "synchronous", "FULL")?;
                    let mode: String = connection.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
                    let sync: u64 = connection.pragma_query_value(None, "synchronous", |r| r.get(0))?;
                    if mode != "wal" || sync != 2 {
                        return Err("durable SQLite pragmas unavailable".into());
                    }
                    connection.execute_batch(include_str!("schema.sql"))?;
                    connection
                        .execute("UPDATE snapshot_archive SET expires = ?1", [lease_clock().saturating_add(60_000)])?;
                    let transaction = connection.transaction()?;
                    let expected = (1u32, cluster, node);
                    let identity: Option<(u32, uuid::Uuid, u64)> = get(&transaction, "identity")?;
                    match identity {
                        Some(value) if value != expected => return Err("storage identity or format mismatch".into()),
                        None => put(&transaction, "identity", &expected)?,
                        _ => {}
                    }
                    transaction.commit()?;
                    Ok(connection)
                })();
                match connection {
                    Ok(mut connection) => {
                        let _ = ready.send(Ok(()));
                        while let Some(job) = receiver.blocking_recv() {
                            match job {
                                Job::Call(f) => f(&mut connection),
                                Job::Stop => break,
                            }
                        }
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                    }
                }
            })
            .map_err(storage_error)?;
        opened.await.map_err(storage_error)?.map_err(storage_error)?;
        Ok(Self {
            sender,
            worker: Arc::new(tokio::sync::Mutex::new(Some(worker))),
        })
    }

    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> StorageResult<T> + Send + 'static,
    ) -> StorageResult<T> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Job::Call(Box::new(move |connection| {
                let _ = tx.send(f(connection));
            })))
            .await
            .map_err(storage_error)?;
        rx.await.map_err(storage_error)?
    }

    pub async fn get<T: DeserializeOwned + Send + 'static>(&self, key: &'static str) -> StorageResult<Option<T>> {
        self.call(move |c| get(c, key).map_err(storage_error)).await
    }

    pub async fn put<T: Serialize + Send + 'static>(&self, key: &'static str, value: T) -> StorageResult<()> {
        self.call(move |c| put(c, key, &value).map_err(storage_error)).await
    }

    pub async fn save_snapshot(
        &self,
        meta: openraft::SnapshotMeta<u64, super::Peer>,
        data: Vec<u8>,
    ) -> StorageResult<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(storage_error)?;
            let current: Option<openraft::SnapshotMeta<u64, super::Peer>> =
                get(&tx, "snapshot_meta").map_err(storage_error)?;
            if current
                .as_ref()
                .is_some_and(|current| current.last_log_id > meta.last_log_id)
            {
                return Ok(());
            }
            put(&tx, "snapshot_meta", &meta).map_err(storage_error)?;
            tx.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('snapshot_data', ?1)", [data])
                .map_err(storage_error)?;
            let transfer: Option<(openraft::SnapshotMeta<u64, super::Peer>, u64, u64)> =
                get(&tx, "transfer").map_err(storage_error)?;
            if transfer.is_some_and(|(pending, _, _)| pending.snapshot_id == meta.snapshot_id) {
                tx.execute("DELETE FROM snapshot_chunks", []).map_err(storage_error)?;
                tx.execute("DELETE FROM metadata WHERE key = 'transfer'", [])
                    .map_err(storage_error)?;
            }
            tx.commit().map_err(storage_error)
        })
        .await
    }

    pub async fn snapshot(&self) -> StorageResult<Option<(openraft::SnapshotMeta<u64, super::Peer>, Vec<u8>)>> {
        self.call(|c| {
            let meta = get(c, "snapshot_meta").map_err(storage_error)?;
            match meta {
                None => Ok(None),
                Some(meta) => {
                    let data = c
                        .query_row("SELECT value FROM metadata WHERE key = 'snapshot_data'", [], |row| row.get(0))
                        .map_err(storage_error)?;
                    Ok(Some((meta, data)))
                }
            }
        })
        .await
    }

    pub async fn snapshot_manifest(&self) -> StorageResult<Option<(openraft::SnapshotMeta<u64, super::Peer>, u64)>> {
        self.call(|c| {
            let meta = get(c, "snapshot_meta").map_err(storage_error)?;
            match meta {
                None => Ok(None),
                Some(meta) => {
                    let length = c
                        .query_row("SELECT length(value) FROM metadata WHERE key = 'snapshot_data'", [], |row| {
                            row.get(0)
                        })
                        .map_err(storage_error)?;
                    Ok(Some((meta, length)))
                }
            }
        })
        .await
    }

    pub async fn pending_snapshot_id(&self) -> StorageResult<Option<String>> {
        let transfer: Option<(openraft::SnapshotMeta<u64, super::Peer>, u64, u64)> = self.get("transfer").await?;
        Ok(transfer.map(|(meta, _, _)| meta.snapshot_id))
    }

    pub async fn lease_snapshot(
        &self,
        preferred: Option<String>,
    ) -> StorageResult<Option<(openraft::SnapshotMeta<u64, super::Peer>, u64)>> {
        self.call(move |c| {
            let now = lease_clock();
            let tx = c.transaction().map_err(storage_error)?;
            tx.execute("DELETE FROM snapshot_archive WHERE expires < ?1", [now]).map_err(storage_error)?;
            if let Some(id) = preferred {
                let existing: Option<(Vec<u8>, u64)> = tx.query_row("SELECT meta, length(data) FROM snapshot_archive WHERE id = ?1", [&id], |row| Ok((row.get(0)?, row.get(1)?))).optional().map_err(storage_error)?;
                if let Some((meta, bytes)) = existing {
                    let meta = serde_json::from_slice(&meta).map_err(storage_error)?;
                    tx.execute("UPDATE snapshot_archive SET expires = ?1 WHERE id = ?2", rusqlite::params![now.saturating_add(60_000), id]).map_err(storage_error)?;
                    tx.commit().map_err(storage_error)?;
                    return Ok(Some((meta, bytes)));
                }
            }
            let Some(meta): Option<openraft::SnapshotMeta<u64, super::Peer>> = get(&tx, "snapshot_meta").map_err(storage_error)? else { return Ok(None); };
            let bytes: u64 = tx.query_row("SELECT length(value) FROM metadata WHERE key = 'snapshot_data'", [], |row| row.get(0)).map_err(storage_error)?;
            if bytes > 256 * 1024 * 1024 { return Ok(None); }
            let existing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM snapshot_archive WHERE id = ?1)", [&meta.snapshot_id], |row| row.get(0)).map_err(storage_error)?;
            if !existing {
                let retained: u64 = tx.query_row("SELECT COALESCE(SUM(length(data)), 0) FROM snapshot_archive", [], |row| row.get(0)).map_err(storage_error)?;
                if retained.saturating_add(bytes) > 512 * 1024 * 1024 { return Ok(None); }
                tx.execute("INSERT INTO snapshot_archive (id, meta, data, expires) SELECT ?1, ?2, value, ?3 FROM metadata WHERE key = 'snapshot_data'", rusqlite::params![meta.snapshot_id, serde_json::to_vec(&meta).map_err(storage_error)?, now.saturating_add(60_000)]).map_err(storage_error)?;
            } else {
                tx.execute("UPDATE snapshot_archive SET expires = ?1 WHERE id = ?2", rusqlite::params![now.saturating_add(60_000), meta.snapshot_id]).map_err(storage_error)?;
            }
            tx.commit().map_err(storage_error)?;
            Ok(Some((meta, bytes)))
        }).await
    }

    pub async fn snapshot_chunk(&self, snapshot_id: String, offset: u64, length: u64) -> StorageResult<Vec<u8>> {
        if length > 256 * 1024 || offset > 256 * 1024 * 1024 {
            return Err(storage_error("snapshot chunk limit exceeded"));
        }
        self.call(move |c| {
            let meta: openraft::SnapshotMeta<u64, super::Peer> = get(c, "snapshot_meta")
                .map_err(storage_error)?
                .ok_or_else(|| storage_error("snapshot missing"))?;
            use std::io::{Read, Seek, SeekFrom};
            let (table, column, rowid, total) = if meta.snapshot_id == snapshot_id {
                let (rowid, total): (i64, u64) = c
                    .query_row("SELECT rowid, length(value) FROM metadata WHERE key = 'snapshot_data'", [], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(storage_error)?;
                ("metadata", "value", rowid, total)
            } else {
                let (rowid, total): (i64, u64) = c
                    .query_row(
                        "SELECT rowid, length(data) FROM snapshot_archive WHERE id = ?1 AND expires >= ?2",
                        rusqlite::params![snapshot_id, lease_clock()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(storage_error)?;
                ("snapshot_archive", "data", rowid, total)
            };
            if offset > total {
                return Err(storage_error("snapshot offset exceeds length"));
            }
            let mut blob = c
                .blob_open(rusqlite::DatabaseName::Main, table, column, rowid, true)
                .map_err(storage_error)?;
            let mut bytes = vec![0; length.min(total - offset) as usize];
            blob.seek(SeekFrom::Start(offset)).map_err(storage_error)?;
            blob.read_exact(&mut bytes).map_err(storage_error)?;
            Ok(bytes)
        })
        .await
    }

    pub async fn resume_snapshot(
        &self,
        meta: openraft::SnapshotMeta<u64, super::Peer>,
        size: u64,
    ) -> StorageResult<u64> {
        if size > 256 * 1024 * 1024 {
            return Err(storage_error("snapshot quota exceeded"));
        }
        self.call(move |c| {
            let tx = c.transaction().map_err(storage_error)?;
            let current: Option<(openraft::SnapshotMeta<u64, super::Peer>, u64, u64)> =
                get(&tx, "transfer").map_err(storage_error)?;
            if let Some((old, total, received)) = current
                && old == meta
                && total == size
            {
                return Ok(received);
            }
            tx.execute("DELETE FROM snapshot_chunks", []).map_err(storage_error)?;
            put(&tx, "transfer", &(meta, size, 0u64)).map_err(storage_error)?;
            tx.commit().map_err(storage_error)?;
            Ok(0)
        })
        .await
    }

    pub async fn receive_chunk(&self, snapshot_id: String, offset: u64, data: Vec<u8>) -> StorageResult<u64> {
        if data.is_empty() || data.len() > 256 * 1024 {
            return Err(storage_error("invalid snapshot chunk size"));
        }
        self.call(move |c| {
            let tx = c.transaction().map_err(storage_error)?;
            let (meta, total, received): (openraft::SnapshotMeta<u64, super::Peer>, u64, u64) = get(&tx, "transfer")
                .map_err(storage_error)?
                .ok_or_else(|| storage_error("no transfer"))?;
            if meta.snapshot_id != snapshot_id
                || offset != received
                || data.len() as u64 > total.saturating_sub(received)
            {
                return Err(storage_error("snapshot chunk identity or range mismatch"));
            }
            let next = received + data.len() as u64;
            tx.execute("INSERT INTO snapshot_chunks (position, data) VALUES (?1, ?2)", rusqlite::params![offset, data])
                .map_err(storage_error)?;
            put(&tx, "transfer", &(meta, total, next)).map_err(storage_error)?;
            tx.commit().map_err(storage_error)?;
            Ok(next)
        })
        .await
    }

    pub async fn assembled_snapshot(&self, snapshot_id: String) -> StorageResult<Vec<u8>> {
        self.call(move |c| {
            let (meta, total, received): (openraft::SnapshotMeta<u64, super::Peer>, u64, u64) = get(c, "transfer")
                .map_err(storage_error)?
                .ok_or_else(|| storage_error("no transfer"))?;
            if meta.snapshot_id != snapshot_id || total != received {
                return Err(storage_error("snapshot incomplete"));
            }
            let mut result = Vec::with_capacity(total as usize);
            let mut statement = c
                .prepare("SELECT position, data FROM snapshot_chunks ORDER BY position")
                .map_err(storage_error)?;
            let chunks = statement
                .query_map([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?)))
                .map_err(storage_error)?;
            for chunk in chunks {
                let (offset, bytes) = chunk.map_err(storage_error)?;
                if offset != result.len() as u64 {
                    return Err(storage_error("snapshot chunk gap"));
                }
                result.extend(bytes);
            }
            if result.len() as u64 != total {
                return Err(storage_error("snapshot byte count mismatch"));
            }
            Ok(result)
        })
        .await
    }

    pub async fn shutdown(self) -> StorageResult<()> {
        let mut failure = self
            .call(|c| c.execute_batch("PRAGMA wal_checkpoint(FULL)").map_err(storage_error))
            .await
            .err();
        if let Err(error) = self.sender.send(Job::Stop).await {
            failure.get_or_insert_with(|| storage_error(error));
        }
        let worker = self.worker.lock().await.take();
        drop(self.sender);
        if let Some(worker) = worker {
            let joined =
                tokio::task::spawn_blocking(move || worker.join().map_err(|_| storage_error("SQLite worker panicked")))
                    .await
                    .map_err(storage_error)
                    .and_then(|result| result);
            if let Err(error) = joined {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }
}

pub(crate) fn get<T: DeserializeOwned>(c: &Connection, key: &str) -> Result<Option<T>, Box<dyn std::error::Error>> {
    let bytes: Option<Vec<u8>> = c
        .query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| row.get(0))
        .optional()?;
    Ok(bytes.map(|b| serde_json::from_slice(&b)).transpose()?)
}

pub(crate) fn put<T: Serialize>(c: &Connection, key: &str, value: &T) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(value)?;
    c.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)", rusqlite::params![key, bytes])?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct LogStore(pub Database);

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Send>(&mut self, range: R) -> StorageResult<Vec<Entry>> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => match x.checked_add(1) {
                Some(x) => x,
                None => return Ok(vec![]),
            },
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => match x.checked_sub(1) {
                Some(x) => x,
                None => return Ok(vec![]),
            },
            Bound::Unbounded => u64::MAX,
        };
        self.0
            .call(move |c| {
                let mut statement = c
                    .prepare("SELECT entry FROM raft_log WHERE position >= ?1 AND position <= ?2 ORDER BY position")
                    .map_err(storage_error)?;
                let rows = statement
                    .query_map(rusqlite::params![start.to_be_bytes().as_slice(), end.to_be_bytes().as_slice()], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })
                    .map_err(storage_error)?;
                rows.map(|row| serde_json::from_slice(&row.map_err(storage_error)?).map_err(storage_error))
                    .collect()
            })
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        self.0
            .call(|c| {
                let last_purged_log_id = get(c, "purged").map_err(storage_error)?;
                let bytes: Option<Vec<u8>> = c
                    .query_row("SELECT entry FROM raft_log ORDER BY position DESC LIMIT 1", [], |r| r.get(0))
                    .optional()
                    .map_err(storage_error)?;
                let entry: Option<Entry> = bytes
                    .map(|b| serde_json::from_slice(&b))
                    .transpose()
                    .map_err(storage_error)?;
                Ok(LogState {
                    last_purged_log_id,
                    last_log_id: entry.map(|e| e.log_id).or(last_purged_log_id),
                })
            })
            .await
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> StorageResult<()> {
        self.0.put("vote", *vote).await
    }
    async fn read_vote(&mut self) -> StorageResult<Option<Vote<u64>>> {
        self.0.get("vote").await
    }
    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> StorageResult<()> {
        self.0.put("committed", committed).await
    }
    async fn read_committed(&mut self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self.0.get("committed").await?.flatten())
    }
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .0
            .call(move |c| {
                let tx = c.transaction().map_err(storage_error)?;
                for entry in entries {
                    let bytes = serde_json::to_vec(&entry).map_err(storage_error)?;
                    tx.execute(
                        "INSERT OR REPLACE INTO raft_log (position, entry) VALUES (?1, ?2)",
                        rusqlite::params![entry.log_id.index.to_be_bytes().as_slice(), bytes],
                    )
                    .map_err(storage_error)?;
                }
                tx.commit().map_err(storage_error)
            })
            .await;
        // I03: acknowledge log durability only after the FULL SQLite transaction commits.
        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|e| std::io::Error::other(e.to_string())),
        );
        result
    }
    async fn truncate(&mut self, log_id: LogId<u64>) -> StorageResult<()> {
        self.0
            .call(move |c| {
                c.execute("DELETE FROM raft_log WHERE position >= ?1", [log_id.index.to_be_bytes().as_slice()])
                    .map_err(storage_error)?;
                Ok(())
            })
            .await
    }
    async fn purge(&mut self, log_id: LogId<u64>) -> StorageResult<()> {
        self.0
            .call(move |c| {
                let tx = c.transaction().map_err(storage_error)?;
                put(&tx, "purged", &log_id).map_err(storage_error)?;
                tx.execute("DELETE FROM raft_log WHERE position <= ?1", [log_id.index.to_be_bytes().as_slice()])
                    .map_err(storage_error)?;
                tx.commit().map_err(storage_error)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::{
        TypeConfig,
        machine::{Machine, ReplicatedState},
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Counter(u64);
    impl ReplicatedState for Counter {
        type Command = u64;
        type Result = u64;
        const SCHEMA_VERSION: u32 = 1;
        fn apply(&mut self, delta: u64) -> u64 {
            self.0 += delta;
            self.0
        }
    }
    struct Builder;
    impl openraft::testing::StoreBuilder<TypeConfig, LogStore, Machine<Counter>, tempfile::TempDir> for Builder {
        async fn build(&self) -> StorageResult<(tempfile::TempDir, LogStore, Machine<Counter>)> {
            let directory = tempfile::tempdir().map_err(storage_error)?;
            let cluster = uuid::Uuid::nil();
            let database = Database::open(&directory.path().join("raft.db"), cluster, 0).await?;
            let machine = Machine::open(Counter::default(), database.clone(), cluster, 100).await?;
            Ok((directory, LogStore(database), machine))
        }
    }
    #[test]
    fn upstream_storage_contract() {
        openraft::testing::Suite::test_all(Builder).unwrap();
    }

    #[tokio::test]
    async fn t13_leased_snapshot_survives_new_checkpoint_and_source_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft.db");
        let cluster = uuid::Uuid::new_v4();
        let database = Database::open(&path, cluster, 1).await.unwrap();
        let first = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "first".into(),
        };
        let second = openraft::SnapshotMeta {
            snapshot_id: "second".into(),
            ..first.clone()
        };
        database.save_snapshot(first.clone(), vec![1; 1024]).await.unwrap();
        assert_eq!(database.lease_snapshot(None).await.unwrap().unwrap().0, first);
        database.save_snapshot(second.clone(), vec![2; 2048]).await.unwrap();
        assert_eq!(database.snapshot_chunk("first".into(), 0, 1024).await.unwrap(), vec![1; 1024]);
        database.shutdown().await.unwrap();
        let database = Database::open(&path, cluster, 1).await.unwrap();
        assert_eq!(database.lease_snapshot(Some("first".into())).await.unwrap().unwrap().0, first);
        assert_eq!(database.snapshot_chunk("first".into(), 0, 1024).await.unwrap(), vec![1; 1024]);
        database
            .call(|c| {
                c.execute("UPDATE snapshot_archive SET expires = -1", [])
                    .map_err(storage_error)?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(database.snapshot_chunk("first".into(), 0, 1024).await.is_err());
        assert_eq!(database.lease_snapshot(Some("first".into())).await.unwrap().unwrap().0, second);
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_checkpoint_cannot_replace_newer_install_or_discard_inflight_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::open(&dir.path().join("raft.db"), uuid::Uuid::new_v4(), 1)
            .await
            .unwrap();
        let meta = |index| openraft::SnapshotMeta {
            last_log_id: Some(openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index)),
            last_membership: Default::default(),
            snapshot_id: format!("snapshot-{index}"),
        };
        database.resume_snapshot(meta(3), 2).await.unwrap();
        database.receive_chunk(meta(3).snapshot_id, 0, vec![3]).await.unwrap();
        database.save_snapshot(meta(2), vec![2]).await.unwrap();
        assert_eq!(database.resume_snapshot(meta(3), 2).await.unwrap(), 1);
        database.save_snapshot(meta(1), vec![1]).await.unwrap();
        assert_eq!(database.snapshot().await.unwrap().unwrap(), (meta(2), vec![2]));
        database.receive_chunk(meta(3).snapshot_id, 1, vec![3]).await.unwrap();
        database.save_snapshot(meta(3), vec![3, 3]).await.unwrap();
        assert_eq!(database.pending_snapshot_id().await.unwrap(), None);
        database.save_snapshot(meta(2), vec![2]).await.unwrap();
        assert_eq!(database.snapshot().await.unwrap().unwrap(), (meta(3), vec![3, 3]));
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn t13_snapshot_chunks_resume_after_restart_and_reject_gaps() {
        use sha2::{Digest, Sha256};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let cluster = uuid::Uuid::new_v4();
        let bytes = vec![7u8; 128 * 1024];
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: format!("{:x}", Sha256::digest(&bytes)),
        };
        let database = Database::open(&path, cluster, 1).await.unwrap();
        assert_eq!(
            database
                .resume_snapshot(meta.clone(), bytes.len() as u64)
                .await
                .unwrap(),
            0
        );
        assert!(
            database
                .receive_chunk(meta.snapshot_id.clone(), 1, vec![7; 64 * 1024])
                .await
                .is_err()
        );
        database
            .receive_chunk(meta.snapshot_id.clone(), 0, bytes[..64 * 1024].to_vec())
            .await
            .unwrap();
        assert!(database.assembled_snapshot(meta.snapshot_id.clone()).await.is_err());
        database.shutdown().await.unwrap();
        let database = Database::open(&path, cluster, 1).await.unwrap();
        assert_eq!(
            database
                .resume_snapshot(meta.clone(), bytes.len() as u64)
                .await
                .unwrap(),
            64 * 1024
        );
        database
            .receive_chunk(meta.snapshot_id.clone(), 64 * 1024, bytes[64 * 1024..].to_vec())
            .await
            .unwrap();
        assert_eq!(database.assembled_snapshot(meta.snapshot_id.clone()).await.unwrap(), bytes);
        database.save_snapshot(meta.clone(), bytes.clone()).await.unwrap();
        assert_eq!(
            database
                .snapshot_chunk(meta.snapshot_id.clone(), 60 * 1024, 64 * 1024)
                .await
                .unwrap(),
            bytes[60 * 1024..124 * 1024]
        );
        assert!(database.snapshot_chunk("expired".into(), 0, 1).await.is_err());
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn exclusive_storage_and_durable_write_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let cluster = uuid::Uuid::new_v4();
        let database = Database::open(&path, cluster, 1).await.unwrap();
        assert!(Database::open(&path, cluster, 1).await.is_err());
        let mut log = LogStore(database.clone());
        log.save_vote(&Vote::new(2, 1)).await.unwrap();
        database
            .call(|c| c.pragma_update(None, "query_only", true).map_err(storage_error))
            .await
            .unwrap();
        assert!(log.save_vote(&Vote::new(3, 1)).await.is_err());
        assert_eq!(log.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
        drop(log);
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn identity_and_vote_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let cluster = uuid::Uuid::new_v4();
        let db = Database::open(&path, cluster, 17).await.unwrap();
        let mut log = LogStore(db.clone());
        log.save_vote(&Vote::new(9, 17)).await.unwrap();
        drop(log);
        db.shutdown().await.unwrap();
        assert!(Database::open(&path, uuid::Uuid::new_v4(), 17).await.is_err());
        assert!(Database::open(&path, cluster, 18).await.is_err());
        let db = Database::open(&path, cluster, 17).await.unwrap();
        let mut log = LogStore(db.clone());
        assert_eq!(log.read_vote().await.unwrap(), Some(Vote::new(9, 17)));
        drop(log);
        db.shutdown().await.unwrap();
    }
}
