//! Disk persistence helpers for recoverable state snapshots.

use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use message_encoding::MessageEncoding;

use crate::state::{
    deterministic_state::DeterministicState, recoverable_state::RecoverableState, subscribable_state::StateHandle,
};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Loads a recoverable state snapshot from disk.
pub fn load_recoverable_state<D>(path: impl AsRef<Path>) -> io::Result<RecoverableState<D>>
where
    D: DeterministicState + MessageEncoding,
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    RecoverableState::read_from(&mut reader)
}

/// Loads a recoverable state snapshot when it exists.
pub fn try_load_recoverable_state<D>(path: impl AsRef<Path>) -> io::Result<Option<RecoverableState<D>>>
where
    D: DeterministicState + MessageEncoding,
{
    match load_recoverable_state(path) {
        Ok(state) => Ok(Some(state)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Loads a recoverable state snapshot from disk, or creates a fresh one.
pub fn load_or_init_recoverable_state<D>(
    path: impl AsRef<Path>,
    init: impl FnOnce() -> RecoverableState<D>,
) -> io::Result<RecoverableState<D>>
where
    D: DeterministicState + MessageEncoding,
{
    Ok(match try_load_recoverable_state(path)? {
        Some(state) => state,
        None => init(),
    })
}

/// Saves a recoverable state snapshot to disk using an atomic rename.
pub fn save_recoverable_state<D>(path: impl AsRef<Path>, state: &RecoverableState<D>) -> io::Result<()>
where
    D: DeterministicState + MessageEncoding,
{
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_path = temp_path_for(path);
    {
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);
        state.write_to(&mut writer)?;
        let file = writer.into_inner()?;
        file.sync_all()?;
    }

    fs::rename(&temp_path, path).inspect_err(|_| {
        let _ = fs::remove_file(&temp_path);
    })
}

fn temp_path_for(path: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "state".into());
    let temp_name = format!(".{}.tmp-{}-{}", file_name, std::process::id(), counter);
    path.with_file_name(temp_name)
}

/// A disk-backed recoverable state snapshot location.
#[derive(Clone, Debug)]
pub struct RecoverableStateStore {
    path: PathBuf,
}

impl RecoverableStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load<D>(&self) -> io::Result<RecoverableState<D>>
    where
        D: DeterministicState + MessageEncoding,
    {
        load_recoverable_state(&self.path)
    }

    pub fn try_load<D>(&self) -> io::Result<Option<RecoverableState<D>>>
    where
        D: DeterministicState + MessageEncoding,
    {
        try_load_recoverable_state(&self.path)
    }

    pub fn load_or_init<D>(&self, init: impl FnOnce() -> RecoverableState<D>) -> io::Result<RecoverableState<D>>
    where
        D: DeterministicState + MessageEncoding,
    {
        load_or_init_recoverable_state(&self.path, init)
    }

    pub fn save<D>(&self, state: &RecoverableState<D>) -> io::Result<()>
    where
        D: DeterministicState + MessageEncoding,
    {
        save_recoverable_state(&self.path, state)
    }

    /// Periodically saves snapshots read from `handle`.
    ///
    /// Dropping the returned guard stops the background worker after it writes
    /// one final snapshot. Use [`PeriodicStateSaver::shutdown`] when callers
    /// need to observe final-save errors.
    pub fn save_periodically<D>(&self, handle: StateHandle<D>, interval: Duration) -> PeriodicStateSaver<D>
    where
        D: DeterministicState + MessageEncoding,
    {
        PeriodicStateSaver::start(self.clone(), handle, interval)
    }
}

/// Guard for a background periodic state snapshot worker.
pub struct PeriodicStateSaver<D: DeterministicState + MessageEncoding> {
    shutdown_tx: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<io::Result<()>>>,
    _state: std::marker::PhantomData<D>,
}

impl<D> PeriodicStateSaver<D>
where
    D: DeterministicState + MessageEncoding,
{
    fn start(store: RecoverableStateStore, mut handle: StateHandle<D>, interval: Duration) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            loop {
                match shutdown_rx.recv_timeout(interval) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        if let Err(error) = save_from_handle(&store, &mut handle) {
                            tracing::warn!(path = ?store.path(), ?error, "failed to save recoverable state snapshot");
                        }
                    }
                }
            }

            save_from_handle(&store, &mut handle)
        });

        Self {
            shutdown_tx: Some(shutdown_tx),
            worker: Some(worker),
            _state: std::marker::PhantomData,
        }
    }

    /// Stops the background worker and returns the result of the final save.
    pub fn shutdown(mut self) -> io::Result<()> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> io::Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(worker) = self.worker.take() {
            match worker.join() {
                Ok(result) => result,
                Err(_) => Err(io::Error::other("recoverable state snapshot worker panicked")),
            }
        } else {
            Ok(())
        }
    }
}

impl<D> Drop for PeriodicStateSaver<D>
where
    D: DeterministicState + MessageEncoding,
{
    fn drop(&mut self) {
        if let Err(error) = self.shutdown_inner() {
            tracing::warn!(?error, "failed to save recoverable state snapshot during shutdown");
        }
    }
}

fn save_from_handle<D>(store: &RecoverableStateStore, handle: &mut StateHandle<D>) -> io::Result<()>
where
    D: DeterministicState + MessageEncoding,
{
    let state = handle.read_with(Clone::clone);
    store.save(&state)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::atomic::{AtomicU64, Ordering},
    };

    use sequenced_broadcast::SequencedBroadcastSettings;

    use super::*;
    use crate::state::{
        deterministic_state::DeterministicState, recoverable_state::RecoverableStateAction,
        subscribable_state::SubscribableState,
    };

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct CounterState(u64);

    impl DeterministicState for CounterState {
        type Action = u64;
        type AuthorityAction = u64;

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

    impl MessageEncoding for CounterState {
        const STATIC_SIZE: Option<usize> = u64::STATIC_SIZE;
        const MAX_SIZE: Option<usize> = u64::MAX_SIZE;

        fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
            self.0.write_to(out)
        }

        fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
            Ok(Self(MessageEncoding::read_from(read)?))
        }
    }

    fn temp_snapshot_path(test_name: &str) -> PathBuf {
        let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("sharedstate-persistence-test-{}-{}-{}", std::process::id(), test_name, counter))
            .join("state.bin")
    }

    #[test]
    fn load_or_init_uses_init_only_when_snapshot_is_missing() {
        let path = temp_snapshot_path("load-or-init");
        let store = RecoverableStateStore::new(&path);
        let initial = store
            .load_or_init(|| RecoverableState::new(7, CounterState::default()))
            .unwrap();

        assert_eq!(initial.details().next_seq(), 1);
        store.save(&RecoverableState::new(8, CounterState(12))).unwrap();

        let loaded: RecoverableState<CounterState> = store
            .load_or_init(|| RecoverableState::new(9, CounterState::default()))
            .unwrap();

        assert_eq!(loaded.details().next_seq(), 1);
        assert_eq!(loaded.state(), &CounterState(12));
    }

    #[test]
    fn save_round_trips_recoverable_state() {
        let path = temp_snapshot_path("round-trip");
        let state = RecoverableState::new(99, CounterState(42));

        save_recoverable_state(&path, &state).unwrap();
        let loaded: RecoverableState<CounterState> = load_recoverable_state(&path).unwrap();

        assert_eq!(loaded, state);
    }

    #[tokio::test]
    async fn periodic_saver_writes_final_snapshot_on_shutdown() {
        let path = temp_snapshot_path("periodic-final");
        let store = RecoverableStateStore::new(&path);
        let state = SubscribableState::new(
            RecoverableState::new(5, CounterState::default()),
            SequencedBroadcastSettings::default(),
        )
        .unwrap();

        state
            .update(
                [
                    RecoverableStateAction::StateAction { action: 1 },
                    RecoverableStateAction::StateAction { action: 2 },
                ]
                .into_iter(),
            )
            .await;

        let saver = store.save_periodically(state.create_handle(), Duration::from_secs(60));
        saver.shutdown().unwrap();

        let loaded: RecoverableState<CounterState> = store.load().unwrap();
        assert_eq!(loaded.state(), &CounterState(2));
        assert_eq!(loaded.details().next_seq(), 3);
    }
}
