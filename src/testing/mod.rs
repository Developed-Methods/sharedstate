use std::time::{Duration, Instant};

use tokio::sync::{RwLock, RwLockWriteGuard};

mod message_io_tests;
pub mod test_sync_io;
pub mod state_tests;

pub fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_test_writer()
        .try_init();
}

pub fn blocking_rw_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    let start = Instant::now();

    loop {
        if let Ok(lock) = lock.try_write() {
            break lock;
        }

        if Duration::from_millis(10) < start.elapsed() {
            tracing::error!("panic waiting for write lock");
            panic!("Waited too long for lock");
        }

        std::thread::yield_now();
    }
}

