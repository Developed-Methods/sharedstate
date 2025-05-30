use std::time::Duration;

use rand_chacha::rand_core::{RngCore, SeedableRng};
use sequenced_broadcast::SequencedBroadcastSettings;
use tokio_util::sync::CancellationToken;

use crate::{recoverable_state::RecoverableState, testing::{state_tests::TestStateAction, test_sync_io::TestIOKillMode}, utils::PanicHelper, worker::sync_state::SyncState};

use super::{setup_logging, state_tests::TestState, test_sync_io::TestSyncNet};


#[tokio::test]
async fn fuzzy_test() {
    _fuzzy_test().await;
}

async fn _fuzzy_test() {
    setup_logging();

    let net = TestSyncNet::new();

    const INSTANT_COUNT: usize = 30;

    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();

    for i in 0..INSTANT_COUNT {
        let id = i as u64 + 1;
        let io = net.io(id).await;
        let sync = SyncState::new(io, id, 1, RecoverableState::new(100, TestState::default()), SequencedBroadcastSettings::default());
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(i as u64 + 100);

        let cancel = cancel.clone();
        let t = tokio::spawn(async move {
            let tx = sync.actions_tx();

            cancel.run_until_cancelled(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    tx.send(TestStateAction::Add { slot: rng.next_u64() as usize % 6, value: rng.next_u64() as i64 }).await.panic("failed to send");
                }
            }).await;

            tokio::time::sleep(Duration::from_secs(10)).await;

            sync
        });

        tasks.push(t);
    }

    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(3214);
    let mut blocks = Vec::new();
    for _ in 0..1024 {
        if rng.next_u64() % 4 == 0 {
            blocks.sort_by_key(|_| rng.next_u64());

            if let Some((a, b)) = blocks.pop() {
                net.block_connection(a, b, false).await;
                let mode = if rng.next_u64() % 2 == 0 {
                    TestIOKillMode::Shutdown
                } else {
                    TestIOKillMode::Timeout
                };
                net.kill_connection(a, b, mode).await;
            }

            continue;
        }

        let a = rng.next_u64() % INSTANT_COUNT as u64;
        let b = rng.next_u64() % INSTANT_COUNT as u64;

        if a == b {
            continue;
        }

        if rng.next_u64() % 4 == 0 {
            blocks.push((a, b));
            net.block_connection(a, b, true).await;
        }
        else if rng.next_u64() % 3 == 0 {
            net.kill_connection(a, b, TestIOKillMode::Shutdown).await;
        } else if rng.next_u64() % 2 == 0 {
            net.kill_connection(a, b, TestIOKillMode::Timeout).await;
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    cancel.cancel();

    let mut iter = tasks.into_iter();
    let expected = iter.next().unwrap().await.unwrap().shared().read().clone();

    for t in iter {
        let result = t.await.unwrap();
        let shared = result.shared();
        let state = shared.read();
        assert_eq!(state.state(), expected.state());
    }
}
