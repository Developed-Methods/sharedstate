use std::{sync::{atomic::{AtomicU64, Ordering}, Arc}, time::Duration};

use rand_chacha::rand_core::{RngCore, SeedableRng};
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

use crate::{state::DeterministicState, testing::{state_tests::TestStateAction, test_sync_io::TestIOKillMode}, utils::PanicHelper, worker::sync_manager::SyncManager};

use super::{setup_logging, state_tests::TestState, test_sync_io::TestSyncNet};


#[tokio::test]
async fn fuzzy_test() {
    _fuzzy_test().await;
}

async fn _fuzzy_test() {
    setup_logging();

    let net = TestSyncNet::new();

    const INSTANT_COUNT: usize = 32;
    const MESS_WITH_LEADER: bool = true;

    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();

    let leader_num = Arc::new(AtomicU64::new(1));

    for i in 0..INSTANT_COUNT {
        let id = i as u64 + 1;
        let io = net.io(id).await;

        let sync = {
            let _span = tracing::info_span!("Node", id).entered();
            SyncManager::new(io, id, TestState::default(), Default::default())
        };
        sync.set_leader(1).await;

        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(i as u64 + 100);

        let cancel = cancel.clone();
        let leader_num = leader_num.clone();

        let t = tokio::spawn(async move {
            let tx = sync.action_tx();

            cancel.run_until_cancelled(async {
                let mut leader = 1;

                loop {
                    let next = leader_num.load(Ordering::SeqCst);
                    if leader != next {
                        /* high chance we apply change first try */
                        if rng.next_u64() % 32 != 0 {
                            sync.set_leader(leader).await;
                            leader = next;
                        }
                        /* low chance we ignore change */
                        else if rng.next_u64() % 10 == 0 {
                            leader = next;
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(5)).await;
                    tx.send(TestStateAction::Add { slot: i % 32, value: 1 }).await.panic("failed to send");
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
        if MESS_WITH_LEADER && rng.next_u32() % 10 == 0 {
            leader_num.store(rng.next_u64() % INSTANT_COUNT as u64, Ordering::SeqCst);
        }

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

    tracing::info!("unblock all connections and let system recover");
    net.unblock_all().await;

    let mut sync_nodes = Vec::new();
    for task in tasks {
        let sync = task.await.unwrap();
        sync_nodes.push(sync);
    }

    for node in &sync_nodes {
        node.set_leader(1).await;
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        let mut sequences = vec![0; INSTANT_COUNT];

        loop {
            let mut has_change = false;

            for (pos, node) in sync_nodes.iter().enumerate() {
                let sequence = node.shared().read().state().accept_seq();
                if sequences[pos] != sequence {
                    has_change = true;
                    tracing::info!("node{} sequence updated: {} => {}", pos, sequences[pos], sequence);
                    sequences[pos] = sequence;
                }
            }

            if !has_change {
                break;
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }).await.panic("timeout waiting for seq to stablize");

    tracing::info!("leader sequence stable");
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    {
        let mut iter = sync_nodes.iter();
        let expected = iter.next().unwrap().shared().read().clone();
        assert_ne!(expected.state().numbers[0], 0);

        for node in iter {
            let shared = node.shared();
            let state = shared.read();
            assert_eq!(state.state(), expected.state());
        }
    }

    sync_nodes[INSTANT_COUNT - 1].action_tx().send(TestStateAction::Set { slot: 0, value: 0 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    {
        let mut iter = sync_nodes.iter();
        let expected = iter.next().unwrap().shared().read().clone();
        assert_eq!(expected.state().numbers[0], 0);

        for node in iter {
            let shared = node.shared();
            let state = shared.read();
            assert_eq!(state.state(), expected.state());
        }
    }
}
