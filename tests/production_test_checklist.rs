//! Production-critical test coverage checklist.
//!
//! This file intentionally contains no tests. Each section below maps to a
//! concrete test file or module owned by a worker agent so the work can happen
//! in parallel without edit conflicts.

// 1. Network partitions and reconnection:
//    Use SimulatedNet blocked edges/nodes/blackholes to verify followers mark
//    themselves disconnected, retry, resubscribe, and converge after healing.

// 2. Fresh-state recovery when history is unavailable:
//    Force an unrecoverable follower generation/history gap and verify the
//    leader sends FreshState and the follower resets to the leader state.

// 3. Out-of-sequence leader stream handling:
//    Feed a follower an unexpected sequence number, verify it retries, and
//    verify no duplicate or skipped actions survive after recovery.

// 4. Protocol handshake rejection:
//    Check wrong protocol versions, missing protocol messages, and unexpected
//    initial requests are rejected with NotConnected or closed cleanly.

// 5. Concurrent follower submissions:
//    Submit many actions concurrently through followers and the leader, then
//    assert all nodes converge and the sequence advances exactly once per
//    accepted action.

// 6. Leadership churn under load:
//    Continuously submit actions while leader_address changes repeatedly, then
//    verify eventual convergence and no stalled leadership state.

// 7. Recoverable history boundary:
//    Test the 2048 generation-history retention boundary: recoverable at the
//    oldest retained generation, unrecoverable just beyond it.

// 8. Framing adversarial input:
//    Cover partial headers, partial payload timeout, invalid message bodies,
//    and oversized declared frame lengths.

// 9. Drop/shutdown behavior:
//    Verify dropping SharedState aborts tasks and closes listeners/connections
//    sufficiently that subsequent traffic fails rather than leaking work.

// 10. Encoding compatibility snapshots:
//     Golden-byte tests for SyncRequest, SyncResponse, and
//     RecoverableStateDetails so wire-format changes are intentional.
