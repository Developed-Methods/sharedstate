# `submit_action` blocks indefinitely when the queue is full and no leader emerges

**Severity:** Low (API ergonomics / backpressure visibility)
**Files:** `src/service.rs:173-180` (`submit_action`), `src/service.rs:63` (`ACTION_QUEUE_CAPACITY = 512`), `src/cluster/state_sync.rs:120-125` (`wait_for_leader` leaves actions queued)

## Summary

`submit_action` awaits `mpsc::Sender::send`. While the cluster has no leader
(election in progress, or the wedge states in issues 01/07), the sync task deliberately
leaves actions in the channel. Once 512 actions accumulate, every subsequent
`submit_action` call parks until a leader emerges — potentially forever. Callers cannot
distinguish "queued, will be forwarded eventually" from "stuck behind a leaderless
cluster", and there is no timeout, no queue-depth signal, and no drop notification
(`drop_queued_actions` at `state_sync.rs:380-382` discards silently on leader change).

Given the project contract is explicitly "writes can be slow and dropped", the current
behavior is defensible — but the contract should be visible at the API surface rather
than manifesting as an indefinite await.

## Recommended fix

1. Add a non-blocking variant:

   ```rust
   pub fn try_submit_action(&self, action: D::Action)
       -> Result<(), TrySendError<(I::Address, D::Action)>>
   ```

   so callers that prefer drop-over-block get it explicitly (`TrySendError::Full` is the
   "cluster is backed up / leaderless" signal).
2. Document on `submit_action` that it applies backpressure and may wait unboundedly
   during elections; suggest wrapping in `tokio::time::timeout` for latency-sensitive
   callers.
3. Emit a counter/`tracing::warn!` (rate-limited) from `drop_queued_actions` with the
   number of discarded actions, and consider a gauge for current queue depth, so
   operators can observe write loss and backlog instead of inferring it.
