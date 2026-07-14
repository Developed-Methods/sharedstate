# Wall-clock dependence: cross-machine timestamp comparison and non-monotonic time

**Severity:** Low
**Files:** `src/utils.rs:15-20` (`now_ms`), `src/cluster/node_state.rs:132-138` (`merge_last_activity`), `src/cluster/peer_discovery.rs:193-206` (retry pacing), `src/utils.rs:8-13` (`unique_state_id`), `src/protocol/messages.rs:62-66` (term nonce)

## Summary

`now_ms()` (wall clock, `SystemTime`) feeds several mechanisms:

- **`merge_last_activity` compares timestamps produced on different machines** (a peer's
  self-reported `last_global_activity` vs. locally-stamped values). Clock skew between
  DCs directly shifts which retry tier `failed_connect_retry_delay` picks — a peer with
  a fast clock looks "recently active" longer; a slow clock relegates a live peer to the
  30-minute retry bucket. If issue 02's expiry recommendation is adopted, this same
  skew feeds quorum-relevant decisions, raising the stakes.
- **Backwards clock jumps** (NTP step, VM migration) affect retry pacing
  (`now.saturating_sub(epoch_ms)` handles underflow but a backwards jump makes recent
  failures look ancient/fresh), `unique_state_id` uniqueness, and term nonces (issue 09).
- `now_ms()` will also `unwrap`-panic if the system clock is before the epoch — absurd,
  but a one-line `unwrap_or(0)` removes the crash path.

None of this breaks safety today (lineage ids and nonces have low collision odds), but
for a 20-DC deployment clock skew is a fact of life, and time currently leaks into
liveness heuristics silently.

## Recommended fix

1. Use a monotonic clock (`tokio::time::Instant`) for all *local* durations: connect
   retry pacing, `ConnectStatus::{Connected,FailedToConnect}` timestamps. Only gossiped
   values need wall time.
2. For gossiped activity timestamps, prefer stamping *receipt* time locally (the
   receiving node records "I heard about activity for P at my-now") rather than trusting
   the origin's clock in comparisons; `merge_last_activity` then compares values from a
   single clock domain per node. Where origin timestamps must be kept, clamp incoming
   values to `min(incoming, local_now + slack)` to bound fast-clock inflation.
3. Replace the time component of `unique_state_id` with (or add) a random `u64`
   (`std::random` / a lazily-seeded RNG) so id uniqueness doesn't depend on clock
   behavior at all.
4. Change `now_ms()` to `unwrap_or(0)` (or `expect` with a clear message) to avoid the
   pre-epoch panic.
