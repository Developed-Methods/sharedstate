# Quorum uses the locally-known voter set — split-brain windows expose readers to writes that later vanish

**Severity:** Medium (behavior is within the stated "writes can be dropped" contract, but the read-side consequence needs handling/документation)
**Files:** `src/cluster/leader.rs:225-226` (majority vs. local voter set), `src/cluster/state_sync.rs:139-167` (deposed leader keeps applying), `src/state/recoverable_state.rs` (lineage detection)

## Summary

Majority is computed against each node's *locally known* voter set, which is gossiped
and only eventually consistent. Two nodes with different peer knowledge (cold start with
partial `initial_peers`, or a partition during membership propagation) can each hold a
legitimate majority of *their* known voters and lead concurrently. Separately, a deposed
leader keeps applying writes with local authority for up to `leader_poll_interval`
(default 100ms) after losing quorum, because the leader check in `lead()` is polled.

The generation/lineage scheme handles the *safety* side well: every takeover starts with
a `BumpGeneration` carrying a unique id, so forked histories always differ in
`(generation, id)` and `can_recover_follower` refuses to splice them — the losing side
resets from a fresh snapshot. The consequence is that **all writes accepted on the
losing side are silently discarded, including writes already served to local readers**.
A consumer that reads a value from the hot-read handle and treats it as durable will
observe state that later disappears without any signal. There is also no persisted
term/vote (Raft persists both precisely to prevent re-voting after restart); safety here
rests entirely on the lineage fork detection, with availability paid as full state
transfers.

## Failure scenario

1. Node A starts knowing only voter B; nodes C, D, E know all five voters.
2. A+B elect A (2/2 of A's known voters); C/D/E elect C (3/5). Two leaders write
   concurrently in different election roots.
3. Gossip merges peer knowledge; terms converge; A concedes and fresh-resets from C.
4. Every write A accepted — some already read and acted upon by A's local readers —
   vanishes. Nothing is logged at a level operators would alert on, and no counter
   tracks it.

## Recommended fix

1. **Make the loss observable.** When a node fresh-resets (`state_sync.rs` `SubscribeFresh`
   path) after having been leading or following a different lineage, emit a
   `tracing::warn!` with the number of discarded outer sequences
   (`old_details.next_seq() - fork_point`) and expose a counter/metric. Same for
   `drop_queued_actions`. Operators of a 20-DC deployment need to see write-drop rates.
2. **Shrink the dual-leader window.** Have `lead()` re-check `leader_state` *before*
   applying each action (it currently does — but only from the queue path; the check and
   the `update` are not atomic). Cheaper and more effective: reduce the damage by having
   the leader task proactively notify the sync task on mode change (e.g. a `watch`
   channel on `leader_state`) instead of 100ms polling.
3. **Document the read model.** State explicitly in crate docs that reads are
   local-lineage-consistent but not durable: a read may reflect writes that a later
   leader change discards. If some consumers need "read only quorum-durable state",
   that requires an ack protocol (leader confirms a majority of voters received seq N
   before exposing it) — a significant feature, worth an explicit non-goal note if you
   don't build it.
4. **Optional hardening:** persist (or at least best-effort persist) the current term
   and vote per node to reduce same-term double-voting after restarts. With the
   deterministic re-vote rule the practical risk is low, so this is secondary to 1–3.
