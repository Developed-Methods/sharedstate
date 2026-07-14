# `progress_recover` is dead code; full generation history is gossiped to every peer every 3s

**Severity:** Low (correctness) / Medium (overhead at scale)
**Files:** `src/state/recoverable_state.rs:93-103` (`progress_recover`, unused), `src/state/recoverable_state.rs:79-81` (2048 cap), `src/cluster/peer_discovery.rs:128` (history in every `LeaderInfo`), `src/cluster/state_sync.rs:292` (history in `SubscribeRecovery`)

## Summary

`progress_recover` — clearly intended to prune `history` once all followers have
advanced past old generations — has no callers. The only pruning is the hard cap of
2048 entries in `apply_generation_bump`. Every leadership change appends an entry
(24 bytes encoded), so on a long-lived cluster `history` trends toward the cap and
stays there.

The full `RecoverableStateDetails`, history included, is serialized into every
`LeaderInfo` gossip message (sent to every peer each `observation_interval`, default 3s)
and into every `SubscribeRecovery` handshake. At the cap that is ~48 KB per message;
with 20 nodes gossiping to 19 peers every 3s, roughly 6 MB/s of cluster-wide overhead
carrying data whose only use is the `can_recover_follower` scoring and recovery check.

## Recommended fix

1. **Delete `progress_recover`** (it's unreferenced), or wire it up if pruning is still
   intended — but note the leader cannot easily know all followers' positions today, so
   deletion plus (2) is the pragmatic path.
2. **Stop gossiping full history.** `can_recover_follower` walks history from the
   candidate's (leader-side) copy; the *follower/peer* side of the comparison only needs
   `(id, generation, inner_state_next_seq, outer_state_next_seq)`. Introduce a compact
   `RecoverableStateSummary` with just those four fields for `LeaderInfo` and election
   scoring; keep the full details only for the `SubscribeRecovery` request, where the
   server needs its own local history anyway (the request also only needs the summary —
   the server checks against its own history). This shrinks gossip to a fixed ~34 bytes
   and removes the incentive to keep the cap low.
3. If full history must remain on the wire anywhere, lower the cap (2048 generations of
   recovery depth is far beyond useful) and note that exceeding it silently converts
   old followers to fresh state transfers — which is the accepted fallback anyway.
