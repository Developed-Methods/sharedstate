# No persistence — simultaneous restart of the voters loses all state

**Severity:** Low if intentional (needs documentation); Medium if durability across full outages is expected
**Files:** `src/service.rs:79-147` (`start` / `start_recoverable`), crate docs (`src/lib.rs`)

## Summary

State lives only in memory: there is no snapshotting, no write-ahead log, and no
persisted election term/vote. Individual node or DC failures are handled — survivors
hold the state and rejoining nodes recover via subscription or fresh transfer. But if
all voters restart near-simultaneously (region-wide power/cloud incident, coordinated
deploy gone wrong), the cluster elects a leader over empty initial state and the entire
dataset is gone, silently.

`start_recoverable` exists and lets the application supply a previously saved
`RecoverableState<D>` (its details survive intact — covered by
`start_recoverable_preserves_initial_recovery_details`), so the building block for
application-managed durability is present; there is just no path that produces such a
snapshot from a running node, and nothing in the docs states the durability model.

## Failure scenario

1. All 5 voter DCs restart within one election window (coordinated deploy / regional outage).
2. Every node starts with `initial_state: D::default()` and a fresh lineage id.
3. A leader is elected over empty state; observers fresh-reset to it.
4. Months of accumulated state vanish with no error — the cluster looks healthy.

## Recommended fix

1. **Expose a snapshot API.** Add something like
   `SharedState::snapshot(&self) -> RecoverableState<D>` (clone via the existing
   `StateHandle::read_with`; `RecoverableState` already implements `MessageEncoding`, so
   callers can serialize it). Applications can then checkpoint periodically and boot
   with `start_recoverable`.
2. **Optionally, built-in periodic checkpointing** behind a setting
   (`snapshot_path + interval`), writing atomically (temp file + rename) and loading on
   start when present. Election preference already favors the candidate that can recover
   the most peers, so a node restored from a recent checkpoint naturally wins leadership
   after a full outage.
3. **Document the durability model** in `lib.rs` either way: state survives any minority
   (or majority-with-one-survivor) failure but not a simultaneous restart of all nodes,
   unless the application persists snapshots.
