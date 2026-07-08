# Forwarded actions can loop forever between relaying followers

**Severity:** High
**Files:** `src/cluster/state_sync.rs:384` (`forward_action`), `src/cluster/state_sync.rs:191` (relay guard), `src/cluster/rpc_server.rs:42-48` (`SyncRequest::Action` handling)

## Summary

A follower forwards queued client actions to its *sync target* (leader or relay), and the
RPC server pushes any received `Action` back into the local sync task's queue. The only
guard on relay selection is "the candidate follows the same leader" (`query_leader`).
If followers A and B both lose their direct path to leader L — while both remain
`Following { leader: L }` because other peers vouch L reachable, so no election
triggers — A can pick B as its relay while B picks A. Every action then ping-pongs
A→B→A→B indefinitely: unbounded network amplification with no hop counter and no
origin/dedup check.

The same cycle also starves both nodes of state updates: each subscribes to the other's
broadcast feed, which never advances. Transport keepalives keep the connection healthy,
so the idle subscription never closes, and there is no progress timeout to escape it.
Even when a working relay (a peer C that *can* reach L) exists, whichever candidate
sorts first in `relay_candidates` wins and is never re-evaluated while the stream stays
open.

## Failure scenario

1. Cluster of 5 voters; A and B lose outbound connectivity to leader L only.
2. Both remain followers of L (reachability vouching prevents an election).
3. A's `follow()` fails to reach L directly, queries B: B follows L → A relay-syncs via B.
4. B does the same via A.
5. Any action submitted at A is forwarded to B, re-queued at B, forwarded back to A, forever.
6. Neither A nor B receives any state updates until L actually changes; reads at both go
   stale indefinitely with no error surfaced.

## Recommended fix

Three complementary changes:

1. **Hop limit on forwarded actions.** Add a `ttl: u8` (e.g. starting at 4) to
   `SyncRequest::Action`. Each node decrements before re-forwarding; drop (and log/count)
   at zero. This bounds amplification even for cycles longer than two nodes. Bump
   `PROTOCOL_VERSION` for the wire change.
2. **Don't relay through a peer that is itself relaying.** Extend the `query_leader`
   RPC response (or add a field to `LeaderState`) with "syncing directly from leader:
   yes/no", and only accept direct-synced peers as relays. This breaks the A↔B cycle at
   selection time and always prefers a candidate like C with a real path to L.
3. **Progress timeout on subscriptions.** In `stream()`, track the time since the last
   `AuthorityAction`. If nothing arrives for a generous window (e.g.
   `N × leader_poll_interval`, configurable) while the node still believes a leader is
   active, return `SyncAttempt::Finished { applied_actions: false }` so `follow()`
   re-evaluates sync sources. A healthy but idle leader still sends actions rarely, so
   pair this with re-running relay selection rather than treating it as an error.
