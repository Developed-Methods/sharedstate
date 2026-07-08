# `Drop for SharedState` leaks child tasks — a dropped node becomes a zombie that can stall peers

**Severity:** Medium
**Files:** `src/service.rs:183-189` (`Drop`), `src/cluster/peer_connections.rs:275` (worker spawn), `src/transport/traits.rs:80-98` (channel task spawns), `src/cluster/rpc_server.rs:100` (per-client spawn), `src/cluster/rpc_server.rs:182` (`stream_feed`)

## Summary

`Drop` aborts only the four top-level task handles (listener, discovery, leader, sync).
Everything they spawned keeps running: `ConnectionWorker`s, `ReadChannel`/`WriteChannel`
tasks, accepted RPC-client tasks, and `stream_feed` tasks. These hold `Arc<NodeState>`,
so the node's state, broadcast, and peer map stay alive, and `WriteChannel` keeps
sending keepalives every ~2s.

The result is a zombie: peers still see healthy, keepalive-fed connections. A follower
subscribed to the dropped node's feed receives keepalives but no actions — its
subscription never closes, and (per issue 01) there is no progress timeout to escape,
so it can stall indefinitely on a node that no longer exists logically. The dropped
node's connection workers also keep dialing peers and answering `LeaderQuery` with
frozen leader state, polluting relay selection.

Process exit is unaffected (the TCP failover test kills a whole runtime, which is why
this never shows up there); the problem is in-process restart or drop — e.g. an
application that rebuilds its `SharedState` on config change.

## Failure scenario

1. App drops a `SharedState` (node X) to rebuild it with new settings.
2. X's `stream_feed` tasks keep streaming-nothing to followers; keepalives flow.
3. Followers of X never see the stream close, never resubscribe elsewhere; their reads
   go permanently stale.
4. The replacement node binds a new listener while zombie tasks still answer on old
   connections, giving peers contradictory views of "X".

## Recommended fix

Thread a single `CancellationToken` through the node:

1. Store a root `CancellationToken` in `NodeState` (or in `SharedState`, passed into
   every constructor). `Drop` calls `cancel()` in addition to aborting top-level tasks.
2. Every spawned loop (`ConnectionWorker::run`, `ReadChannel::start`,
   `WriteChannel::start`, `RpcServer::handle_client`, `stream_feed`) selects on
   `token.cancelled()` and exits, sending the close frame where a write half is held
   (`WriteChannel` already has this path for input-closed).
3. On cancellation, `stream_feed`/`handle_client` should drop the connection rather
   than idle, so remote followers observe a real close and resubscribe.
4. Optionally add an explicit `async fn shutdown(self)` that cancels and awaits task
   completion for clean teardown, keeping `Drop` as the best-effort fallback.
