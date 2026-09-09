# architecture decisions

## Commitment and reads

Successful writes require majority commitment and leader application.
Local reads may be stale and include their committed revision.
Applications implement deterministic commands without external side effects.
Immutable `Arc` snapshots let retained readers coexist with subsequent publication.

## Membership and bootstrap

Openraft owns membership and elections.
Bootstrap is explicit and requires empty storage with a declared initial voter set.
Seeds provide routes only. Unreachable seeds never authorize creating a cluster.
Persist cluster and node identities independently from transport addresses.
Add replacement voters as learners before promotion.

## Persistence and snapshots

Use SQLite with verified WAL mode and FULL synchronization on local storage.
Serialize durable I/O outside asynchronous runtime workers.
Persist votes, logs, committed positions, and complete snapshot metadata.
A snapshot includes application state, applied position, membership, and client sessions.
Publish installed state only after verification and durable installation.

## Client retries

Clients persist their identity and next sequence before submission.
Each session permits one outstanding operation, beginning at sequence one.
Retry uncertain operations with the same sequence and identical command.
Cache the latest response and reject sequence gaps, older requests, and changed payloads.
Never silently evict a session. Retirement permanently closes its retry contract.
Cancelling a caller does not revoke a committed operation.

## Release envelope

Target three or five durable voters, 20–50 readers, and approximately 60 MB of state.
The benchmark uses 60 MiB as a conservative approximation.
The production write rate is unspecified; throughput measurements do not establish an admission target.
Measure command rate, clone cost, memory, RTT, disk latency, and transfer bandwidth.
