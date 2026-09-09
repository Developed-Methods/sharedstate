# protocol contract

The implementation pins Openraft 0.9.25 with its storage-v2 trait API.
Application-facing types hide the engine configuration and wire types.
SQLite uses WAL, FULL synchronization, and exclusive database access.
The upstream [storage contract](https://docs.rs/openraft/0.9.25/openraft/storage/index.html) defines the required durable acknowledgements.
SQLite documents the [synchronization modes](https://www.sqlite.org/pragma.html#pragma_synchronous) and [WAL behavior](https://www.sqlite.org/wal.html).

## Limits and timing

| Resource | Default or maximum |
|---|---|
| Voting membership at bootstrap | Exactly 3 or 5 distinct identities and addresses |
| Frame body | 1 MiB |
| Encoded command | 64 KiB; configurable downward |
| Encoded cached result | 64 KiB |
| Client admission | 128 operations; configurable downward |
| Sessions | 10,000, persisted and checked during handshake |
| SQLite request queue | 64 jobs per database |
| Accepted connection tasks | 256 per voter |
| Control RPC permits | 64 |
| Bulk RPC permits | 16 |
| Concurrent complete voter installations | 1 |
| Snapshot | 256 MiB |
| Snapshot chunk | 256 KiB on resumable transfers |
| Leased checkpoint archive | 512 MiB per source |
| Checkpoint lease | 60 seconds, renewed by manifest requests |
| Heartbeat | 500 ms |
| Election timeout | Randomized between 3 and 5 seconds |
| Ordinary RPC lifetime | At most 5 seconds and the caller deadline |
| Snapshot transfer attempt | 60 seconds, with 5-second chunk deadlines |
| Publication interval | 100 ms |

The session limit bounds identity count, not aggregate cached-result bytes.
Ten thousand maximum-sized results require approximately 625 MiB before metadata and allocator overhead.
Retired sessions retain tombstones and continue consuming capacity.
Applications must budget retained read handles separately.

## Identity and ownership

The handshake checks protocol magic, cluster UUID, source and target IDs, application schema, and session limit.
Each RPC owns one connection and one response.
A reader has one sequential session owner for source requests and state application.
Committed membership supplies voter routes after initial source discovery.
The current wire handshake does not authenticate peers or carry a process-incarnation token.
The bundled TCP transport is for trusted IPv4 networks.

## Persistence ordering

Votes and appended logs acknowledge only after durable database operations finish.
Snapshot chunks acknowledge their durable contiguous offsets.
Final snapshot installation verifies content and metadata before replacing application state.
Voter installation remains subject to Openraft's vote and log checks.
An older checkpoint cannot overwrite a newer durable snapshot manifest.
Unrelated checkpoint creation preserves an in-flight transfer's chunks.
Replacement voters receive a complete checkpoint containing imported state and session receipts.

## Ownership limits

Shutdown cancels ingress and awaits cooperative workers before closing storage.
A blocked application callback or physical disk operation can outlive a shutdown deadline.
An expired shutdown deadline reports incomplete cleanup.
Dropping a handle requests cancellation; it does not certify that cleanup completed.
