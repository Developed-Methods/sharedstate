CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_log (
    position BLOB PRIMARY KEY CHECK(length(position) = 8),
    entry BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot_chunks (
    position INTEGER PRIMARY KEY,
    data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot_archive (
    id TEXT PRIMARY KEY,
    meta BLOB NOT NULL,
    data BLOB NOT NULL,
    expires INTEGER NOT NULL
);
