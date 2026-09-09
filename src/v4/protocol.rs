use std::io;

use openraft::{
    error::{ClientWriteError, InstallSnapshotError, RaftError},
    raft::*,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Command, Peer, TypeConfig};

pub(crate) const MAX_FRAME: usize = 1024 * 1024;
const MAGIC: &[u8; 8] = b"SHSTATE2";

#[derive(Serialize, Deserialize)]
pub(crate) enum Request {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Checkpoint,
    Snapshot(InstallSnapshotRequest<TypeConfig>),
    SnapshotBegin {
        meta: openraft::SnapshotMeta<u64, Peer>,
        bytes: u64,
    },
    SnapshotPart {
        snapshot_id: String,
        offset: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        digest: Vec<u8>,
    },
    SnapshotEnd {
        vote: openraft::Vote<u64>,
        meta: openraft::SnapshotMeta<u64, Peer>,
    },
    Write(Command),
    Feed {
        after: Option<openraft::LogId<u64>>,
    },
    Manifest {
        preferred: Option<String>,
    },
    Chunk {
        snapshot_id: String,
        offset: u64,
    },
}

impl Request {
    pub fn lane(&self) -> u8 {
        match self {
            Self::Append(_) | Self::Vote(_) | Self::Checkpoint => 0,
            Self::Write(_) => 1,
            Self::SnapshotBegin { .. }
            | Self::SnapshotPart { .. }
            | Self::SnapshotEnd { .. }
            | Self::Snapshot(_)
            | Self::Feed { .. }
            | Self::Manifest { .. }
            | Self::Chunk { .. } => 2,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Response {
    Append(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),
    Snapshot(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),
    Write(Result<ClientWriteResponse<TypeConfig>, RaftError<u64, ClientWriteError<u64, Peer>>>),
    SnapshotOffset(u64),
    SnapshotInstalled(Result<SnapshotResponse<u64>, openraft::error::Fatal<u64>>),
    Rejected(String),
    Feed {
        entries: Vec<super::Entry>,
        applied: Option<openraft::LogId<u64>>,
        leader: Option<u64>,
        checkpoint: Option<openraft::SnapshotMeta<u64, Peer>>,
    },
    SnapshotRequired,
    Manifest {
        meta: openraft::SnapshotMeta<u64, Peer>,
        bytes: u64,
    },
    Chunk {
        snapshot_id: String,
        offset: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        digest: Vec<u8>,
    },
}

pub(crate) async fn hello<W: AsyncWrite + Unpin>(
    out: &mut W,
    cluster: uuid::Uuid,
    source: u64,
    target: u64,
    schema: u32,
    session_limit: u64,
) -> io::Result<()> {
    out.write_all(MAGIC).await?;
    out.write_all(cluster.as_bytes()).await?;
    out.write_u64(source).await?;
    out.write_u64(target).await?;
    out.write_u32(schema).await?;
    out.write_u64(session_limit).await?;
    out.flush().await
}

pub(crate) async fn verify<R: AsyncRead + Unpin>(
    input: &mut R,
    cluster: uuid::Uuid,
    target: u64,
    schema: u32,
    session_limit: u64,
) -> io::Result<u64> {
    let mut magic = [0; 8];
    input.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err(io::Error::other("incompatible wire protocol"));
    }
    let mut origin = [0; 16];
    input.read_exact(&mut origin).await?;
    let source = input.read_u64().await?;
    let requested_target = input.read_u64().await?;
    let requested_schema = input.read_u32().await?;
    let requested_limit = input.read_u64().await?;
    if origin != *cluster.as_bytes()
        || requested_target != target
        || requested_schema != schema
        || requested_limit != session_limit
    {
        return Err(io::Error::other("cluster, node, or schema mismatch"));
    }
    Ok(source)
}

pub(crate) fn encode<T: Serialize>(value: &T, limit: usize) -> io::Result<Vec<u8>> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::other("encoding limit exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}

pub(crate) fn canonical<T: Serialize>(value: &T, limit: usize) -> io::Result<Vec<u8>> {
    let bytes = encode(value, limit)?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    value.sort_all_objects();
    encode(&value, limit)
}

pub(crate) fn canonical_value<T: Serialize>(value: &T) -> io::Result<serde_json::Value> {
    let mut value = serde_json::to_value(value)?;
    value.sort_all_objects();
    Ok(value)
}

pub(crate) async fn write<W: AsyncWrite + Unpin, T: Serialize>(out: &mut W, value: &T) -> io::Result<()> {
    let bytes = encode(value, MAX_FRAME)?;
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err(io::Error::other("frame limit exceeded"));
    }
    out.write_u32(bytes.len() as u32).await?;
    out.write_all(&bytes).await?;
    out.flush().await
}

pub(crate) async fn read<R: AsyncRead + Unpin, T: DeserializeOwned>(input: &mut R) -> io::Result<T> {
    let size = input.read_u32().await? as usize;
    if size == 0 || size > MAX_FRAME {
        return Err(io::Error::other("frame limit exceeded"));
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes).await?;
    // JSON decoding bounds collections by actual input bytes and rejects trailing data.
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn t18_invalid_lengths_trailing_data_and_nested_values_are_rejected() {
        for size in [0u32, MAX_FRAME as u32 + 1, u32::MAX] {
            let bytes = size.to_be_bytes();
            assert!(read::<_, Request>(&mut &bytes[..]).await.is_err());
        }
        let payload = b"\"Manifest\" false";
        let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
        bytes.extend(payload);
        assert!(read::<_, Request>(&mut &bytes[..]).await.is_err());
        let nested = format!("{}0{}", "[".repeat(256), "]".repeat(256));
        let mut bytes = (nested.len() as u32).to_be_bytes().to_vec();
        bytes.extend(nested.bytes());
        assert!(read::<_, serde_json::Value>(&mut &bytes[..]).await.is_err());
        assert!(encode(&vec![0u8; 100], 10).is_err());
    }
    #[tokio::test]
    async fn v2_handshake_rejects_wire_cluster_schema_identity_and_contract_mismatches() {
        let cluster = uuid::Uuid::new_v4();
        for (remote_cluster, target, schema, sessions) in [
            (uuid::Uuid::new_v4(), 2, 1, 10),
            (cluster, 3, 1, 10),
            (cluster, 2, 9, 10),
            (cluster, 2, 1, 11),
        ] {
            let mut bytes = Vec::new();
            hello(&mut bytes, remote_cluster, 1, target, schema, sessions)
                .await
                .unwrap();
            assert!(verify(&mut &bytes[..], cluster, 2, 1, 10).await.is_err());
        }
        assert!(verify(&mut &b"SHSTATE1"[..], cluster, 2, 1, 10).await.is_err());
        let mut bytes = Vec::new();
        hello(&mut bytes, cluster, 1, 2, 1, 10).await.unwrap();
        assert_eq!(verify(&mut &bytes[..], cluster, 2, 1, 10).await.unwrap(), 1);
    }
}
