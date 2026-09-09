use std::{io, sync::Arc, time::Duration};

use openraft::{
    error::{
        Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError,
    },
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::*,
};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{
    Peer, TypeConfig,
    protocol::{self, Request, Response},
};
use crate::transport::traits::SyncIO;

pub(crate) struct Network<I> {
    pub io: Arc<I>,
    pub cluster: uuid::Uuid,
    pub local: u64,
    pub schema: u32,
    pub session_limit: u64,
    pub cancel: CancellationToken,
}

impl<I> Clone for Network<I> {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
            cluster: self.cluster,
            local: self.local,
            schema: self.schema,
            session_limit: self.session_limit,
            cancel: self.cancel.clone(),
        }
    }
}

pub(crate) struct Client<I> {
    network: Network<I>,
    target: u64,
    peer: Peer,
}

impl<I: SyncIO<Address = u64>> Network<I> {
    pub async fn request(&self, target: u64, peer: &Peer, request: Request, deadline: Instant) -> io::Result<Response> {
        let exchange = async {
            let mut connection = self.io.connect(&peer.address).await?;
            protocol::hello(&mut connection.write, self.cluster, self.local, target, self.schema, self.session_limit)
                .await?;
            let remote =
                protocol::verify(&mut connection.read, self.cluster, self.local, self.schema, self.session_limit)
                    .await?;
            if remote != target {
                return Err(io::Error::other("unexpected remote node identity"));
            }
            use tokio::io::AsyncWriteExt;
            connection.write.write_u8(request.lane()).await?;
            protocol::write(&mut connection.write, &request).await?;
            protocol::read(&mut connection.read).await
        };
        tokio::select! {
            _ = self.cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "node shutting down")),
            result = timeout_at(deadline, exchange) => result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RPC deadline exceeded"))?,
        }
    }
}

impl<I: SyncIO<Address = u64>> RaftNetworkFactory<TypeConfig> for Network<I> {
    type Network = Client<I>;
    async fn new_client(&mut self, target: u64, node: &Peer) -> Self::Network {
        Client {
            network: self.clone(),
            target,
            peer: node.clone(),
        }
    }
}

impl<I: SyncIO<Address = u64>> Client<I> {
    async fn request(&self, request: Request, option: RPCOption) -> io::Result<Response> {
        self.network
            .request(self.target, &self.peer, request, Instant::now() + option.hard_ttl().min(Duration::from_secs(5)))
            .await
    }
    fn remote<E: std::error::Error>(&self, error: E) -> RPCError<u64, Peer, E> {
        RPCError::RemoteError(RemoteError::new_with_node(self.target, self.peer.clone(), error))
    }
}

fn network_error<E: std::error::Error>(error: impl std::error::Error + 'static) -> RPCError<u64, Peer, E> {
    RPCError::Network(NetworkError::new(&error))
}

impl<I: SyncIO<Address = u64>> RaftNetwork<TypeConfig> for Client<I> {
    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<u64>,
        snapshot: openraft::Snapshot<TypeConfig>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        use sha2::{Digest, Sha256};
        let transfer = async {
            let bytes = snapshot.snapshot.into_inner();
            let request = Request::SnapshotBegin {
                meta: snapshot.meta.clone(),
                bytes: bytes.len() as u64,
            };
            let response = self
                .network
                .request(self.target, &self.peer, request, Instant::now() + Duration::from_secs(5))
                .await?;
            let Response::SnapshotOffset(mut offset) = response else {
                return Err(io::Error::other("snapshot resume rejected"));
            };
            if offset > bytes.len() as u64 {
                return Err(io::Error::other("invalid snapshot resume offset"));
            }
            while offset < bytes.len() as u64 {
                let end = bytes.len().min(offset as usize + 256 * 1024);
                let data = bytes[offset as usize..end].to_vec();
                let digest = Sha256::digest(&data).to_vec();
                let request = Request::SnapshotPart {
                    snapshot_id: snapshot.meta.snapshot_id.clone(),
                    offset,
                    data,
                    digest,
                };
                match self
                    .network
                    .request(self.target, &self.peer, request, Instant::now() + Duration::from_secs(5))
                    .await?
                {
                    Response::SnapshotOffset(next) if next == end as u64 => offset = next,
                    _ => return Err(io::Error::other("snapshot chunk rejected")),
                }
            }
            let request = Request::SnapshotEnd {
                vote,
                meta: snapshot.meta,
            };
            match self
                .network
                .request(self.target, &self.peer, request, Instant::now() + Duration::from_secs(5))
                .await?
            {
                Response::SnapshotInstalled(result) => Ok(result),
                _ => Err(io::Error::other("snapshot install rejected")),
            }
        };
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            result = timeout_at(Instant::now() + Duration::from_secs(60), transfer) => {
                let result = result.map_err(|_| StreamingError::Network(NetworkError::new(&io::Error::new(io::ErrorKind::TimedOut, "snapshot transfer deadline exceeded"))))?;
                result.map_err(|error| StreamingError::Network(NetworkError::new(&error)))?
                    .map_err(|error| StreamingError::RemoteError(RemoteError::new_with_node(self.target, self.peer.clone(), error)))
            }
        }
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Peer, RaftError<u64>>> {
        match self
            .request(Request::Append(rpc), option)
            .await
            .map_err(network_error)?
        {
            Response::Append(result) => result.map_err(|e| self.remote(e)),
            _ => Err(network_error(io::Error::other("unexpected append response"))),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Peer, RaftError<u64>>> {
        match self.request(Request::Vote(rpc), option).await.map_err(network_error)? {
            Response::Vote(result) => result.map_err(|e| self.remote(e)),
            _ => Err(network_error(io::Error::other("unexpected vote response"))),
        }
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, Peer, RaftError<u64, InstallSnapshotError>>> {
        match self
            .request(Request::Snapshot(rpc), option)
            .await
            .map_err(network_error)?
        {
            Response::Snapshot(result) => result.map_err(|e| self.remote(e)),
            _ => Err(network_error(io::Error::other("unexpected snapshot response"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        transport::{simulated::SimulatedNet, traits::SyncIOListener},
        v4::storage::Database,
    };
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn t13_voter_snapshot_resumes_after_receiver_restart_and_lost_chunk_response() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("receiver.db");
        let cluster = uuid::Uuid::new_v4();
        let net = SimulatedNet::new();
        let sender = net.start_io(1).await;
        let receiver = net.start_io(2).await;
        let data = vec![42; 300 * 1024];
        let expected = data.clone();
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: format!("{:x}", Sha256::digest(&data)),
        };
        let server = tokio::spawn(async move {
            let mut database = Database::open(&path, cluster, 2).await.unwrap();
            let mut offsets = Vec::new();
            let mut dropped = false;
            loop {
                let mut connection = receiver.next_client().await.unwrap();
                protocol::verify(&mut connection.read, cluster, 2, 1, 10).await.unwrap();
                protocol::hello(&mut connection.write, cluster, 2, 1, 1, 10)
                    .await
                    .unwrap();
                assert_eq!(connection.read.read_u8().await.unwrap(), 2);
                let response = match protocol::read(&mut connection.read).await.unwrap() {
                    Request::SnapshotBegin { meta, bytes } => {
                        Response::SnapshotOffset(database.resume_snapshot(meta, bytes).await.unwrap())
                    }
                    Request::SnapshotPart {
                        snapshot_id,
                        offset,
                        data,
                        digest,
                    } => {
                        assert_eq!(Sha256::digest(&data).as_slice(), digest.as_slice());
                        offsets.push(offset);
                        let next = database.receive_chunk(snapshot_id, offset, data).await.unwrap();
                        if !dropped {
                            dropped = true;
                            database.shutdown().await.unwrap();
                            database = Database::open(&path, cluster, 2).await.unwrap();
                            continue;
                        }
                        Response::SnapshotOffset(next)
                    }
                    Request::SnapshotEnd { meta, vote } => {
                        assert_eq!(database.assembled_snapshot(meta.snapshot_id).await.unwrap(), expected);
                        protocol::write(
                            &mut connection.write,
                            &Response::SnapshotInstalled(Ok(SnapshotResponse { vote })),
                        )
                        .await
                        .unwrap();
                        break;
                    }
                    _ => panic!("unexpected request"),
                };
                protocol::write(&mut connection.write, &response).await.unwrap();
            }
            database.shutdown().await.unwrap();
            assert_eq!(offsets, vec![0, 256 * 1024]);
        });
        let mut network = Network {
            io: sender,
            cluster,
            local: 1,
            schema: 1,
            session_limit: 10,
            cancel: CancellationToken::new(),
        };
        let mut client = network.new_client(2, &Peer { address: 2 }).await;
        let snapshot = || openraft::Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(std::io::Cursor::new(data.clone())),
        };
        let option = || RPCOption::new(Duration::from_secs(5));
        let vote = openraft::Vote::new_committed(1, 1);
        assert!(
            client
                .full_snapshot(vote, snapshot(), std::future::pending(), option())
                .await
                .is_err()
        );
        client
            .full_snapshot(vote, snapshot(), std::future::pending(), option())
            .await
            .unwrap();
        server.await.unwrap();
    }
}
