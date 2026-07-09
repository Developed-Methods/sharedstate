use std::{io, net::SocketAddr, sync::Arc};

use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};

use crate::transport::traits::{SyncConnection, SyncIO, SyncIOListener};

#[derive(Clone)]
pub struct TcpIo {
    address: SocketAddr,
    listener: Arc<TcpListener>,
}

impl TcpIo {
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        Self::from_listener(TcpListener::bind(address).await?)
    }

    pub fn from_listener(listener: TcpListener) -> io::Result<Self> {
        let address = listener.local_addr()?;
        Ok(Self {
            address,
            listener: Arc::new(listener),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

impl SyncIO for TcpIo {
    type Address = SocketAddr;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        let stream = TcpStream::connect(remote).await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }
}

impl SyncIOListener for TcpIo {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (stream, remote) = self.listener.accept().await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection { remote, read, write })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn localhost_ephemeral() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    #[tokio::test]
    async fn bind_updates_ephemeral_port() {
        let io = TcpIo::bind(localhost_ephemeral()).await.unwrap();

        assert_ne!(io.address().port(), 0);
        assert_eq!(io.address(), io.listener.local_addr().unwrap());
    }

    #[tokio::test]
    async fn connect_and_accept_return_socket_addresses() {
        let server = TcpIo::bind(localhost_ephemeral()).await.unwrap();
        let client = TcpIo::bind(localhost_ephemeral()).await.unwrap();

        let accept = tokio::spawn({
            let server = server.clone();
            async move { server.next_client().await }
        });

        let mut client_conn = client.connect(&server.address()).await.unwrap();
        let mut server_conn = accept.await.unwrap().unwrap();

        assert_eq!(client_conn.remote, server.address());
        assert_eq!(server_conn.remote, client_conn.write.local_addr().unwrap());

        client_conn.write.write_all(b"ping").await.unwrap();

        let mut received = [0; 4];
        server_conn.read.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"ping");
    }
}
