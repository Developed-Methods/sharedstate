use crate::transport::traits::{SyncConnection, SyncIO, SyncIOListener};
use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
};
use tokio::net::{
    TcpListener, TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};

/// IPv4 transport for trusted networks. Deployments supply network authentication externally.
pub struct TcpTransport {
    listener: TcpListener,
}

impl TcpTransport {
    pub async fn bind(address: SocketAddrV4) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(address).await?,
        })
    }
    pub fn address(&self) -> io::Result<u64> {
        match self.listener.local_addr()? {
            std::net::SocketAddr::V4(address) => Ok(Self::encode(address)),
            _ => Err(io::Error::other("IPv4 required")),
        }
    }
    pub fn encode(address: SocketAddrV4) -> u64 {
        (u64::from(u32::from(*address.ip())) << 16) | u64::from(address.port())
    }
    fn decode(value: u64) -> io::Result<SocketAddrV4> {
        if value >> 48 != 0 {
            return Err(io::Error::other("invalid IPv4 address"));
        }
        Ok(SocketAddrV4::new(Ipv4Addr::from((value >> 16) as u32), value as u16))
    }
    fn connection(stream: TcpStream, remote: u64) -> io::Result<SyncConnection<Self>> {
        stream.set_nodelay(true)?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection { remote, read, write })
    }
}
impl SyncIO for TcpTransport {
    type Address = u64;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;
    async fn connect(&self, remote: &u64) -> io::Result<SyncConnection<Self>> {
        Self::connection(TcpStream::connect(Self::decode(*remote)?).await?, *remote)
    }
}
impl SyncIOListener for TcpTransport {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (stream, remote) = self.listener.accept().await?;
        let std::net::SocketAddr::V4(remote) = remote else {
            return Err(io::Error::other("IPv4 required"));
        };
        Self::connection(stream, Self::encode(remote))
    }
}
