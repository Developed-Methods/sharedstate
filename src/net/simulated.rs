use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc as StdArc, Mutex as StdMutex},
    task::{Context, Poll},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{duplex, split, AsyncRead, AsyncWrite, DuplexStream, ReadBuf, ReadHalf, WriteHalf},
    sync::{mpsc, oneshot, Mutex},
};

use crate::net::sync_io::{SyncConnection, SyncIO, SyncIOListener};

#[derive(Clone)]
pub struct SimulatedNet {
    inner: StdArc<Mutex<SimulatedNetInner>>,
}

struct SimulatedNetInner {
    listeners: HashMap<u64, mpsc::Sender<SimulatedIncoming>>,
    active_connections: HashMap<u64, Vec<KillHandle>>,
    active_connection_edges: HashMap<(u64, u64), Vec<KillHandle>>,
    blocked_nodes: HashSet<u64>,
    blocked_edges: HashSet<(u64, u64)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimulatedTopologySnapshot {
    pub online: BTreeSet<u64>,
    pub blocked_nodes: BTreeSet<u64>,
    pub blocked_edges: BTreeSet<(u64, u64)>,
}

impl SimulatedNet {
    pub fn new() -> Self {
        Self {
            inner: StdArc::new(Mutex::new(SimulatedNetInner {
                listeners: HashMap::new(),
                active_connections: HashMap::new(),
                active_connection_edges: HashMap::new(),
                blocked_nodes: HashSet::new(),
                blocked_edges: HashSet::new(),
            })),
        }
    }

    pub async fn start_io(&self, address: u64) -> StdArc<SimulatedIo> {
        let (tx, rx) = mpsc::channel(128);
        self.inner.lock().await.listeners.insert(address, tx);
        StdArc::new(SimulatedIo {
            address,
            net: self.clone(),
            incoming: StdArc::new(Mutex::new(rx)),
        })
    }

    pub async fn stop_node(&self, address: u64) {
        let handles = {
            let mut inner = self.inner.lock().await;
            inner.listeners.remove(&address);
            inner.active_connections.remove(&address).unwrap_or_default()
        };

        for handle in handles {
            handle.kill();
        }
    }

    pub async fn set_edge_blocked(&self, a: u64, b: u64, blocked: bool) {
        let edge = Self::edge_key(a, b);
        let handles = {
            let mut inner = self.inner.lock().await;
            if blocked {
                inner.blocked_edges.insert(edge);
                inner.active_connection_edges.remove(&edge).unwrap_or_default()
            } else {
                inner.blocked_edges.remove(&edge);
                Vec::new()
            }
        };

        for handle in handles {
            handle.kill();
        }
    }

    pub async fn clear_edge_blocks(&self) {
        self.inner.lock().await.blocked_edges.clear();
    }

    pub async fn set_node_blocked(&self, address: u64, blocked: bool) {
        let handles = {
            let mut inner = self.inner.lock().await;
            if blocked {
                inner.blocked_nodes.insert(address);
                inner.active_connections.remove(&address).unwrap_or_default()
            } else {
                inner.blocked_nodes.remove(&address);
                Vec::new()
            }
        };

        for handle in handles {
            handle.kill();
        }
    }

    pub async fn clear_node_blocks(&self) {
        self.inner.lock().await.blocked_nodes.clear();
    }

    pub async fn topology_snapshot(&self) -> SimulatedTopologySnapshot {
        let inner = self.inner.lock().await;
        SimulatedTopologySnapshot {
            online: inner.listeners.keys().copied().collect(),
            blocked_nodes: inner.blocked_nodes.iter().copied().collect(),
            blocked_edges: inner.blocked_edges.iter().copied().collect(),
        }
    }

    pub fn edge_key(a: u64, b: u64) -> (u64, u64) {
        if a < b {
            (a, b)
        } else {
            (b, a)
        }
    }
}

impl Default for SimulatedNet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct SimulatedIo {
    address: u64,
    net: SimulatedNet,
    incoming: StdArc<Mutex<mpsc::Receiver<SimulatedIncoming>>>,
}

struct SimulatedIncoming {
    remote: u64,
    read: KillableIo<ReadHalf<DuplexStream>>,
    write: KillableIo<WriteHalf<DuplexStream>>,
}

impl SyncIO for SimulatedIo {
    type Address = u64;
    type Read = KillableIo<ReadHalf<DuplexStream>>;
    type Write = KillableIo<WriteHalf<DuplexStream>>;

    async fn connect(&self, remote: &Self::Address) -> std::io::Result<SyncConnection<Self>> {
        let (tx, handles) = {
            let mut net = self.net.inner.lock().await;
            if net.blocked_nodes.contains(&self.address)
                || net.blocked_nodes.contains(remote)
                || net
                    .blocked_edges
                    .contains(&SimulatedNet::edge_key(self.address, *remote))
            {
                return Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "connection blocked"));
            }
            let tx = net.listeners.get(remote).cloned();
            let handles = [
                KillHandle::new(),
                KillHandle::new(),
                KillHandle::new(),
                KillHandle::new(),
            ];
            let active = net.active_connections.entry(self.address).or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            let active = net.active_connections.entry(*remote).or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            let active = net
                .active_connection_edges
                .entry(SimulatedNet::edge_key(self.address, *remote))
                .or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            (tx, handles)
        };

        let Some(tx) = tx else {
            return Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "remote offline"));
        };

        let (client, server) = duplex(64 * 1024);
        let (client_read, client_write) = split(client);
        let (server_read, server_write) = split(server);
        let [(client_read_handle, client_read_kill), (client_write_handle, client_write_kill), (server_read_handle, server_read_kill), (server_write_handle, server_write_kill)] =
            handles;
        drop((client_read_handle, client_write_handle, server_read_handle, server_write_handle));

        tx.send(SimulatedIncoming {
            remote: self.address,
            read: KillableIo::new(server_read, server_read_kill),
            write: KillableIo::new(server_write, server_write_kill),
        })
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::NotConnected, "remote listener closed"))?;

        Ok(SyncConnection {
            remote: *remote,
            read: KillableIo::new(client_read, client_read_kill),
            write: KillableIo::new(client_write, client_write_kill),
        })
    }
}

impl SyncIOListener for SimulatedIo {
    async fn next_client(&self) -> std::io::Result<SyncConnection<Self>> {
        let incoming = self
            .incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "listener closed"))?;
        Ok(SyncConnection {
            remote: incoming.remote,
            read: incoming.read,
            write: incoming.write,
        })
    }
}

#[derive(Clone)]
struct KillHandle(StdArc<StdMutex<Option<oneshot::Sender<()>>>>);

impl KillHandle {
    fn new() -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (Self(StdArc::new(StdMutex::new(Some(tx)))), rx)
    }

    fn kill(&self) {
        if let Some(tx) = self.0.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

pub struct KillableIo<I> {
    inner: I,
    kill: oneshot::Receiver<()>,
    killed: bool,
}

impl<I> KillableIo<I> {
    fn new(inner: I, kill: oneshot::Receiver<()>) -> Self {
        Self {
            inner,
            kill,
            killed: false,
        }
    }

    fn poll_kill(&mut self, cx: &mut Context<'_>) -> Option<Poll<std::io::Result<()>>> {
        if self.killed {
            return Some(Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection killed"))));
        }

        match Pin::new(&mut self.kill).poll(cx) {
            Poll::Ready(_) => {
                self.killed = true;
                Some(Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection killed"))))
            }
            Poll::Pending => None,
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for KillableIo<I> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if let Some(kill) = self.poll_kill(cx) {
            return kill;
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for KillableIo<I> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        if let Some(kill) = self.poll_kill(cx) {
            return match kill {
                Poll::Ready(Ok(())) => unreachable!(),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            };
        }

        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some(kill) = self.poll_kill(cx) {
            return kill;
        }

        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some(kill) = self.poll_kill(cx) {
            return kill;
        }

        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
