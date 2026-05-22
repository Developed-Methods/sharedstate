use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sharedstate::net::io::{SyncConnection, SyncIO};
use tokio::{
    io::{duplex, split, AsyncRead, AsyncWrite, DuplexStream, ReadBuf, ReadHalf, WriteHalf},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    time::Sleep,
};

static CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct SimNet {
    inner: Arc<RwLock<SimNetInner>>,
}

pub struct SimSyncIo {
    listen_addr: u64,
    next: Mutex<Receiver<(u64, DuplexStream)>>,
    net: SimNet,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
pub struct LinkKey {
    pub a: u64,
    pub b: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkConfig {
    pub enabled: bool,
    pub latency_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LinkSnapshot {
    pub a: u64,
    pub b: u64,
    pub enabled: bool,
    pub latency_ms: u64,
}

struct SimNetInner {
    listeners: HashMap<u64, Sender<(u64, DuplexStream)>>,
    node_network_enabled: HashMap<u64, bool>,
    links: HashMap<LinkKey, LinkConfig>,
    connections: HashMap<(u64, u64), Vec<ConnectionControl>>,
}

#[derive(Clone)]
struct ConnectionControl {
    id: u64,
    killed: Arc<AtomicBool>,
}

pub struct SimIo<I> {
    conn_id: (u64, u64, u64),
    net: SimNet,
    io: I,
    killed: Arc<AtomicBool>,
    delay: Option<Pin<Box<Sleep>>>,
}

impl SimNet {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SimNetInner {
                listeners: HashMap::new(),
                node_network_enabled: HashMap::new(),
                links: HashMap::new(),
                connections: HashMap::new(),
            })),
        }
    }

    pub async fn create_io(&self, listen: u64) -> io::Result<Arc<SimSyncIo>> {
        let (tx, rx) = channel(1024);
        {
            let mut inner = self.inner.write();
            if inner.listeners.contains_key(&listen) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "listener already exists",
                ));
            }
            inner.listeners.insert(listen, tx);
            inner.node_network_enabled.insert(listen, true);
        }

        Ok(Arc::new(SimSyncIo {
            listen_addr: listen,
            next: Mutex::new(rx),
            net: self.clone(),
        }))
    }

    pub fn unregister_node(&self, node: u64) {
        let mut inner = self.inner.write();
        inner.listeners.remove(&node);
        inner.node_network_enabled.insert(node, false);
        inner.kill_node_connections(node);
    }

    pub fn set_node_networking(&self, node: u64, enabled: bool) {
        let mut inner = self.inner.write();
        inner.node_network_enabled.insert(node, enabled);
        if !enabled {
            inner.kill_node_connections(node);
        }
    }

    pub fn node_networking_enabled(&self, node: u64) -> bool {
        self.inner
            .read()
            .node_network_enabled
            .get(&node)
            .copied()
            .unwrap_or(false)
    }

    pub fn set_link(&self, a: u64, b: u64, enabled: bool, latency_ms: u64) {
        let key = LinkKey::new(a, b);
        let mut inner = self.inner.write();
        inner.links.insert(
            key,
            LinkConfig {
                enabled,
                latency_ms,
            },
        );
        if !enabled {
            inner.kill_pair_connections(a, b);
        }
    }

    pub fn link_config(&self, a: u64, b: u64) -> LinkConfig {
        self.inner
            .read()
            .links
            .get(&LinkKey::new(a, b))
            .cloned()
            .unwrap_or_default()
    }

    pub fn link_snapshots(&self, nodes: &[u64]) -> Vec<LinkSnapshot> {
        let inner = self.inner.read();
        let mut links = Vec::new();
        for (i, a) in nodes.iter().enumerate() {
            for b in nodes.iter().skip(i + 1) {
                let config = inner
                    .links
                    .get(&LinkKey::new(*a, *b))
                    .cloned()
                    .unwrap_or_default();
                links.push(LinkSnapshot {
                    a: *a,
                    b: *b,
                    enabled: config.enabled,
                    latency_ms: config.latency_ms,
                });
            }
        }
        links
    }

    pub fn node_link_snapshots(&self, node: u64, nodes: &[u64]) -> Vec<NodeLinkSnapshot> {
        nodes
            .iter()
            .copied()
            .filter(|peer| *peer != node)
            .map(|peer| {
                let config = self.link_config(node, peer);
                NodeLinkSnapshot {
                    peer,
                    enabled: config.enabled,
                    latency_ms: config.latency_ms,
                }
            })
            .collect()
    }

    fn latency_ms(&self, from: u64, to: u64) -> u64 {
        self.link_config(from, to).latency_ms
    }

    fn add_connection(&self, from: u64, to: u64, control: ConnectionControl) {
        self.inner
            .write()
            .connections
            .entry((from, to))
            .or_default()
            .push(control);
    }

    fn remove_connection(&self, from: u64, to: u64, id: u64) {
        let mut inner = self.inner.write();
        let Some(connections) = inner.connections.get_mut(&(from, to)) else {
            return;
        };
        connections.retain(|conn| conn.id != id);
        if connections.is_empty() {
            inner.connections.remove(&(from, to));
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeLinkSnapshot {
    pub peer: u64,
    pub enabled: bool,
    pub latency_ms: u64,
}

impl LinkKey {
    pub fn new(a: u64, b: u64) -> Self {
        if a <= b {
            Self { a, b }
        } else {
            Self { a: b, b: a }
        }
    }
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            latency_ms: 0,
        }
    }
}

impl SimNetInner {
    fn can_connect(&self, from: u64, to: u64) -> io::Result<Sender<(u64, DuplexStream)>> {
        if !self
            .node_network_enabled
            .get(&from)
            .copied()
            .unwrap_or(false)
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "source node networking disabled",
            ));
        }
        if !self.node_network_enabled.get(&to).copied().unwrap_or(false) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "target node networking disabled",
            ));
        }
        let config = self
            .links
            .get(&LinkKey::new(from, to))
            .cloned()
            .unwrap_or_default();
        if !config.enabled {
            return Err(io::Error::new(io::ErrorKind::NotConnected, "link disabled"));
        }
        self.listeners
            .get(&to)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "target node stopped"))
    }

    fn kill_node_connections(&mut self, node: u64) {
        for ((from, to), connections) in &self.connections {
            if *from == node || *to == node {
                for conn in connections {
                    conn.killed.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    fn kill_pair_connections(&mut self, a: u64, b: u64) {
        for ((from, to), connections) in &self.connections {
            if (*from == a && *to == b) || (*from == b && *to == a) {
                for conn in connections {
                    conn.killed.store(true, Ordering::SeqCst);
                }
            }
        }
    }
}

impl SyncIO for SimSyncIo {
    type Address = u64;
    type Read = SimIo<ReadHalf<DuplexStream>>;
    type Write = SimIo<WriteHalf<DuplexStream>>;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        let target = *remote;
        let sender = {
            let inner = self.net.inner.read();
            inner.can_connect(self.listen_addr, target)?
        };

        let (client, server) = duplex(4096);
        sender
            .send((self.listen_addr, server))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "target listener closed"))?;

        let conn_id = CONN_ID.fetch_add(1, Ordering::SeqCst);
        let (read, write) = split(client);
        let read_kill = Arc::new(AtomicBool::new(false));
        let write_kill = Arc::new(AtomicBool::new(false));

        self.net.add_connection(
            target,
            self.listen_addr,
            ConnectionControl {
                id: conn_id,
                killed: read_kill.clone(),
            },
        );
        self.net.add_connection(
            self.listen_addr,
            target,
            ConnectionControl {
                id: conn_id,
                killed: write_kill.clone(),
            },
        );

        Ok(SyncConnection {
            remote: target,
            read: SimIo::new(
                (target, self.listen_addr, conn_id),
                self.net.clone(),
                read,
                read_kill,
            ),
            write: SimIo::new(
                (self.listen_addr, target, conn_id),
                self.net.clone(),
                write,
                write_kill,
            ),
        })
    }

    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (peer, stream) = self
            .next
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "listener stopped"))?;

        let conn_id = CONN_ID.fetch_add(1, Ordering::SeqCst);
        let (read, write) = split(stream);
        let read_kill = Arc::new(AtomicBool::new(false));
        let write_kill = Arc::new(AtomicBool::new(false));

        self.net.add_connection(
            peer,
            self.listen_addr,
            ConnectionControl {
                id: conn_id,
                killed: read_kill.clone(),
            },
        );
        self.net.add_connection(
            self.listen_addr,
            peer,
            ConnectionControl {
                id: conn_id,
                killed: write_kill.clone(),
            },
        );

        Ok(SyncConnection {
            remote: peer,
            read: SimIo::new(
                (peer, self.listen_addr, conn_id),
                self.net.clone(),
                read,
                read_kill,
            ),
            write: SimIo::new(
                (self.listen_addr, peer, conn_id),
                self.net.clone(),
                write,
                write_kill,
            ),
        })
    }
}

impl<I> SimIo<I> {
    fn new(conn_id: (u64, u64, u64), net: SimNet, io: I, killed: Arc<AtomicBool>) -> Self {
        Self {
            conn_id,
            net,
            io,
            killed,
            delay: None,
        }
    }

    fn poll_delay(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.killed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection killed",
            )));
        }

        if self.delay.is_none() {
            let latency = self.net.latency_ms(self.conn_id.0, self.conn_id.1);
            if latency == 0 {
                return Poll::Ready(Ok(()));
            }
            self.delay = Some(Box::pin(tokio::time::sleep(Duration::from_millis(latency))));
        }

        let delay = self.delay.as_mut().unwrap();
        match delay.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.delay = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I> Drop for SimIo<I> {
    fn drop(&mut self) {
        self.net
            .remove_connection(self.conn_id.0, self.conn_id.1, self.conn_id.2);
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for SimIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_delay(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for SimIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_delay(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.killed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection killed",
            )));
        }
        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.killed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection killed",
            )));
        }
        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_shutdown(cx)
    }
}

impl Drop for SimSyncIo {
    fn drop(&mut self) {
        self.net.unregister_node(self.listen_addr);
    }
}
