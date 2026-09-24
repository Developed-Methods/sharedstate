use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc as StdArc, Mutex as StdMutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{duplex, split, AsyncRead, AsyncWrite, DuplexStream, ReadBuf, ReadHalf, WriteHalf},
    sync::{mpsc, oneshot, Mutex},
};

use crate::transport::traits::{SyncConnection, SyncIO, SyncIOListener};

#[derive(Clone)]
pub struct SimulatedNet {
    inner: StdArc<Mutex<SimulatedNetInner>>,
}

struct SimulatedNetInner {
    listeners: HashMap<u64, mpsc::Sender<SimulatedIncoming>>,
    active_connections: HashMap<u64, Vec<KillHandle>>,
    active_connection_edges: HashMap<(u64, u64), Vec<KillHandle>>,
    edge_connections_opened: HashMap<(u64, u64), usize>,
    /// Connections opened by dialer, keyed by what was dialed (a node or a
    /// gateway address), so tests can tell direct dials from routed ones.
    dials: HashMap<(u64, u64), usize>,
    blocked_nodes: HashSet<u64>,
    blocked_edges: HashSet<(u64, u64)>,
    /// One-way blocks: `(from, to)` means `from` cannot dial `to`, while
    /// `to` may still dial `from`. Models voters without egress to observers.
    blocked_dials: HashSet<(u64, u64)>,
    edge_latencies: HashMap<(u64, u64), Duration>,
    blackholed_edges: HashMap<(u64, u64), StdArc<AtomicBool>>,
    /// Load balancers: a dial to the gateway address lands on one of its
    /// members, round robin over the ones currently reachable.
    gateways: HashMap<u64, SimulatedGateway>,
    gateway_connections_opened: HashMap<(u64, u64), usize>,
}

struct SimulatedGateway {
    members: Vec<u64>,
    next: usize,
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
                edge_connections_opened: HashMap::new(),
                dials: HashMap::new(),
                blocked_nodes: HashSet::new(),
                blocked_edges: HashSet::new(),
                blocked_dials: HashSet::new(),
                edge_latencies: HashMap::new(),
                blackholed_edges: HashMap::new(),
                gateways: HashMap::new(),
                gateway_connections_opened: HashMap::new(),
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

    /// Blocks dials from `from` to `to` only; the other direction still
    /// works. Existing connections are left alone.
    pub async fn set_dial_blocked(&self, from: u64, to: u64, blocked: bool) {
        let mut inner = self.inner.lock().await;
        if blocked {
            inner.blocked_dials.insert((from, to));
        } else {
            inner.blocked_dials.remove(&(from, to));
        }
    }

    /// Registers a load balancer at `address` in front of `members`. Dials
    /// to `address` land on members in round robin order, skipping members
    /// that are offline, blocked, or cut off from the dialer. The dialer's
    /// edge to the gateway is what tests block or count; the member edge
    /// only tracks the node so stopping a member still kills its
    /// connections.
    pub async fn add_gateway(&self, address: u64, members: Vec<u64>) {
        let mut inner = self.inner.lock().await;
        assert!(!inner.listeners.contains_key(&address), "gateway address {address} is a node");
        inner.gateways.insert(address, SimulatedGateway { members, next: 0 });
    }

    /// Connections ever routed through `gateway` to `member`.
    pub async fn gateway_connections_opened(&self, gateway: u64, member: u64) -> usize {
        let inner = self.inner.lock().await;
        inner
            .gateway_connections_opened
            .get(&(gateway, member))
            .copied()
            .unwrap_or(0)
    }

    /// Connections `from` ever opened by dialing `to`, where `to` is the
    /// address dialed: a gateway address for routed connections, the node
    /// itself for direct ones.
    pub async fn dials(&self, from: u64, to: u64) -> usize {
        let inner = self.inner.lock().await;
        inner.dials.get(&(from, to)).copied().unwrap_or(0)
    }

    /// Silently stalls all traffic on an edge without closing connections,
    /// like a half-open TCP connection: reads and writes hang instead of
    /// erroring, and new connections establish but carry no bytes.
    pub async fn set_edge_blackholed(&self, a: u64, b: u64, blackholed: bool) {
        let edge = Self::edge_key(a, b);
        self.inner
            .lock()
            .await
            .blackholed_edges
            .entry(edge)
            .or_default()
            .store(blackholed, Ordering::Relaxed);
    }

    pub async fn set_edge_latency(&self, a: u64, b: u64, latency: Option<Duration>) {
        let edge = Self::edge_key(a, b);
        let mut inner = self.inner.lock().await;
        match latency {
            Some(latency) if !latency.is_zero() => {
                inner.edge_latencies.insert(edge, latency);
            }
            _ => {
                inner.edge_latencies.remove(&edge);
            }
        }
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

    /// Connections open on an edge, in either direction. A connection counts
    /// while any of its stream halves is still alive.
    pub async fn edge_connection_count(&self, a: u64, b: u64) -> usize {
        let inner = self.inner.lock().await;
        inner
            .active_connection_edges
            .get(&Self::edge_key(a, b))
            .map(|handles| {
                handles
                    .chunks(HANDLES_PER_CONNECTION)
                    .filter(|connection| connection.iter().any(KillHandle::is_live))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Connections ever opened on an edge, in either direction. Unlike
    /// [`edge_connection_count`](Self::edge_connection_count) this never
    /// decreases, so it also sees connections that closed between polls.
    pub async fn edge_connections_opened(&self, a: u64, b: u64) -> usize {
        let inner = self.inner.lock().await;
        inner
            .edge_connections_opened
            .get(&Self::edge_key(a, b))
            .copied()
            .unwrap_or(0)
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
        let (tx, handles, latency, blackhole) = {
            let mut net = self.net.inner.lock().await;
            let edge = SimulatedNet::edge_key(self.address, *remote);
            if net.blocked_nodes.contains(&self.address)
                || net.blocked_nodes.contains(remote)
                || net.blocked_edges.contains(&edge)
                || net.blocked_dials.contains(&(self.address, *remote))
            {
                return Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "connection blocked"));
            }
            *net.dials.entry((self.address, *remote)).or_default() += 1;

            /* a gateway dial lands on a member; from here on the connection
             * belongs to that member node, but the dialer's edge stays the
             * gateway edge so blocking or counting it covers routed traffic */
            let target = match net.gateways.get_mut(remote) {
                Some(gateway) => {
                    let candidates = gateway.members.clone();
                    let start = gateway.next;
                    gateway.next = gateway.next.wrapping_add(1);
                    let dialer = self.address;
                    let reachable = |net: &SimulatedNetInner, member: u64| {
                        net.listeners.contains_key(&member)
                            && !net.blocked_nodes.contains(&member)
                            && !net.blocked_edges.contains(&SimulatedNet::edge_key(dialer, member))
                            && !net.blocked_dials.contains(&(dialer, member))
                    };
                    let Some(member) = (0..candidates.len())
                        .map(|offset| candidates[(start + offset) % candidates.len()])
                        .find(|member| reachable(&net, *member))
                    else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            "no gateway member reachable",
                        ));
                    };
                    *net.gateway_connections_opened.entry((*remote, member)).or_default() += 1;
                    member
                }
                None => *remote,
            };

            let tx = net.listeners.get(&target).cloned();
            let latency = net.edge_latencies.get(&edge).copied().unwrap_or_default();
            let blackhole = net.blackholed_edges.entry(edge).or_default().clone();
            let handles = [
                KillHandle::new(),
                KillHandle::new(),
                KillHandle::new(),
                KillHandle::new(),
            ];
            let active = net.active_connections.entry(self.address).or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            let active = net.active_connections.entry(target).or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            let active = net.active_connection_edges.entry(edge).or_default();
            active.extend(handles.iter().map(|(handle, _)| handle.clone()));
            *net.edge_connections_opened.entry(edge).or_default() += 1;
            (tx, handles, latency, blackhole)
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
            read: KillableIo::new(server_read, server_read_kill, latency, blackhole.clone()),
            write: KillableIo::new(server_write, server_write_kill, latency, blackhole.clone()),
        })
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::NotConnected, "remote listener closed"))?;

        Ok(SyncConnection {
            remote: *remote,
            read: KillableIo::new(client_read, client_read_kill, latency, blackhole.clone()),
            write: KillableIo::new(client_write, client_write_kill, latency, blackhole),
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

    /// True until the stream half is killed or dropped.
    fn is_live(&self) -> bool {
        self.0.lock().unwrap().as_ref().is_some_and(|tx| !tx.is_closed())
    }
}

/// Each connection registers one handle per stream half: client read and
/// write, server read and write.
const HANDLES_PER_CONNECTION: usize = 4;

/// How often a blackholed stream re-checks whether the blackhole lifted.
const BLACKHOLE_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub struct KillableIo<I> {
    inner: I,
    kill: oneshot::Receiver<()>,
    killed: bool,
    latency: Duration,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
    blackhole: StdArc<AtomicBool>,
    blackhole_delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<I> KillableIo<I> {
    fn new(inner: I, kill: oneshot::Receiver<()>, latency: Duration, blackhole: StdArc<AtomicBool>) -> Self {
        Self {
            inner,
            kill,
            killed: false,
            latency,
            delay: None,
            blackhole,
            blackhole_delay: None,
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

    /// Pending while the edge is blackholed. Wakes on a timer to re-check, so
    /// a lifted blackhole resumes traffic without an explicit wake-up.
    fn poll_blackhole(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if !self.blackhole.load(Ordering::Relaxed) {
                self.blackhole_delay = None;
                return Poll::Ready(());
            }

            if self.blackhole_delay.is_none() {
                self.blackhole_delay = Some(Box::pin(tokio::time::sleep(BLACKHOLE_POLL_INTERVAL)));
            }

            match self.blackhole_delay.as_mut().expect("delay initialized").as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.blackhole_delay = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_latency(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.latency.is_zero() {
            return Poll::Ready(());
        }

        if self.delay.is_none() {
            self.delay = Some(Box::pin(tokio::time::sleep(self.latency)));
        }

        match self.delay.as_mut().expect("delay initialized").as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.delay = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for KillableIo<I> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if let Some(kill) = self.poll_kill(cx) {
            return kill;
        }

        if self.poll_blackhole(cx).is_pending() {
            return Poll::Pending;
        }

        if self.poll_latency(cx).is_pending() {
            return Poll::Pending;
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

        if self.poll_blackhole(cx).is_pending() {
            return Poll::Pending;
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

#[cfg(test)]
mod test {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn gateway_dials_round_robin_over_reachable_members() {
        let net = SimulatedNet::new();
        let observer = net.start_io(9).await;
        let members = [net.start_io(1).await, net.start_io(2).await, net.start_io(3).await];
        net.add_gateway(100, vec![1, 2, 3]).await;

        /* the server side sees the dialer, the dialer sees the gateway */
        let mut landed = Vec::new();
        for _ in 0..3 {
            let conn = observer.connect(&100).await.unwrap();
            assert_eq!(conn.remote, 100);
            for member in &members {
                if let Ok(incoming) = tokio::time::timeout(Duration::from_millis(10), member.next_client()).await {
                    assert_eq!(incoming.unwrap().remote, 9);
                    landed.push(member.address);
                }
            }
        }
        assert_eq!(landed, vec![1, 2, 3]);

        net.set_node_blocked(2, true).await;
        let _ = observer.connect(&100).await.unwrap();
        let _ = observer.connect(&100).await.unwrap();
        assert_eq!(net.gateway_connections_opened(100, 2).await, 1);
        assert_eq!(net.gateway_connections_opened(100, 1).await, 2);
        assert_eq!(net.gateway_connections_opened(100, 3).await, 2);

        /* direct dials and routed dials are counted apart */
        assert_eq!(net.dials(9, 100).await, 5);
        assert_eq!(net.dials(9, 1).await, 0);
        assert_eq!(net.edge_connections_opened(9, 1).await, 0);

        net.set_edge_blocked(9, 100, true).await;
        assert!(observer.connect(&100).await.is_err());
    }

    #[tokio::test]
    async fn edge_latency_delays_reads() {
        let net = SimulatedNet::new();
        let io1 = net.start_io(1).await;
        let io2 = net.start_io(2).await;
        net.set_edge_latency(1, 2, Some(Duration::from_millis(100))).await;

        let server = tokio::spawn(async move {
            let mut conn = io2.next_client().await.unwrap();
            let started = tokio::time::Instant::now();
            let mut buf = [0u8; 1];
            conn.read.read_exact(&mut buf).await.unwrap();
            (started.elapsed(), buf[0])
        });

        let mut client = io1.connect(&2).await.unwrap();
        client.write.write_all(&[42]).await.unwrap();

        let (elapsed, byte) = server.await.unwrap();
        assert_eq!(byte, 42);
        assert!(elapsed >= Duration::from_millis(80), "elapsed={elapsed:?}");
    }
}
