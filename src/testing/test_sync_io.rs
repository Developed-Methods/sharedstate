use std::{collections::{hash_map::{self, Entry}, HashMap, HashSet}, future::Future, pin::Pin, sync::{atomic::{AtomicU64, Ordering}, Arc}, task::{Context, Poll}};
use tokio::{io::{duplex, split, AsyncRead, AsyncWrite, DuplexStream, ReadHalf, WriteHalf}, sync::{mpsc::{channel, Receiver, Sender}, oneshot, Mutex, RwLock}};
use crate::io::{SyncConnection, SyncIO};

use super::blocking_rw_lock;

static CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct TestSyncNet(Arc<RwLock<NetInner>>);

impl TestSyncNet {
    pub fn new() -> Self {
        TestSyncNet(Arc::new(RwLock::new(NetInner {
            listeners: HashMap::new(),
            block_connections: HashSet::new(),
            connections: HashMap::new(),
        })))
    }

    pub async fn block_connection(&self, a: u64, b: u64, block: bool) {
        let key = if b < a {
            (b, a)
        } else {
            (a, b)
        };

        let mut lock = self.0.write().await;
        if block {
            lock.block_connections.insert(key);
        } else {
            lock.block_connections.remove(&key);
        }
    }

    pub async fn kill_connection(&self, from: u64, to: u64, mode: TestIOKillMode) -> u64 {
        let mut lock = self.0.write().await;
        let Some(conns) = lock.connections.get_mut(&(from, to)) else { return 0 };

        let mut count = 0;
        for (_, send) in conns {
            if let Some(send) = send.take() {
                count += 1;
                let _ = send.send(mode);
            }
        }

        count
    }

    pub async fn io(&self, listen: u64) -> Arc<TestSyncIO> {
        Arc::new(self.create(listen).await.unwrap())
    }

    pub async fn create(&self, listen: u64) -> Option<TestSyncIO> {
        let rx = {
            let mut lock = self.0.write().await;
            let Entry::Vacant(entry) = lock.listeners.entry(listen) else { return None };

            let (tx, rx) = channel(1024);
            entry.insert(tx);
            rx
        };

        Some(TestSyncIO {
            listen_addr: listen,
            next: Mutex::new(rx),
            net: self.clone(),
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TestIOKillMode {
    Timeout,
    Shutdown,
}

pub struct TestSyncIO {
    listen_addr: u64,
    next: Mutex<Receiver<(u64, DuplexStream)>>,
    net: TestSyncNet,
}

struct NetInner {
    listeners: HashMap<u64, Sender<(u64, DuplexStream)>>,
    block_connections: HashSet<(u64, u64)>,
    connections: HashMap<(u64, u64), Vec<(u64, Option<oneshot::Sender<TestIOKillMode>>)>>,
}

impl NetInner {
    fn add_conn(&mut self, id: (u64, u64, u64), tx: oneshot::Sender<TestIOKillMode>) {
        let conns = match self.connections.entry((id.0, id.1)) {
            hash_map::Entry::Occupied(o) => o.into_mut(),
            hash_map::Entry::Vacant(v) => v.insert(vec![]),
        };

        conns.push((id.2, Some(tx)));
    }
}

impl SyncIO for TestSyncIO {
    type Address = u64;
    type Read = BreakableIO<ReadHalf<DuplexStream>>;
    type Write = BreakableIO<WriteHalf<DuplexStream>>;

    async fn connect(&self, remote: &Self::Address) -> std::io::Result<crate::io::SyncConnection<Self>> {
        let mut lock = self.net.0.write().await;

        let Some(tx) = lock.listeners.get(remote) else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "not listener on address"));
        };

        let key = if self.listen_addr < *remote {
            (self.listen_addr, *remote)
        } else {
            (*remote, self.listen_addr)
        };

        if lock.block_connections.contains(&key) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "connection blocked"));
        }

        let (client, server) = duplex(1024);
        if tx.send((self.listen_addr, server)).await.is_err() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "not listener on address"));
        }

        let conn_id = CONN_ID.fetch_add(1, Ordering::SeqCst);
        let (read, write) = split(client);

        let (read, read_kill) = BreakableIO::new((*remote, self.listen_addr, conn_id), self.net.clone(), read);
        let (write, write_kill) = BreakableIO::new((self.listen_addr, *remote, conn_id), self.net.clone(), write);

        lock.add_conn((*remote, self.listen_addr, conn_id), read_kill);
        lock.add_conn((self.listen_addr, *remote, conn_id), write_kill);

        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }

    async fn next_client(&self) -> std::io::Result<crate::io::SyncConnection<Self>> {
        let (peer_id, client) = {
            let mut lock = self.next.lock().await;
            let Some(recv) = lock.recv().await else {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "network shutdown"));
            };
            recv
        };

        let (read, write) = split(client);
        let conn_id = CONN_ID.fetch_add(1, Ordering::SeqCst);
        
        let (read, read_kill) = BreakableIO::new((peer_id, self.listen_addr, conn_id), self.net.clone(), read);
        let (write, write_kill) = BreakableIO::new((self.listen_addr, peer_id, conn_id), self.net.clone(), write);

        let mut lock = self.net.0.write().await;
        lock.add_conn((peer_id, self.listen_addr, conn_id), read_kill);
        lock.add_conn((self.listen_addr, peer_id, conn_id), write_kill);

        Ok(SyncConnection {
            remote: peer_id,
            read,
            write
        })
    }
}

pub struct BreakableIO<I> {
    conn_id: (u64, u64, u64),
    net: TestSyncNet,
    io: I,
    kill: oneshot::Receiver<TestIOKillMode>,
    killed: Option<TestIOKillMode>,
}

impl<I: Unpin> BreakableIO<I> {
    fn new(conn_id: (u64, u64, u64), net: TestSyncNet, io: I) -> (Self, oneshot::Sender<TestIOKillMode>) {
        let (tx, rx) = oneshot::channel();

        (
            BreakableIO {
                conn_id,
                net,
                io,
                kill: rx,
                killed: None,
            },
            tx
        )
    }

    fn poll_kill(&mut self, cx: &mut Context<'_>) -> Option<Poll<std::io::Error>> {
        if self.killed.is_none() {
            let kill = unsafe { Pin::new_unchecked(&mut self.kill) };
            self.killed = match kill.poll(cx) {
                Poll::Ready(kill) => kill.ok(),
                Poll::Pending => None,
            };
        }

        match self.killed {
            Some(TestIOKillMode::Timeout) => Some(Poll::Pending),
            Some(TestIOKillMode::Shutdown) => Some(Poll::Ready(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection shutdown"))),
            None => None,
        }
    }
}

impl<I> Drop for BreakableIO<I> {
    fn drop(&mut self) {
        let mut lock = blocking_rw_lock(&self.net.0);

        let key = (self.conn_id.0, self.conn_id.1);
        let conns = lock.connections.get_mut(&key).unwrap();

        let index = conns.iter().position(|c| c.0 == self.conn_id.2).unwrap();
        conns.swap_remove(index);

        if conns.is_empty() {
            lock.connections.remove(&key);
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for BreakableIO<I> {
    fn poll_read(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>,) -> std::task::Poll<std::io::Result<()>> {
        match self.poll_kill(cx) {
            Some(Poll::Pending) => return Poll::Pending,
            Some(Poll::Ready(err)) => return Poll::Ready(Err(err)),
            None => {}
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for BreakableIO<I> {
    fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
        match self.poll_kill(cx) {
            Some(Poll::Pending) => return Poll::Pending,
            Some(Poll::Ready(err)) => return Poll::Ready(Err(err)),
            None => {}
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.poll_kill(cx) {
            Some(Poll::Pending) => return Poll::Pending,
            Some(Poll::Ready(err)) => return Poll::Ready(Err(err)),
            None => {}
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.poll_kill(cx) {
            Some(Poll::Pending) => return Poll::Pending,
            Some(Poll::Ready(err)) => return Poll::Ready(Err(err)),
            None => {}
        }

        let io = unsafe { Pin::new_unchecked(&mut self.io) };
        io.poll_shutdown(cx)
    }
}

impl Drop for TestSyncIO {
    fn drop(&mut self) {
        let mut lock = blocking_rw_lock(&self.net.0);
        lock.listeners.remove(&self.listen_addr);
    }
}

