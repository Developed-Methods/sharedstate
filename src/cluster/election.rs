use std::{
    future::Future,
    io,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, Identity, PutOptions, TlsOptions, Txn, TxnOp,
    WatchOptions, WatchStream,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    protocol::messages::{LeadershipEpoch, PROTOCOL_VERSION},
    transport::traits::SyncIOAddress,
};

#[derive(Clone, Debug, Default)]
pub struct EtcdTlsConfig {
    pub ca_certificate: Option<Vec<u8>>,
    pub client_certificate: Option<Vec<u8>>,
    pub client_key: Option<Vec<u8>>,
    pub domain_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EtcdElectionConfig {
    pub endpoints: Vec<String>,
    pub cluster_id: String,
    pub election_incarnation: u128,
    pub tls_configuration: Option<EtcdTlsConfig>,
    pub credentials: Option<(String, String)>,
    pub lease_ttl: Duration,
    pub renewal_interval: Duration,
    pub request_timeout: Duration,
    pub authority_safety_margin: Duration,
    pub promotion_timeout: Duration,
    pub bootstrap: bool,
}

impl Default for EtcdElectionConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://127.0.0.1:2379".into()],
            cluster_id: "default".into(),
            election_incarnation: 1,
            tls_configuration: None,
            credentials: None,
            lease_ttl: Duration::from_secs(15),
            renewal_interval: Duration::from_secs(5),
            request_timeout: Duration::from_secs(2),
            authority_safety_margin: Duration::from_secs(3),
            promotion_timeout: Duration::from_secs(30),
            bootstrap: false,
        }
    }
}

impl EtcdElectionConfig {
    pub fn validate(&self) -> io::Result<()> {
        if self.endpoints.is_empty()
            || self.cluster_id.is_empty()
            || self.cluster_id.contains('/')
            || self.election_incarnation == 0
        {
            return Err(io::Error::other("etcd endpoints, cluster ID and nonzero incarnation are required"));
        }
        if self.lease_ttl.as_secs() == 0
            || self.lease_ttl.as_secs() > i64::MAX as u64
            || self.renewal_interval.is_zero()
            || self.request_timeout.is_zero()
            || self.promotion_timeout.is_zero()
            || self.authority_safety_margin.is_zero()
            || self.renewal_interval + self.request_timeout + self.authority_safety_margin
                >= Duration::from_secs(self.lease_ttl.as_secs())
        {
            return Err(io::Error::other("lease TTL must exceed renewal interval, request timeout and safety margin"));
        }
        if let Some(tls) = &self.tls_configuration
            && tls.client_certificate.is_some() != tls.client_key.is_some()
        {
            return Err(io::Error::other("etcd TLS client certificate and key must be supplied together"));
        }
        Ok(())
    }

    pub fn leader_key(&self) -> String {
        format!("/sharedstate/{}/{:032x}/leader", self.cluster_id, self.election_incarnation)
    }
}

#[derive(Clone, Debug)]
pub struct LeadershipPermit {
    pub session_id: u128,
    pub epoch: LeadershipEpoch,
    inner: Arc<Mutex<PermitDeadline>>,
}

#[derive(Debug)]
struct PermitDeadline {
    deadline: Instant,
    revoked: bool,
}

impl LeadershipPermit {
    pub(crate) fn new(session_id: u128, epoch: LeadershipEpoch, deadline: Instant) -> Self {
        Self {
            session_id,
            epoch,
            inner: Arc::new(Mutex::new(PermitDeadline {
                deadline,
                revoked: false,
            })),
        }
    }
    pub fn valid(&self) -> bool {
        let mut state = self.inner.lock().unwrap();
        if Instant::now() >= state.deadline {
            state.revoked = true;
        }
        !state.revoked
    }
    pub fn deadline(&self) -> Instant {
        self.inner.lock().unwrap().deadline
    }
    pub fn revoke(&self) {
        self.inner.lock().unwrap().revoked = true;
    }
    fn renew(&self, deadline: Instant) -> io::Result<()> {
        let mut state = self.inner.lock().unwrap();
        if state.revoked || Instant::now() >= state.deadline || Instant::now() >= deadline {
            state.revoked = true;
            return Err(io::Error::other("leadership permit expired"));
        }
        state.deadline = deadline;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner<A> {
    pub address: A,
    pub session_id: u128,
    pub epoch: LeadershipEpoch,
    pub lease_id: i64,
    pub ready: bool,
}

pub struct Observation<A> {
    pub owner: Option<Owner<A>>,
    pub revision: i64,
    pub valid_until: Instant,
}

pub struct Session<A> {
    pub owner: Owner<A>,
    pub permit: LeadershipPermit,
    preparing: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    protocol_version: u64,
    node_address: Vec<u8>,
    process_session_id: String,
    phase: String,
}

pub struct EtcdElection<A> {
    client: Client,
    config: EtcdElectionConfig,
    key: String,
    watch: Option<WatchStream>,
    revision: i64,
    next_read: Instant,
    address: PhantomData<A>,
    #[cfg(test)]
    lose_acquisition_response: bool,
}

async fn bounded<T>(duration: Duration, future: impl Future<Output = Result<T, etcd_client::Error>>) -> io::Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "etcd request timed out"))?
        .map_err(io::Error::other)
}

fn lease_deadline(start: Instant, ttl: i64, margin: Duration) -> io::Result<Instant> {
    let remaining = u64::try_from(ttl)
        .ok()
        .and_then(|ttl| Duration::from_secs(ttl).checked_sub(margin))
        .ok_or_else(|| io::Error::other("etcd returned unusable lease TTL"))?;
    let deadline = start
        .checked_add(remaining)
        .ok_or_else(|| io::Error::other("lease deadline overflow"))?;
    if deadline <= Instant::now() {
        return Err(io::Error::other("etcd lease response arrived too late"));
    }
    Ok(deadline)
}

impl<A: SyncIOAddress> EtcdElection<A> {
    pub async fn connect(config: EtcdElectionConfig) -> io::Result<Self> {
        config.validate()?;
        let mut options = ConnectOptions::new()
            .with_timeout(config.request_timeout)
            .with_connect_timeout(config.request_timeout);
        if let Some((user, password)) = &config.credentials {
            options = options.with_user(user, password);
        }
        if let Some(tls) = &config.tls_configuration {
            let mut options_tls = TlsOptions::new();
            if let Some(ca) = &tls.ca_certificate {
                options_tls = options_tls.ca_certificate(Certificate::from_pem(ca));
            }
            if let (Some(cert), Some(key)) = (&tls.client_certificate, &tls.client_key) {
                options_tls = options_tls.identity(Identity::from_pem(cert, key));
            }
            if let Some(domain) = &tls.domain_name {
                options_tls = options_tls.domain_name(domain);
            }
            options = options.with_tls(options_tls);
        }
        let client = bounded(config.request_timeout, Client::connect(&config.endpoints, Some(options))).await?;
        Ok(Self {
            key: config.leader_key(),
            config,
            client,
            watch: None,
            revision: 0,
            next_read: Instant::now(),
            address: PhantomData,
            #[cfg(test)]
            lose_acquisition_response: false,
        })
    }

    fn decode(&self, kv: &etcd_client::KeyValue) -> io::Result<Owner<A>> {
        let record: Record = serde_json::from_slice(kv.value()).map_err(io::Error::other)?;
        if record.protocol_version != PROTOCOL_VERSION
            || !matches!(record.phase.as_str(), "ready" | "preparing")
            || kv.lease() == 0
            || kv.create_revision() <= 0
        {
            return Err(io::Error::other("invalid etcd leader record"));
        }
        let mut encoded = record.node_address.as_slice();
        let address = A::read_from(&mut encoded)?;
        if !encoded.is_empty() {
            return Err(io::Error::other("trailing owner address bytes"));
        }
        Ok(Owner {
            address,
            session_id: record.process_session_id.parse().map_err(io::Error::other)?,
            epoch: LeadershipEpoch {
                incarnation: self.config.election_incarnation,
                revision: kv.create_revision(),
            },
            lease_id: kv.lease(),
            ready: record.phase == "ready",
        })
    }

    pub async fn read(&mut self) -> io::Result<Observation<A>> {
        let start = Instant::now();
        let response = bounded(self.config.request_timeout, self.client.get(self.key.clone(), None)).await?;
        let revision = response
            .header()
            .ok_or_else(|| io::Error::other("missing etcd response header"))?
            .revision();
        self.revision = revision;
        self.next_read = start + self.config.renewal_interval;
        Ok(Observation {
            owner: response.kvs().first().map(|kv| self.decode(kv)).transpose()?,
            revision,
            valid_until: start + self.config.renewal_interval + self.config.request_timeout,
        })
    }

    pub async fn observe(&mut self) -> io::Result<Observation<A>> {
        if self.revision == 0 || Instant::now() >= self.next_read {
            return self.read().await;
        }
        if self.watch.is_none() {
            self.watch = Some(
                bounded(
                    self.config.request_timeout,
                    self.client
                        .watch(self.key.clone(), Some(WatchOptions::new().with_start_revision(self.revision + 1))),
                )
                .await?,
            );
        }
        tokio::select! {
            _ = tokio::time::sleep_until(self.next_read) => {},
            event = self.watch.as_mut().unwrap().message() => {
                match event {
                    Ok(Some(response)) if !response.canceled() && response.compact_revision() == 0 => {},
                    _ => self.watch = None,
                }
            }
        }
        self.read().await
    }

    pub async fn acquire(&mut self, address: A) -> io::Result<Option<Session<A>>> {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(io::Error::other)?;
        let session_id = u128::from_le_bytes(random);
        let mut node_address = Vec::new();
        address.write_to(&mut node_address)?;
        let preparing = serde_json::to_vec(&Record {
            protocol_version: PROTOCOL_VERSION,
            node_address,
            process_session_id: session_id.to_string(),
            phase: "preparing".into(),
        })
        .map_err(io::Error::other)?;
        let start = Instant::now();
        let lease =
            bounded(self.config.request_timeout, self.client.lease_grant(self.config.lease_ttl.as_secs() as i64, None))
                .await?;
        let attempt = async {
            let deadline = lease_deadline(start, lease.ttl(), self.config.authority_safety_margin)?;
            let txn = Txn::new()
                .when([Compare::version(self.key.clone(), CompareOp::Equal, 0)])
                .and_then([TxnOp::put(
                    self.key.clone(),
                    preparing.clone(),
                    Some(PutOptions::new().with_lease(lease.id())),
                )])
                .or_else([TxnOp::get(self.key.clone(), None)]);
            // A read verifies ownership even when the transaction response was lost.
            let _ = self.acquisition_transaction(txn).await;
            let observed = self.read().await?;
            match observed.owner {
                Some(owner)
                    if owner.address == address
                        && owner.session_id == session_id
                        && owner.lease_id == lease.id()
                        && !owner.ready
                        && Instant::now() < deadline =>
                {
                    let permit = LeadershipPermit::new(session_id, owner.epoch, deadline);
                    Ok(Some(Session {
                        owner,
                        permit,
                        preparing,
                    }))
                }
                _ => Ok(None),
            }
        }
        .await;
        if !matches!(attempt, Ok(Some(_))) {
            let _ = bounded(self.config.request_timeout, self.client.lease_revoke(lease.id())).await;
        }
        attempt
    }

    async fn acquisition_transaction(&mut self, txn: Txn) -> io::Result<()> {
        bounded(self.config.request_timeout, self.client.txn(txn)).await?;
        #[cfg(test)]
        if self.lose_acquisition_response {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "injected lost acquisition response"));
        }
        Ok(())
    }

    pub async fn renew(&mut self, session: &mut Session<A>) -> io::Result<()> {
        let result = async {
            if !session.permit.valid() {
                return Err(io::Error::other("leadership permit expired"));
            }
            let start = Instant::now();
            let ttl = bounded(self.config.request_timeout, async {
                let (mut keeper, mut stream) = self.client.lease_keep_alive(session.owner.lease_id).await?;
                keeper.keep_alive().await?;
                stream.message().await
            })
            .await?
            .ok_or_else(|| io::Error::other("etcd closed lease renewal stream"))?;
            if ttl.id() != session.owner.lease_id {
                return Err(io::Error::other("lease renewal identity mismatch"));
            }
            let deadline = lease_deadline(start, ttl.ttl(), self.config.authority_safety_margin)?;
            if self.read().await?.owner.as_ref() != Some(&session.owner) {
                return Err(io::Error::other("etcd leadership ownership changed"));
            }
            session.permit.renew(deadline)
        }
        .await;
        if result.is_err() {
            session.permit.revoke();
        }
        result
    }

    pub async fn publish_ready(&mut self, session: &mut Session<A>) -> io::Result<Owner<A>> {
        let result = async {
            if !session.permit.valid() {
                return Err(io::Error::other("leadership permit expired"));
            }
            let mut record: Record = serde_json::from_slice(&session.preparing).map_err(io::Error::other)?;
            record.phase = "ready".into();
            let ready = serde_json::to_vec(&record).map_err(io::Error::other)?;
            let txn = Txn::new()
                .when([
                    Compare::create_revision(self.key.clone(), CompareOp::Equal, session.owner.epoch.revision),
                    Compare::value(self.key.clone(), CompareOp::Equal, session.preparing.clone()),
                    Compare::lease(self.key.clone(), CompareOp::Equal, session.owner.lease_id),
                ])
                .and_then([TxnOp::put(
                    self.key.clone(),
                    ready,
                    Some(PutOptions::new().with_lease(session.owner.lease_id)),
                )]);
            bounded(self.config.request_timeout, self.client.txn(txn)).await?;
            let mut expected = session.owner.clone();
            expected.ready = true;
            if self.read().await?.owner.as_ref() != Some(&expected) || !session.permit.valid() {
                return Err(io::Error::other("ready owner verification failed"));
            }
            session.owner = expected.clone();
            Ok(expected)
        }
        .await;
        if result.is_err() {
            session.permit.revoke();
        }
        result
    }

    pub async fn revoke(&mut self, session: &Session<A>) -> io::Result<()> {
        session.permit.revoke();
        bounded(self.config.request_timeout, self.client.lease_revoke(session.owner.lease_id)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn expired_permits_cannot_be_revived() {
        let epoch = LeadershipEpoch {
            incarnation: u128::MAX,
            revision: i64::MAX,
        };
        let permit = LeadershipPermit::new(1, epoch, Instant::now() + Duration::from_secs(5));
        let clone = permit.clone();
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(!clone.valid());
        assert!(permit.renew(Instant::now() + Duration::from_secs(20)).is_err());
        assert!(!permit.valid());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_lease_response_is_rejected() {
        let start = Instant::now();
        tokio::time::advance(Duration::from_secs(13)).await;
        assert!(lease_deadline(start, 15, Duration::from_secs(3)).is_err());
    }

    #[test]
    fn rejects_unsafe_configuration() {
        let mut config = EtcdElectionConfig::default();
        assert!(config.validate().is_ok());
        config.renewal_interval = Duration::from_secs(10);
        assert!(config.validate().is_err());
    }
    #[tokio::test]
    async fn contenders_ready_renewal_and_same_address_restart() {
        let server = crate::test_support::TestEtcd::start().await;
        let mut first = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        let mut second = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        let (a, b) = tokio::join!(first.acquire(1), second.acquire(2));
        let (mut winner, mut loser, mut session) = match (a.unwrap(), b.unwrap()) {
            (Some(session), None) => (first, second, session),
            (None, Some(session)) => (second, first, session),
            _ => panic!("expected exactly one owner"),
        };
        assert!(!session.owner.ready);
        let epoch = session.owner.epoch;
        winner.publish_ready(&mut session).await.unwrap();
        assert_eq!(session.owner.epoch, epoch);
        winner.renew(&mut session).await.unwrap();
        assert!(session.permit.valid());
        assert_eq!(loser.read().await.unwrap().owner, Some(session.owner.clone()));
        winner.revoke(&session).await.unwrap();
        assert!(!session.permit.valid());
        let mut replacement = loser.acquire(session.owner.address).await.unwrap().unwrap();
        loser.publish_ready(&mut replacement).await.unwrap();
        assert_ne!(replacement.owner.session_id, session.owner.session_id);
        assert!(replacement.owner.epoch.revision > epoch.revision);
        let _ = winner.revoke(&session).await;
        assert_eq!(loser.read().await.unwrap().owner, Some(replacement.owner.clone()));
        loser.revoke(&replacement).await.unwrap();
    }

    #[tokio::test]
    async fn renewal_failure_revokes_shared_permit() {
        let mut server = crate::test_support::TestEtcd::start().await;
        let mut adapter = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        let mut session = adapter.acquire(1).await.unwrap().unwrap();
        adapter.publish_ready(&mut session).await.unwrap();
        let permit = session.permit.clone();
        server.stop();
        assert!(adapter.renew(&mut session).await.is_err());
        assert!(!permit.valid());
    }

    #[tokio::test]
    async fn compacted_watch_and_silent_watch_refresh_from_linearizable_read() {
        let server = crate::test_support::TestEtcd::start().await;
        let mut observer = EtcdElection::<u64>::connect(server.config(false)).await.unwrap();
        let initial = observer.read().await.unwrap();
        assert!(initial.owner.is_none());
        let mut writer = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        let mut old = writer.acquire(1).await.unwrap().unwrap();
        writer.publish_ready(&mut old).await.unwrap();
        writer.revoke(&old).await.unwrap();
        let mut latest = writer.acquire(2).await.unwrap().unwrap();
        writer.publish_ready(&mut latest).await.unwrap();
        let revision = writer.read().await.unwrap().revision;
        writer.client.compact(revision, None).await.unwrap();
        assert_eq!(observer.observe().await.unwrap().owner, Some(latest.owner.clone()));
        // Periodic reads refresh validity without any owner watch event.
        observer.watch = None;
        observer.next_read = Instant::now();
        let fresh = observer.observe().await.unwrap();
        assert!(fresh.valid_until > Instant::now());
        assert_eq!(fresh.owner, Some(latest.owner.clone()));
        writer.revoke(&latest).await.unwrap();
    }
    #[tokio::test]
    async fn lost_acquisition_response_requires_session_verification() {
        let server = crate::test_support::TestEtcd::start().await;
        let mut first = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        first.lose_acquisition_response = true;
        let mut session = first.acquire(1).await.unwrap().unwrap();
        assert!(!session.owner.ready);
        assert_eq!(first.read().await.unwrap().owner, Some(session.owner.clone()));
        first.publish_ready(&mut session).await.unwrap();
        let mut second = EtcdElection::<u64>::connect(server.config(true)).await.unwrap();
        second.lose_acquisition_response = true;
        assert!(second.acquire(1).await.unwrap().is_none());
        first.revoke(&session).await.unwrap();
    }

    #[tokio::test]
    async fn three_member_quorum_loss_stops_authority_and_acquisition() {
        use std::{
            net::TcpListener,
            path::PathBuf,
            process::{Child, Command, Stdio},
        };
        struct Cluster {
            children: Vec<Child>,
            directory: PathBuf,
        }
        impl Drop for Cluster {
            fn drop(&mut self) {
                for child in &mut self.children {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                let _ = std::fs::remove_dir_all(&self.directory);
            }
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).unwrap();
        let id = u128::from_le_bytes(random);
        let mut cluster = Cluster {
            children: Vec::new(),
            directory: std::env::temp_dir().join(format!("sharedstate-quorum-{id}")),
        };
        let sockets = (0..6)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let urls = sockets
            .iter()
            .map(|socket| format!("http://{}", socket.local_addr().unwrap()))
            .collect::<Vec<_>>();
        let initial_cluster = (0..3)
            .map(|index| format!("node{index}={}", urls[index * 2 + 1]))
            .collect::<Vec<_>>()
            .join(",");
        drop(sockets);
        for index in 0..3 {
            let child = Command::new(std::env::var("ETCD_BIN").unwrap_or_else(|_| "etcd".into()))
                .arg("--name")
                .arg(format!("node{index}"))
                .arg("--data-dir")
                .arg(cluster.directory.join(index.to_string()))
                .arg("--listen-client-urls")
                .arg(&urls[index * 2])
                .arg("--advertise-client-urls")
                .arg(&urls[index * 2])
                .arg("--listen-peer-urls")
                .arg(&urls[index * 2 + 1])
                .arg("--initial-advertise-peer-urls")
                .arg(&urls[index * 2 + 1])
                .arg("--initial-cluster")
                .arg(&initial_cluster)
                .arg("--initial-cluster-token")
                .arg(id.to_string())
                .args(["--log-level", "error"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("install etcd or set ETCD_BIN");
            cluster.children.push(child);
        }
        let config = EtcdElectionConfig {
            endpoints: vec![urls[0].clone()],
            cluster_id: format!("quorum-{id}"),
            ..EtcdElectionConfig::default()
        };
        let mut adapter = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(mut candidate) = EtcdElection::<u64>::connect(config.clone()).await
                    && candidate.read().await.is_ok()
                {
                    break candidate;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        let mut session = adapter.acquire(1).await.unwrap().unwrap();
        adapter.publish_ready(&mut session).await.unwrap();
        let permit = session.permit.clone();
        for child in &mut cluster.children[1..] {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        assert!(adapter.renew(&mut session).await.is_err());
        assert!(!permit.valid());
        assert!(adapter.acquire(2).await.is_err());
    }
}
