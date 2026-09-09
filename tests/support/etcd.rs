use crate::cluster::election::EtcdElectionConfig;
use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static NEXT_CLUSTER: AtomicU64 = AtomicU64::new(1);

pub struct TestEtcd {
    child: Option<Child>,
    directory: PathBuf,
    endpoint: String,
    cluster_id: String,
}

impl TestEtcd {
    pub async fn start() -> Self {
        let id = NEXT_CLUSTER.fetch_add(1, Ordering::Relaxed);
        let cluster_id = format!("test-{}-{id}", std::process::id());
        let directory = std::env::temp_dir().join(format!("sharedstate-{cluster_id}"));
        let reserve = || TcpListener::bind("127.0.0.1:0").unwrap();
        let client = reserve();
        let peer = reserve();
        let endpoint = format!("http://{}", client.local_addr().unwrap());
        let peer_endpoint = format!("http://{}", peer.local_addr().unwrap());
        drop((client, peer));
        let binary = std::env::var("ETCD_BIN").unwrap_or_else(|_| "etcd".to_owned());
        let child = Command::new(binary)
            .args(["--name", "test", "--data-dir"])
            .arg(&directory)
            .args([
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_endpoint,
                "--initial-advertise-peer-urls",
                &peer_endpoint,
                "--initial-cluster",
                &format!("test={peer_endpoint}"),
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("install etcd or set ETCD_BIN to run election integration tests");
        let server = Self {
            child: Some(child),
            directory,
            endpoint,
            cluster_id,
        };
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(mut client) = etcd_client::Client::connect([server.endpoint.clone()], None).await
                    && client.get("health", None).await.is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("test etcd did not become healthy");
        server
    }

    pub fn config(&self, bootstrap: bool) -> EtcdElectionConfig {
        EtcdElectionConfig {
            endpoints: vec![self.endpoint.clone()],
            cluster_id: self.cluster_id.clone(),
            election_incarnation: 1,
            bootstrap,
            ..EtcdElectionConfig::default()
        }
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for TestEtcd {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
