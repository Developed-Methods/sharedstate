use std::{collections::BTreeMap, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout},
};

struct Process {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
}
impl Process {
    async fn start(path: &std::path::Path, cluster: uuid::Uuid, id: u64, port: u16) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sharedstate-counter"))
            .arg(path)
            .arg(cluster.to_string())
            .arg(id.to_string())
            .arg(format!("127.0.0.1:{port}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
        assert_eq!(
            timeout(Duration::from_secs(10), output.next_line())
                .await
                .unwrap()
                .unwrap()
                .as_deref(),
            Some("ready")
        );
        Self { child, input, output }
    }
    async fn command(&mut self, command: &str) -> serde_json::Value {
        self.input.write_all(format!("{command}\n").as_bytes()).await.unwrap();
        self.input.flush().await.unwrap();
        let line = timeout(Duration::from_secs(7), self.output.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("process response");
        serde_json::from_str(&line).unwrap()
    }
    async fn kill(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t10_process_kill_complete_restart_uses_durable_receipts_with_default_timers() {
    let directory = tempfile::tempdir().unwrap();
    let cluster = uuid::Uuid::new_v4();
    let sockets: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = sockets.iter().map(|s| s.local_addr().unwrap().port()).collect();
    drop(sockets);
    let mut processes = BTreeMap::new();
    for id in 1..=3 {
        processes.insert(
            id,
            Process::start(&directory.path().join(format!("{id}.db")), cluster, id, ports[id as usize - 1]).await,
        );
    }
    let members = ports
        .iter()
        .enumerate()
        .map(|(i, port)| format!("{}=127.0.0.1:{port}", i + 1))
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(
        processes
            .get_mut(&1)
            .unwrap()
            .command(&format!("bootstrap {members}"))
            .await["ok"],
        true
    );
    let client = uuid::Uuid::new_v4();
    let command = format!("submit {client} 1 17");
    let started = Instant::now();
    let mut committed = false;
    while started.elapsed() < Duration::from_secs(20) {
        for (id, process) in processes.iter_mut() {
            if process.command("status").await["leader"].as_u64() == Some(*id) {
                let result = process.command(&command).await;
                if result["ok"] == true {
                    assert_eq!(result["result"], 17);
                    committed = true;
                    break;
                }
            }
        }
        if committed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(committed);
    for (_, process) in processes {
        process.kill().await;
    }
    let recovered = Instant::now();
    let mut processes = BTreeMap::new();
    for id in 1..=3 {
        processes.insert(
            id,
            Process::start(&directory.path().join(format!("{id}.db")), cluster, id, ports[id as usize - 1]).await,
        );
    }
    let mut verified = false;
    while recovered.elapsed() < Duration::from_secs(20) {
        for (id, process) in processes.iter_mut() {
            if process.command("status").await["leader"].as_u64() == Some(*id) {
                let result = process.command(&command).await;
                if result["ok"] == true {
                    assert_eq!(result["result"], 17, "retry after process death must not increment again");
                    verified = true;
                    break;
                }
            }
        }
        if verified {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(verified);
    eprintln!("default-timer full-cluster recovery: {:?}", recovered.elapsed());
    for (_, process) in processes {
        process.kill().await;
    }
}
