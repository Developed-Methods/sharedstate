use std::{collections::BTreeMap, io::Result, sync::Arc, time::Duration};

use message_encoding::MessageEncoding;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestStore {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
enum TestAction {
    Set { key: String, value: String },
    Delete { key: String },
}

impl DeterministicState for TestStore {
    type Action = TestAction;
    type AuthorityAction = TestAction;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            TestAction::Set { key, value } => {
                self.values.insert(key.clone(), value.clone());
            }
            TestAction::Delete { key } => {
                self.values.remove(key);
            }
        }
        self.seq += 1;
    }
}

impl MessageEncoding for TestAction {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
            Self::Delete { key } => {
                sum += 2u16.write_to(out)?;
                key.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(Self::Delete {
                key: MessageEncoding::read_from(read)?,
            }),
            id => Err(crate::utils::unknown_id_err(id, "TestAction")),
        }
    }
}

impl MessageEncoding for TestStore {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

fn orchestrator() -> SharedStateTestOrchestrator<TestStore> {
    SharedStateTestOrchestrator::new(config())
}

fn config() -> SharedStateTestOrchestratorConfig<TestStore> {
    SharedStateTestOrchestratorConfig {
        io_settings: NetIoSettings::default(),
        initial_state: Arc::new(|address| {
            RecoverableState::new(
                address,
                TestStore {
                    seq: 1,
                    values: BTreeMap::new(),
                },
            )
        }),
    }
}

fn all_online_nodes_agree_on_one_online_leader(snapshot: &OrchestratorSnapshot<TestStore>) -> bool {
    let online = snapshot.nodes.iter().filter(|node| node.online).collect::<Vec<_>>();
    if online.is_empty() {
        return false;
    }

    let Some(leader) = online[0].leader else {
        return false;
    };

    if online.iter().any(|node| node.leader != Some(leader)) {
        return false;
    }

    let Some(leader_node) = online.iter().find(|node| node.address == leader) else {
        return false;
    };

    if !leader_node.can_lead
        || leader_node.leader != Some(leader)
        || leader_node.leader_path.as_deref() != Some(&[leader])
        || leader_node.follow_remote.is_some()
    {
        return false;
    }

    online.iter().all(|node| {
        let Some(path) = node.leader_path.as_ref() else {
            return false;
        };
        !path.is_empty()
            && path.first() == Some(&leader)
            && path.iter().collect::<std::collections::BTreeSet<_>>().len() == path.len()
    })
}

fn ready_to_stop_original_leader_for_indirect_recording(snapshot: &OrchestratorSnapshot<TestStore>) -> bool {
    let nodes = snapshot
        .nodes
        .iter()
        .map(|node| (node.address, node))
        .collect::<BTreeMap<_, _>>();
    let Some(node1) = nodes.get(&1) else {
        return false;
    };
    if !node1.online || node1.leader != Some(1) || node1.leader_path.as_deref() != Some(&[1]) {
        return false;
    }

    for address in 2..=8 {
        let Some(node) = nodes.get(&address) else {
            return false;
        };
        if !node.online || node.leader != Some(1) {
            return false;
        }
        let Some(path) = node.leader_path.as_ref() else {
            return false;
        };
        if path.is_empty()
            || path.first() != Some(&1)
            || path.last() != Some(&address)
            || path.iter().collect::<std::collections::BTreeSet<_>>().len() != path.len()
        {
            return false;
        }
    }

    nodes.get(&4).is_some_and(|node| {
        node.can_lead && node.follow_remote.is_some() && node.leader_path.as_ref().is_some_and(|path| path.contains(&5))
    }) && nodes.get(&5).is_some_and(|node| {
        node.can_lead && node.follow_remote == Some(1) && node.leader_path.as_deref() == Some(&[1, 5])
    })
}

fn leader_return_cluster_accepts_node2(snapshot: &OrchestratorSnapshot<TestStore>) -> bool {
    let nodes = snapshot
        .nodes
        .iter()
        .map(|node| (node.address, node))
        .collect::<BTreeMap<_, _>>();

    let (Some(node1), Some(node2), Some(node3)) = (nodes.get(&1), nodes.get(&2), nodes.get(&3)) else {
        return false;
    };

    if !node1.online
        || !node2.online
        || !node3.online
        || node1.networking_disabled
        || snapshot
            .nodes
            .iter()
            .filter(|node| node.online)
            .any(|node| node.leader != Some(2))
    {
        return false;
    }

    if !node2.can_lead
        || node2.status != NodeStatus::Leading
        || node2.leader_path.as_deref() != Some(&[2])
        || node2.follow_remote.is_some()
        || node2.term.is_none_or(|term| term < 2)
    {
        return false;
    }

    for node in [node1, node3] {
        let Some(path) = node.leader_path.as_ref() else {
            return false;
        };
        if path.is_empty()
            || path.first() != Some(&2)
            || path.iter().collect::<std::collections::BTreeSet<_>>().len() != path.len()
        {
            return false;
        }
    }

    snapshot.nodes.iter().filter(|node| node.online).all(|node| {
        let Some(state) = &node.state else {
            return false;
        };
        state.seq >= 5
            && state.values.get("1").is_some_and(|value| value == "2")
            && state.values.get("2").is_some_and(|value| value == "22")
            && state.values.get("3").is_some_and(|value| value == "4")
    })
}

async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if deadline <= tokio::time::Instant::now() {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_value(
    orchestrator: &SharedStateTestOrchestrator<TestStore>,
    address: u64,
    key: &str,
    expected: &str,
) -> bool {
    wait_until(Duration::from_secs(3), || async {
        orchestrator
            .node_details(address)
            .await
            .ok()
            .and_then(|node| node.state)
            .and_then(|state| state.values.get(key).cloned())
            .as_deref()
            == Some(expected)
    })
    .await
}

#[tokio::test]
async fn records_add_node_before_applying() {
    let orchestrator = orchestrator();

    orchestrator
        .add_node(AddNodeRequest {
            address: 7001,
            can_lead: true,
            peers: vec![],
        })
        .await
        .unwrap();

    let recording = orchestrator.recording().await;
    assert_eq!(recording.events.len(), 1);
    assert!(matches!(recording.events[0].action, OrchestratorAction::AddNode { address: 7001, .. }));
    assert!(recording.events[0].before.nodes.is_empty());
}

#[tokio::test]
async fn records_network_mutations() {
    let orchestrator = orchestrator();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7001,
            can_lead: true,
            peers: vec![7002],
        })
        .await
        .unwrap();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7002,
            can_lead: false,
            peers: vec![7001],
        })
        .await
        .unwrap();

    orchestrator
        .set_networking(7002, SetNetworkingRequest { disabled: true })
        .await
        .unwrap();
    orchestrator
        .set_blocked_peers(
            7001,
            SetBlockedPeersRequest {
                blocked_peers: vec![7002],
            },
        )
        .await
        .unwrap();

    let actions = orchestrator
        .recording()
        .await
        .events
        .into_iter()
        .map(|event| event.action)
        .collect::<Vec<_>>();
    assert!(actions.iter().any(|action| matches!(
        action,
        OrchestratorAction::SetNetworking {
            address: 7002,
            disabled: true
        }
    )));
    assert!(actions.iter().any(|action| {
        matches!(
            action,
            OrchestratorAction::SetBlockedPeers {
                address: 7001,
                blocked_peers
            } if blocked_peers == &vec![7002]
        )
    }));
}

#[tokio::test]
async fn records_send_action() {
    let orchestrator = orchestrator();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7001,
            can_lead: true,
            peers: vec![],
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || async {
            orchestrator.node_details(7001).await.unwrap().leader == Some(7001)
        })
        .await
    );

    orchestrator
        .send_action(
            7001,
            TestAction::Set {
                key: "alpha".to_owned(),
                value: "one".to_owned(),
            },
        )
        .await
        .unwrap();

    let recording = orchestrator.recording().await;
    let event = recording.events.last().unwrap();
    assert!(matches!(event.action, OrchestratorAction::SendAction { address: 7001, .. }));
    let state = event
        .before
        .nodes
        .iter()
        .find(|node| node.address == 7001)
        .and_then(|node| node.state.as_ref())
        .unwrap();
    assert!(!state.values.contains_key("alpha"));
}

#[tokio::test]
async fn replay_applies_recorded_history() {
    let orchestrator = orchestrator();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7001,
            can_lead: true,
            peers: vec![7002],
        })
        .await
        .unwrap();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7002,
            can_lead: false,
            peers: vec![7001],
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || async {
            orchestrator.node_details(7002).await.unwrap().leader == Some(7001)
        })
        .await
    );
    orchestrator
        .send_action(
            7002,
            TestAction::Set {
                key: "bug".to_owned(),
                value: "found".to_owned(),
            },
        )
        .await
        .unwrap();
    assert!(wait_for_value(&orchestrator, 7002, "bug", "found").await);
    orchestrator
        .set_networking(7002, SetNetworkingRequest { disabled: true })
        .await
        .unwrap();

    let recording = orchestrator.recording().await;
    let original_checkpoint = checkpoint_from_snapshot(&orchestrator.snapshot().await);
    let report = replay_recording(config(), recording, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!(report.applied_events, 4);
    assert_eq!(checkpoint_from_snapshot(&report.final_snapshot), original_checkpoint);
}

#[tokio::test]
async fn replay_waits_for_checkpoint_before_next_action() {
    let orchestrator = orchestrator();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7001,
            can_lead: true,
            peers: vec![7002],
        })
        .await
        .unwrap();
    orchestrator
        .add_node(AddNodeRequest {
            address: 7002,
            can_lead: false,
            peers: vec![7001],
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || async {
            orchestrator.node_details(7002).await.unwrap().leader == Some(7001)
        })
        .await
    );
    orchestrator.stop_node(7001).await.unwrap();

    let report = replay_recording(config(), orchestrator.recording().await, ReplayOptions::default())
        .await
        .unwrap();
    assert_eq!(report.applied_events, 3);
    assert!(report
        .final_snapshot
        .nodes
        .iter()
        .find(|node| node.address == 7001)
        .is_some_and(|node| !node.online));
}

#[tokio::test]
async fn failed_mutation_is_not_recorded() {
    let orchestrator = orchestrator();

    assert!(orchestrator.stop_node(7001).await.is_err());
    assert!(orchestrator.recording().await.events.is_empty());
}

#[tokio::test]
async fn replay_new_leader_indirect_converges_after_original_leader_stops() {
    let recording: OrchestratorRecording<TestStore> =
        serde_json::from_str(include_str!("../res/new-leader-indirect.json")).unwrap();
    let orchestrator = SharedStateTestOrchestrator::new(config());
    let options = ReplayOptions::default();
    let event_count = recording.events.len();

    for (event_index, event) in recording.events.into_iter().enumerate() {
        if event_index + 1 == event_count {
            assert!(
                wait_until(Duration::from_secs(5), || async {
                    ready_to_stop_original_leader_for_indirect_recording(&orchestrator.snapshot().await)
                })
                .await,
                "{:#?}",
                orchestrator.snapshot().await
            );
        } else {
            wait_for_checkpoint(&orchestrator, &event.before_checkpoint, event_index, &options)
                .await
                .unwrap();
        }
        orchestrator.apply_action(event.action, false).await.unwrap();
    }

    let converged = wait_until(Duration::from_secs(5), || async {
        all_online_nodes_agree_on_one_online_leader(&orchestrator.snapshot().await)
    })
    .await;

    assert!(converged, "{:#?}", orchestrator.snapshot().await);
}

#[tokio::test]
async fn replay_leader_return_keeps_newer_healthy_leader() {
    let recording: OrchestratorRecording<TestStore> =
        serde_json::from_str(include_str!("../res/leader-return.json")).unwrap();
    let orchestrator = SharedStateTestOrchestrator::new(config());
    let options = ReplayOptions::default();

    for (event_index, event) in recording.events.into_iter().enumerate() {
        wait_for_checkpoint(&orchestrator, &event.before_checkpoint, event_index, &options)
            .await
            .unwrap();
        orchestrator.apply_action(event.action, false).await.unwrap();
    }

    let converged = wait_until(Duration::from_secs(5), || async {
        leader_return_cluster_accepts_node2(&orchestrator.snapshot().await)
    })
    .await;

    assert!(converged, "{:#?}", orchestrator.snapshot().await);
}
