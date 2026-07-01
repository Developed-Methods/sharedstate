use std::{fmt, time::Duration};

use message_encoding::MessageEncoding;
use serde::{Deserialize, Serialize};

use crate::state::deterministic::DeterministicState;

use super::{
    NodeStatus, OrchestratorError, OrchestratorSnapshot, SharedStateTestOrchestrator, SharedStateTestOrchestratorConfig,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "D: Serialize, D::Action: Serialize",
    deserialize = "D: DeterministicState + Deserialize<'de>, D::Action: Deserialize<'de>"
))]
pub struct OrchestratorRecording<D: DeterministicState>
where
    D::Action: Clone + fmt::Debug,
{
    pub version: u64,
    pub events: Vec<RecordedOrchestratorEvent<D>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "D: Serialize, D::Action: Serialize",
    deserialize = "D: DeterministicState + Deserialize<'de>, D::Action: Deserialize<'de>"
))]
pub struct RecordedOrchestratorEvent<D: DeterministicState>
where
    D::Action: Clone + fmt::Debug,
{
    pub before: OrchestratorSnapshot<D>,
    pub before_checkpoint: ReplayCheckpoint<D>,
    pub action: OrchestratorAction<D::Action>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OrchestratorAction<Action> {
    AddNode {
        address: u64,
        can_lead: bool,
        peers: Vec<u64>,
    },
    StartNode {
        address: u64,
    },
    StopNode {
        address: u64,
    },
    SetNetworking {
        address: u64,
        disabled: bool,
    },
    SetBlockedPeers {
        address: u64,
        blocked_peers: Vec<u64>,
    },
    SendAction {
        address: u64,
        action: Action,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct ReplayCheckpoint<D: DeterministicState> {
    pub nodes: Vec<ReplayNodeCheckpoint<D>>,
    pub topology: ReplayTopologyCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct ReplayNodeCheckpoint<D: DeterministicState> {
    pub address: u64,
    pub can_lead: bool,
    pub online: bool,
    pub networking_disabled: bool,
    pub blocked_peers: Vec<u64>,
    pub known_peers: Vec<u64>,
    pub leader: Option<u64>,
    pub leader_path: Option<Vec<u64>>,
    pub follow_remote: Option<u64>,
    pub follow_leader_path: Option<Vec<u64>>,
    pub known_can_lead: Vec<u64>,
    pub status: NodeStatus,
    pub state: Option<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayTopologyCheckpoint {
    pub online: Vec<u64>,
    pub blocked_nodes: Vec<u64>,
    pub blocked_edges: Vec<(u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct ReplayOptions {
    pub settle_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            settle_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplayReport<D: DeterministicState> {
    pub applied_events: usize,
    pub final_snapshot: OrchestratorSnapshot<D>,
}
#[derive(Debug)]
pub enum ReplayError<D: DeterministicState> {
    Orchestrator(OrchestratorError),
    CheckpointTimeout {
        event_index: usize,
        expected: Box<ReplayCheckpoint<D>>,
        actual: Box<ReplayCheckpoint<D>>,
    },
}

impl<D: DeterministicState> From<OrchestratorError> for ReplayError<D> {
    fn from(value: OrchestratorError) -> Self {
        Self::Orchestrator(value)
    }
}
pub async fn replay_recording<D>(
    config: SharedStateTestOrchestratorConfig<D>,
    recording: OrchestratorRecording<D>,
    options: ReplayOptions,
) -> Result<ReplayReport<D>, ReplayError<D>>
where
    D: DeterministicState + MessageEncoding + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::Action: MessageEncoding + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::AuthorityAction: MessageEncoding,
{
    let orchestrator = SharedStateTestOrchestrator::new(config);
    let event_count = recording.events.len();

    for (event_index, event) in recording.events.into_iter().enumerate() {
        wait_for_checkpoint(&orchestrator, &event.before_checkpoint, event_index, &options).await?;
        orchestrator.apply_action(event.action, false).await?;
    }

    let final_snapshot = orchestrator.snapshot().await;
    Ok(ReplayReport {
        applied_events: event_count,
        final_snapshot,
    })
}

pub(crate) async fn wait_for_checkpoint<D>(
    orchestrator: &SharedStateTestOrchestrator<D>,
    expected: &ReplayCheckpoint<D>,
    event_index: usize,
    options: &ReplayOptions,
) -> Result<(), ReplayError<D>>
where
    D: DeterministicState + MessageEncoding + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::Action: MessageEncoding + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::AuthorityAction: MessageEncoding,
{
    let deadline = tokio::time::Instant::now() + options.settle_timeout;
    loop {
        let snapshot = orchestrator.snapshot().await;
        let actual = checkpoint_from_snapshot(&snapshot);
        if &actual == expected {
            return Ok(());
        }
        if deadline <= tokio::time::Instant::now() {
            return Err(ReplayError::CheckpointTimeout {
                event_index,
                expected: Box::new(expected.clone()),
                actual: Box::new(actual),
            });
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

pub fn checkpoint_from_snapshot<D>(snapshot: &OrchestratorSnapshot<D>) -> ReplayCheckpoint<D>
where
    D: DeterministicState + Clone,
{
    ReplayCheckpoint {
        nodes: snapshot
            .nodes
            .iter()
            .map(|node| ReplayNodeCheckpoint {
                address: node.address,
                can_lead: node.can_lead,
                online: node.online,
                networking_disabled: node.networking_disabled,
                blocked_peers: node.blocked_peers.clone(),
                known_peers: node.known_peers.clone(),
                leader: node.leader,
                leader_path: node.leader_path.clone(),
                follow_remote: node.follow_remote,
                follow_leader_path: node.follow_leader_path.clone(),
                known_can_lead: node.known_can_lead.clone(),
                status: node.status,
                state: node.state.clone(),
            })
            .collect(),
        topology: ReplayTopologyCheckpoint {
            online: snapshot.topology.online.iter().copied().collect(),
            blocked_nodes: snapshot.topology.blocked_nodes.iter().copied().collect(),
            blocked_edges: snapshot.topology.blocked_edges.iter().copied().collect(),
        },
    }
}
