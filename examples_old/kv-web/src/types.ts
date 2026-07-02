export type NodeStatus = "offline" | "isolated" | "noLeader" | "leading" | "connected";

export interface KvStore {
  seq: number;
  values: Record<string, string>;
}

export interface SnapshotResponse {
  nodes: NodeView[];
  connections: ConnectionView[];
  topology: SimulatedTopologySnapshot;
}

export interface OrchestratorRecording {
  version: number;
  events: RecordedOrchestratorEvent[];
}

export interface RecordedOrchestratorEvent {
  before: SnapshotResponse;
  before_checkpoint: ReplayCheckpoint;
  action: OrchestratorAction;
}

export type OrchestratorAction =
  | { type: "addNode"; address: number; can_lead: boolean; peers: number[] }
  | { type: "startNode"; address: number }
  | { type: "stopNode"; address: number }
  | { type: "setNetworking"; address: number; disabled: boolean }
  | { type: "setBlockedPeers"; address: number; blocked_peers: number[] }
  | { type: "sendAction"; address: number; action: KvAction };

export type KvAction =
  | { type: "set"; key: string; value: string }
  | { type: "delete"; key: string };

export interface ReplayCheckpoint {
  nodes: ReplayNodeCheckpoint[];
  topology: ReplayTopologyCheckpoint;
}

export interface ReplayNodeCheckpoint {
  address: number;
  can_lead: boolean;
  online: boolean;
  networking_disabled: boolean;
  blocked_peers: number[];
  known_peers: number[];
  leader: number | null;
  leader_path: number[] | null;
  follow_remote: number | null;
  follow_leader_path: number[] | null;
  known_can_lead: number[];
  status: NodeStatus;
  state: KvStore | null;
}

export interface ReplayTopologyCheckpoint {
  online: number[];
  blocked_nodes: number[];
  blocked_edges: [number, number][];
}

export interface SimulatedTopologySnapshot {
  online: number[];
  blocked_nodes: number[];
  blocked_edges: [number, number][];
}

export interface NodeView {
  address: number;
  can_lead: boolean;
  online: boolean;
  networking_disabled: boolean;
  blocked_peers: number[];
  known_peers: number[];
  leader: number | null;
  leader_path: number[] | null;
  term: number | null;
  follow_remote: number | null;
  follow_leader_path: number[] | null;
  known_can_lead: number[];
  last_promoted_leader: number | null;
  status: NodeStatus;
  state: KvStore | null;
  debug: NodeDebugInfo | null;
}

export interface ConnectionView {
  source: number;
  target: number;
  blocked: boolean;
}

export interface NodeDebugInfo {
  address: number;
  can_lead: boolean;
  leader: number | null;
  leader_path: number[] | null;
  term: number;
  follow_remote: number | null;
  follow_leader_path: number[] | null;
  known_can_lead: number[];
  last_promoted_leader: number | null;
  observations: ElectionObservation[];
  peers: PeerDebugInfo[];
}

export interface PeerDebugInfo {
  address: number;
  known: boolean;
  can_lead: boolean | null;
  connected: boolean | null;
  latency_ms: number | null;
  repeat_connect_fails: number | null;
  last_activity_ms_ago: number | null;
  last_global_activity_ms_ago: number | null;
  last_connect_attempt_ms_ago: number | null;
  last_connect_fail_ms_ago: number | null;
  observed_leader: number | null;
  observed_term: number | null;
  observed_leader_path: number[] | null;
  observed_reachable_can_lead: number[] | null;
}

export interface ElectionObservation {
  observer: number;
  term: number;
  leader: number | null;
  leader_path: number[] | null;
  can_lead: boolean;
  reachable_can_lead: number[];
  state_accept_seq?: number;
}

export interface AddNodeRequest {
  address: number;
  can_lead: boolean;
  peers: number[];
}
