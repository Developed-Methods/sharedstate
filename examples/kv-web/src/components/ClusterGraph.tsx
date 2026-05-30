import ReactFlow, { Background, Controls, Edge, Node, Position } from "reactflow";
import type { ConnectionView, NodeView } from "../types";

interface Props {
  nodes: NodeView[];
  connections: ConnectionView[];
  selectedAddress: number | null;
  onSelect: (address: number | null) => void;
}

const statusLabels: Record<string, string> = {
  offline: "offline",
  isolated: "isolated",
  noLeader: "no leader",
  leading: "leading",
  connected: "connected"
};

function layout(index: number, count: number) {
  const radius = Math.max(180, Math.min(360, count * 54));
  const angle = count <= 1 ? 0 : (Math.PI * 2 * index) / count - Math.PI / 2;
  return {
    x: Math.cos(angle) * radius + 430,
    y: Math.sin(angle) * radius + 310
  };
}

export function ClusterGraph({ nodes, connections, selectedAddress, onSelect }: Props) {
  const flowNodes: Node[] = nodes.map((node, index) => ({
    id: String(node.address),
    type: "default",
    position: layout(index, nodes.length),
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    className: `flow-node status-${node.status} ${selectedAddress === node.address ? "selected-node" : ""}`,
    data: {
      label: (
        <div className="node-shell">
          <div className="node-address">{node.address}</div>
          <div className="node-line">can_lead: {node.can_lead ? "yes" : "no"}</div>
          <div className="node-line">leader: {node.leader ?? "none"}</div>
          <div className="node-status">{statusLabels[node.status]}</div>
        </div>
      )
    }
  }));

  const flowEdges: Edge[] = connections.map((connection) => ({
    id: `${connection.source}-${connection.target}`,
    source: String(connection.source),
    target: String(connection.target),
    animated: !connection.blocked,
    className: connection.blocked ? "blocked-edge" : "active-edge",
    label: connection.blocked ? "blocked" : undefined
  }));

  return (
    <div className="graph-surface">
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        fitView
        minZoom={0.25}
        maxZoom={1.7}
        onNodeClick={(_, node) => onSelect(Number(node.id))}
        onPaneClick={() => onSelect(null)}
      >
        <Background color="#27313a" gap={22} size={1} />
        <Controls />
      </ReactFlow>
      {nodes.length === 0 && <div className="empty-graph">Add a node to start the cluster lab.</div>}
    </div>
  );
}
