import { Plus, RadioTower } from "lucide-react";
import type { SnapshotResponse } from "../types";

interface Props {
  snapshot: SnapshotResponse | null;
  onAddNode: () => void;
}

export function TopHeader({ snapshot, onAddNode }: Props) {
  const nodes = snapshot?.nodes ?? [];
  const online = nodes.filter((node) => node.online).length;
  const leaders = nodes.filter((node) => node.status === "leading").length;
  const isolated = nodes.filter((node) => node.status === "isolated").length;

  return (
    <header className="top-header">
      <div className="brand-mark">
        <RadioTower size={22} />
        <div>
          <h1>Sharedstate Cluster Lab</h1>
          <span>in-process network simulator</span>
        </div>
      </div>
      <div className="cluster-strip">
        <span>{nodes.length} nodes</span>
        <span>{online} online</span>
        <span>{leaders} leaders</span>
        <span>{isolated} isolated</span>
      </div>
      <button className="primary-button" onClick={onAddNode}>
        <Plus size={17} />
        Add Node
      </button>
    </header>
  );
}
