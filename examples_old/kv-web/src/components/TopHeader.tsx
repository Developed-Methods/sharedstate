import { Download, Plus, RadioTower } from "lucide-react";
import type { SnapshotResponse } from "../types";

interface Props {
  snapshot: SnapshotResponse | null;
  onAddNode: () => void;
  onSaveRecording: () => void;
  recordingError: string | null;
}

export function TopHeader({ snapshot, onAddNode, onSaveRecording, recordingError }: Props) {
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
      {recordingError && <div className="header-error">{recordingError}</div>}
      <div className="header-actions">
        <button className="secondary-button" onClick={onSaveRecording}>
          <Download size={17} />
          Save Recording
        </button>
        <button className="primary-button" onClick={onAddNode}>
          <Plus size={17} />
          Add Node
        </button>
      </div>
    </header>
  );
}
