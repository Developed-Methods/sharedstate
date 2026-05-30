import { useCallback, useEffect, useMemo, useState } from "react";
import { addNode, getRecording, getSnapshot } from "./api";
import { AddNodeModal } from "./components/AddNodeModal";
import { ClusterGraph } from "./components/ClusterGraph";
import { NodePanel } from "./components/NodePanel";
import { TopHeader } from "./components/TopHeader";
import type { AddNodeRequest, SnapshotResponse } from "./types";

export default function App() {
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [selectedAddress, setSelectedAddress] = useState<number | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [recordingError, setRecordingError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const next = await getSnapshot();
      setSnapshot(next);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 750);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const selectedNode = useMemo(
    () => snapshot?.nodes.find((node) => node.address === selectedAddress) ?? null,
    [snapshot, selectedAddress]
  );

  async function createNode(request: AddNodeRequest) {
    await addNode(request);
    await refresh();
    setSelectedAddress(request.address);
  }

  async function saveRecording() {
    setRecordingError(null);
    try {
      const recording = await getRecording();
      const body = JSON.stringify(recording, null, 2);
      const blob = new Blob([body], { type: "application/json" });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..+/, "").replace("T", "-");
      link.href = url;
      link.download = `sharedstate-recording-${stamp}.json`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setRecordingError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <div className="app-shell">
      <TopHeader
        snapshot={snapshot}
        onAddNode={() => setModalOpen(true)}
        onSaveRecording={saveRecording}
        recordingError={recordingError}
      />
      {error && <div className="global-error">{error}</div>}
      <main className={`main-grid ${selectedNode ? "with-panel" : ""}`}>
        <ClusterGraph
          nodes={snapshot?.nodes ?? []}
          connections={snapshot?.connections ?? []}
          selectedAddress={selectedAddress}
          onSelect={setSelectedAddress}
        />
        <NodePanel node={selectedNode} onChanged={refresh} />
      </main>
      {modalOpen && <AddNodeModal onClose={() => setModalOpen(false)} onSubmit={createNode} />}
    </div>
  );
}
