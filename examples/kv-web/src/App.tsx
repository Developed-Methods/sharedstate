import { useCallback, useEffect, useMemo, useState } from "react";
import { addNode, getSnapshot } from "./api";
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

  return (
    <div className="app-shell">
      <TopHeader snapshot={snapshot} onAddNode={() => setModalOpen(true)} />
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
