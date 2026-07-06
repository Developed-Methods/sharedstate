import { PlugZap, Power, Save, Send, Trash2 } from "lucide-react";
import { FormEvent, useEffect, useState } from "react";
import { deleteKey, setBlockedPeers, setKey, setNetworking, startNode, stopNode } from "../api";
import type { NodeView } from "../types";

interface Props {
  node: NodeView | null;
  onChanged: () => Promise<void>;
}

function list(values: number[] | null | undefined) {
  return values && values.length > 0 ? values.join(", ") : "none";
}

export function NodePanel({ node, onChanged }: Props) {
  const [blockedPeers, setBlockedPeersInput] = useState("");
  const [setKeyName, setSetKeyName] = useState("");
  const [setValue, setSetValue] = useState("");
  const [deleteKeyName, setDeleteKeyName] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setBlockedPeersInput(node?.blocked_peers.join(",") ?? "");
    setError(null);
  }, [node?.address]);

  if (!node) {
    return (
      <aside className="context-panel empty-panel">
        <h2>Node Details</h2>
        <p>Select a node in the graph to inspect its election state, network controls, and key/value data.</p>
      </aside>
    );
  }

  async function run(action: () => Promise<void>) {
    setError(null);
    try {
      await action();
      await onChanged();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  function parseBlockedPeers() {
    if (!blockedPeers.trim()) {
      return [];
    }
    return blockedPeers
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const parsed = Number(part);
        if (!Number.isSafeInteger(parsed) || parsed <= 0) {
          throw new Error(`invalid peer address: ${part}`);
        }
        return parsed;
      });
  }

  async function submitSet(event: FormEvent) {
    event.preventDefault();
    if (!setKeyName.trim()) {
      setError("set requires a key");
      return;
    }
    await run(async () => {
      await setKey(node.address, setKeyName.trim(), setValue);
      setSetKeyName("");
      setSetValue("");
    });
  }

  async function submitDelete(event: FormEvent) {
    event.preventDefault();
    if (!deleteKeyName.trim()) {
      setError("delete requires a key");
      return;
    }
    await run(async () => {
      await deleteKey(node.address, deleteKeyName.trim());
      setDeleteKeyName("");
    });
  }

  const values = Object.entries(node.state?.values ?? {});

  return (
    <aside className="context-panel">
      <div className="panel-heading">
        <div>
          <h2>{node.address}</h2>
          <span className={`status-pill status-${node.status}`}>{node.status}</span>
        </div>
      </div>

      {error && <div className="form-error">{error}</div>}

      <section className="panel-section action-grid">
        <button
          className="secondary-button"
          onClick={() => run(() => (node.online ? stopNode(node.address) : startNode(node.address)))}
        >
          <Power size={16} />
          {node.online ? "Turn Off" : "Turn On"}
        </button>
        <button
          className="secondary-button"
          disabled={!node.online}
          onClick={() => run(() => setNetworking(node.address, !node.networking_disabled))}
        >
          <PlugZap size={16} />
          {node.networking_disabled ? "Enable Networking" : "Disable Networking"}
        </button>
      </section>

      <section className="panel-section facts">
        <div><span>can_lead</span><b>{node.can_lead ? "yes" : "no"}</b></div>
        <div><span>leader</span><b>{node.leader ?? "none"}</b></div>
        <div><span>term</span><b>{node.term ?? "none"}</b></div>
        <div><span>follow remote</span><b>{node.follow_remote ?? "none"}</b></div>
        <div><span>leader path</span><b>{list(node.leader_path)}</b></div>
        <div><span>known peers</span><b>{list(node.known_peers)}</b></div>
        <div><span>known can_lead</span><b>{list(node.known_can_lead)}</b></div>
      </section>

      <section className="panel-section">
        <label>
          Blocked peers
          <input value={blockedPeers} onChange={(event) => setBlockedPeersInput(event.target.value)} placeholder="7002,7003" />
        </label>
        <button
          className="secondary-button full-width"
          onClick={() => run(() => setBlockedPeers(node.address, parseBlockedPeers()))}
        >
          <Save size={16} />
          Save Blocked Peers
        </button>
      </section>

      <section className="panel-section">
        <h3>Key Value State</h3>
        <div className="kv-table">
          {values.length === 0 && <div className="empty-row">empty</div>}
          {values.map(([key, value]) => (
            <div className="kv-row" key={key}>
              <span>{key}</span>
              <b>{value}</b>
            </div>
          ))}
        </div>
      </section>

      <form className="panel-section compact-form" onSubmit={submitSet}>
        <h3>Set</h3>
        <input value={setKeyName} onChange={(event) => setSetKeyName(event.target.value)} placeholder="key" />
        <input value={setValue} onChange={(event) => setSetValue(event.target.value)} placeholder="value" />
        <button className="primary-button" disabled={!node.online}>
          <Send size={16} />
          Send
        </button>
      </form>

      <form className="panel-section compact-form" onSubmit={submitDelete}>
        <h3>Delete</h3>
        <input value={deleteKeyName} onChange={(event) => setDeleteKeyName(event.target.value)} placeholder="key" />
        <button className="danger-button" disabled={!node.online}>
          <Trash2 size={16} />
          Delete
        </button>
      </form>

      <section className="panel-section">
        <h3>Peers</h3>
        <div className="peer-list">
          {node.debug?.peers.map((peer) => (
            <div className="peer-row" key={peer.address}>
              <b>{peer.address}</b>
              <span>{peer.connected ? "connected" : "not connected"}</span>
              <span>leader {peer.observed_leader ?? "none"}</span>
            </div>
          )) ?? <div className="empty-row">offline</div>}
        </div>
      </section>
    </aside>
  );
}
