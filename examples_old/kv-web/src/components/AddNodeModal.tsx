import { X } from "lucide-react";
import { FormEvent, useState } from "react";
import type { AddNodeRequest } from "../types";

interface Props {
  onClose: () => void;
  onSubmit: (request: AddNodeRequest) => Promise<void>;
}

function parsePeers(value: string): number[] {
  if (!value.trim()) {
    return [];
  }
  return value
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

export function AddNodeModal({ onClose, onSubmit }: Props) {
  const [address, setAddress] = useState("");
  const [peers, setPeers] = useState("");
  const [canLead, setCanLead] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError(null);

    const parsedAddress = Number(address);
    if (!Number.isSafeInteger(parsedAddress) || parsedAddress <= 0) {
      setError("address must be a positive integer");
      return;
    }

    try {
      setSubmitting(true);
      await onSubmit({
        address: parsedAddress,
        can_lead: canLead,
        peers: parsePeers(peers)
      });
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true">
      <form className="modal" onSubmit={submit}>
        <div className="modal-title">
          <h2>Add Node</h2>
          <button type="button" className="icon-button" onClick={onClose} aria-label="Close">
            <X size={18} />
          </button>
        </div>
        <label>
          Address
          <input value={address} onChange={(event) => setAddress(event.target.value)} placeholder="7001" />
        </label>
        <label>
          Known peers
          <input value={peers} onChange={(event) => setPeers(event.target.value)} placeholder="7001,7002" />
        </label>
        <label className="checkbox-row">
          <input type="checkbox" checked={canLead} onChange={(event) => setCanLead(event.target.checked)} />
          can_lead
        </label>
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button type="button" className="secondary-button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary-button" disabled={submitting}>
            Create
          </button>
        </div>
      </form>
    </div>
  );
}
