import type { AddNodeRequest, NodeView, SnapshotResponse } from "./types";

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: {
      "content-type": "application/json",
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: string };
      message = body.error ?? message;
    } catch {
      // Keep the HTTP status text if the body is not JSON.
    }
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function getSnapshot(): Promise<SnapshotResponse> {
  return request<SnapshotResponse>("/api/snapshot");
}

export function addNode(body: AddNodeRequest): Promise<NodeView> {
  return request<NodeView>("/api/nodes", {
    method: "POST",
    body: JSON.stringify(body)
  });
}

export function startNode(address: number): Promise<void> {
  return request<void>(`/api/nodes/${address}/start`, { method: "POST" });
}

export function stopNode(address: number): Promise<void> {
  return request<void>(`/api/nodes/${address}/stop`, { method: "POST" });
}

export function setNetworking(address: number, disabled: boolean): Promise<void> {
  return request<void>(`/api/nodes/${address}/networking`, {
    method: "POST",
    body: JSON.stringify({ disabled })
  });
}

export function setBlockedPeers(address: number, blockedPeers: number[]): Promise<void> {
  return request<void>(`/api/nodes/${address}/blocked-peers`, {
    method: "POST",
    body: JSON.stringify({ blocked_peers: blockedPeers })
  });
}

export function setKey(address: number, key: string, value: string): Promise<void> {
  return request<void>(`/api/nodes/${address}/actions/set`, {
    method: "POST",
    body: JSON.stringify({ key, value })
  });
}

export function deleteKey(address: number, key: string): Promise<void> {
  return request<void>(`/api/nodes/${address}/actions/delete`, {
    method: "POST",
    body: JSON.stringify({ key })
  });
}
