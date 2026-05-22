let snapshot = null;
let selectedNodeId = 1;
let selectedTab = "summary";

const nodesBody = document.getElementById("nodes-body");
const actionNode = document.getElementById("action-node");
const detailTitle = document.getElementById("detail-title");
const detailJson = document.getElementById("detail-json");
const linksGrid = document.getElementById("links-grid");
const apiMessage = document.getElementById("api-message");
const clusterMeta = document.getElementById("cluster-meta");
const pollState = document.getElementById("poll-state");

document.addEventListener("click", (event) => {
  const command = event.target?.dataset?.command;
  if (command) runCommand(command);

  const tab = event.target?.dataset?.tab;
  if (tab) {
    selectedTab = tab;
    renderDetail();
  }
});

document.getElementById("action-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const type = form.get("type");
  const node = Number(form.get("node"));
  let action;

  if (type === "AppendLog") {
    action = { type, label: String(form.get("label") || "") };
  } else {
    action = {
      type,
      slot: Number(form.get("slot")),
      value: Number(form.get("value")),
    };
  }

  await api("/api/actions", {
    method: "POST",
    body: JSON.stringify({ node, action }),
  });
  await refresh();
});

async function runCommand(command) {
  const routes = {
    reset: ["/api/reset", {}],
    "start-all": ["/api/nodes/start-all", {}],
    "stop-all": ["/api/nodes/stop-all", {}],
    "network-all-on": ["/api/nodes/network", { enabled: true }],
    "network-all-off": ["/api/nodes/network", { enabled: false }],
  };
  const [url, body] = routes[command];
  await api(url, { method: "POST", body: JSON.stringify(body) });
  await refresh();
}

async function api(url, options = {}) {
  const response = await fetch(url, {
    headers: { "content-type": "application/json" },
    ...options,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) {
    apiMessage.textContent = data.error || response.statusText;
    throw new Error(apiMessage.textContent);
  }
  apiMessage.textContent = "ok";
  return data;
}

async function refresh() {
  try {
    const response = await fetch("/api/status");
    snapshot = await response.json();
    if (!snapshot.nodes.some((node) => node.id === selectedNodeId)) {
      selectedNodeId = snapshot.nodes[0]?.id ?? 1;
    }
    render();
    pollState.textContent = new Date().toLocaleTimeString();
  } catch (error) {
    pollState.textContent = String(error.message || error);
  }
}

function render() {
  clusterMeta.textContent = `master_guess=${snapshot.current_master_guess} sentinels=${snapshot.sentinels.join(",")} peers=${snapshot.regular_peers.join(",")}`;
  renderNodeOptions();
  renderNodes();
  renderLinks();
  renderDetail();
}

function renderNodeOptions() {
  const current = actionNode.value || String(selectedNodeId);
  actionNode.innerHTML = snapshot.nodes
    .map((node) => `<option value="${node.id}">node ${node.id}</option>`)
    .join("");
  actionNode.value = current;
}

function renderNodes() {
  nodesBody.innerHTML = "";
  for (const node of snapshot.nodes) {
    const row = document.createElement("tr");
    row.className = `node-row ${node.id === selectedNodeId ? "selected" : ""}`;
    row.addEventListener("click", () => {
      selectedNodeId = node.id;
      render();
    });

    row.innerHTML = `
      <td>${node.id}</td>
      <td>${node.is_sentinel ? "sentinel" : "peer"}</td>
      <td>${pill(node.running ? "running" : "stopped", node.running ? "green" : "red")}</td>
      <td>${pill(node.networking_enabled ? "enabled" : "disabled", node.networking_enabled ? "green" : "red")}</td>
      <td>${fmt(node.state_id)}</td>
      <td>${fmt(node.generation)}</td>
      <td>${fmt(node.recover_seq)}</td>
      <td>${fmt(node.state_seq)}</td>
      <td>${fmt(node.app_seq)}</td>
      <td>${node.counters ? node.counters.join(",") : "none"}</td>
      <td class="row-actions">
        <button data-node-start="${node.id}">${node.running ? "Restart" : "Start"}</button>
        <button data-node-stop="${node.id}" ${node.running ? "" : "disabled"}>Stop</button>
        <button data-node-net="${node.id}" data-enabled="${!node.networking_enabled}">
          ${node.networking_enabled ? "Net off" : "Net on"}
        </button>
      </td>
    `;

    row.querySelector("[data-node-start]").addEventListener("click", async (event) => {
      event.stopPropagation();
      const id = Number(event.currentTarget.dataset.nodeStart);
      if (node.running) await api(`/api/nodes/${id}/stop`, { method: "POST", body: "{}" });
      await api(`/api/nodes/${id}/start`, { method: "POST", body: "{}" });
      await refresh();
    });
    row.querySelector("[data-node-stop]").addEventListener("click", async (event) => {
      event.stopPropagation();
      const id = Number(event.currentTarget.dataset.nodeStop);
      await api(`/api/nodes/${id}/stop`, { method: "POST", body: "{}" });
      await refresh();
    });
    row.querySelector("[data-node-net]").addEventListener("click", async (event) => {
      event.stopPropagation();
      const id = Number(event.currentTarget.dataset.nodeNet);
      const enabled = event.currentTarget.dataset.enabled === "true";
      await api(`/api/nodes/${id}/network`, {
        method: "POST",
        body: JSON.stringify({ enabled }),
      });
      await refresh();
    });

    nodesBody.appendChild(row);
  }
}

function renderLinks() {
  linksGrid.innerHTML = "";
  for (const link of snapshot.links) {
    const card = document.createElement("div");
    card.className = `link-card ${!link.enabled ? "disabled" : link.latency_ms > 0 ? "latent" : ""}`;
    card.innerHTML = `
      <div class="link-title">${link.a} ↔ ${link.b}</div>
      <label>
        Enabled
        <input type="checkbox" ${link.enabled ? "checked" : ""} />
      </label>
      <input type="number" min="0" max="60000" value="${link.latency_ms}" />
      <button>Apply</button>
    `;
    const enabledInput = card.querySelector('input[type="checkbox"]');
    const latencyInput = card.querySelector('input[type="number"]');
    card.querySelector("button").addEventListener("click", async () => {
      await api("/api/links", {
        method: "POST",
        body: JSON.stringify({
          a: link.a,
          b: link.b,
          enabled: enabledInput.checked,
          latency_ms: Number(latencyInput.value),
        }),
      });
      await refresh();
    });
    linksGrid.appendChild(card);
  }
}

function renderDetail() {
  const node = snapshot?.nodes.find((item) => item.id === selectedNodeId);
  if (!node) return;

  detailTitle.textContent = `Node ${node.id} · ${node.is_sentinel ? "Sentinel" : "Regular Peer"} · ${node.running ? "Running" : "Stopped"} · ${node.networking_enabled ? "Network Enabled" : "Network Disabled"}`;
  for (const button of document.querySelectorAll("#detail-tabs button")) {
    button.classList.toggle("active", button.dataset.tab === selectedTab);
  }

  const data = detailData(node);
  detailJson.textContent = JSON.stringify(data, null, 2);
}

function detailData(node) {
  if (selectedTab === "summary") {
    return {
      id: node.id,
      is_sentinel: node.is_sentinel,
      running: node.running,
      networking_enabled: node.networking_enabled,
      state_id: node.state_id,
      generation: node.generation,
      recover_accept_seq: node.recover_seq,
      state_accept_seq: node.state_seq,
      app_sequence: node.app_seq,
    };
  }
  if (selectedTab === "recoverable") {
    return node.recoverable || { available: false, reason: "node stopped" };
  }
  if (selectedTab === "state") {
    return node.state || { available: false, reason: "node stopped" };
  }
  if (selectedTab === "network") {
    return {
      networking_enabled: node.networking_enabled,
      links: node.links,
    };
  }
  return node;
}

function pill(text, color) {
  return `<span class="pill ${color}">${text}</span>`;
}

function fmt(value) {
  return value === null || value === undefined ? "none" : value;
}

refresh();
setInterval(refresh, 500);
