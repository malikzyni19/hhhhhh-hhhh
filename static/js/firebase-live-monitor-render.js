import { createSignalCard } from "./firebase-live-monitor-card.js";

export const HOST_ID = "firebase-live-monitor-feed";

export function drawLiveMonitor(host, model) {
  host.dataset.version = String(model.version);
  host.replaceChildren();

  const head = document.createElement("div");
  head.className = "flm-head";
  const title = document.createElement("div");
  title.className = "flm-title";
  const dot = document.createElement("span");
  dot.className = `flm-dot${model.state === "connected" ? " ok" : model.state === "error" ? " bad" : ""}`;
  const labels = document.createElement("div");
  const label = document.createElement("div");
  label.textContent = "Realtime signals";
  const sub = document.createElement("div");
  sub.className = "flm-sub";
  sub.textContent = model.message;
  labels.append(label, sub);
  title.append(dot, labels);

  const count = document.createElement("span");
  count.className = "flm-count";
  count.textContent = `${model.signals.length} Firebase signal${model.signals.length === 1 ? "" : "s"}`;
  head.append(title, count);
  host.appendChild(head);

  if (model.state === "error" || !model.signals.length) {
    const empty = document.createElement("div");
    empty.className = `flm-empty${model.state === "error" ? " flm-error" : ""}`;
    empty.textContent = model.state === "error"
      ? model.message
      : model.state === "connected"
        ? "No active live_setups signals."
        : "Waiting for Firestore…";
    host.appendChild(empty);
    return;
  }

  const grid = document.createElement("div");
  grid.className = "flm-grid";
  model.signals.slice(0, 12).forEach((row) => grid.appendChild(createSignalCard(row)));
  host.appendChild(grid);
}
