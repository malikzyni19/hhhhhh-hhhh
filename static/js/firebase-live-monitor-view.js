import { installLiveMonitorStyles } from "./firebase-live-monitor-style.js";
import { drawLiveMonitor, HOST_ID } from "./firebase-live-monitor-render.js";

const model = {
  signals: [],
  state: "connecting",
  message: "Connecting to Firestore…",
  version: 0,
};
let queued = false;

function liveViewOpen() {
  return document.body.dataset.view === "live" ||
    Boolean(document.querySelector('#topnav [data-view="live"].active'));
}

function ensure() {
  queued = false;
  if (!liveViewOpen()) return;
  const tabView = document.getElementById("tabView");
  if (!tabView || tabView.hidden) return;

  let host = document.getElementById(HOST_ID);
  if (!host || !tabView.contains(host)) {
    host = document.createElement("section");
    host.id = HOST_ID;
    tabView.prepend(host);
  }
  if (host.dataset.version !== String(model.version)) drawLiveMonitor(host, model);
}

export function scheduleRender() {
  if (queued) return;
  queued = true;
  requestAnimationFrame(ensure);
}

export function updateView(signals, state, message) {
  model.signals = signals;
  model.state = state;
  model.message = message;
  model.version += 1;
  scheduleRender();
}

export function startView() {
  installLiveMonitorStyles(HOST_ID);
  new MutationObserver(scheduleRender).observe(document.body, {
    attributes: true,
    attributeFilter: ["data-view"],
    childList: true,
    subtree: true,
  });
  document.addEventListener("click", (event) => {
    if (event.target.closest('[data-view="live"]')) setTimeout(scheduleRender, 0);
  });
  scheduleRender();
}
