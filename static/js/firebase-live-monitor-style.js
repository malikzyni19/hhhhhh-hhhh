export function installLiveMonitorStyles(hostId) {
  if (document.getElementById("firebase-live-monitor-style")) return;
  const style = document.createElement("style");
  style.id = "firebase-live-monitor-style";
  style.textContent = `
    #${hostId}{margin:0 0 12px;border:1px solid var(--border);border-radius:11px;background:var(--bg-card);overflow:hidden}
    .flm-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 13px;background:var(--bg-card-2);border-bottom:1px solid var(--border)}
    .flm-title{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700}.flm-dot{width:8px;height:8px;border-radius:50%;background:var(--yellow)}.flm-dot.ok{background:var(--green-2)}.flm-dot.bad{background:var(--red-2)}
    .flm-sub,.flm-empty{font-size:10px;color:var(--text-3)}.flm-count{font:10px var(--mono);color:var(--accent)}
    .flm-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;padding:10px}.flm-card{padding:9px;border:1px solid var(--border);border-radius:8px;background:var(--bg-panel)}
    .flm-top{display:flex;justify-content:space-between;gap:8px}.flm-symbol{font:700 12px var(--mono)}.flm-tf{font:700 9px var(--mono);color:var(--accent)}
    .flm-meta{margin-top:6px;font-size:10px;color:var(--text-2)}.flm-strength{color:var(--yellow)}.flm-status{color:var(--green-2)}.flm-time{margin-top:6px;font:9px var(--mono);color:var(--text-3)}
    .flm-empty{padding:14px;text-align:center}.flm-error{color:var(--red-2)}
  `;
  document.head.appendChild(style);
}
