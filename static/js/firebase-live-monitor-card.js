export function dateOf(value) {
  if (!value) return null;
  if (typeof value.toDate === "function") return value.toDate();
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function createSignalCard(row) {
  const card = document.createElement("article");
  card.className = "flm-card";

  const top = document.createElement("div");
  top.className = "flm-top";
  const symbol = document.createElement("span");
  symbol.className = "flm-symbol";
  symbol.textContent = row.symbol || "Unknown pair";
  const timeframe = document.createElement("span");
  timeframe.className = "flm-tf";
  timeframe.textContent = row.timeframe || "—";
  top.append(symbol, timeframe);

  const meta = document.createElement("div");
  meta.className = "flm-meta";
  meta.append(document.createTextNode(`${row.signalType || "unknown_signal"} · `));
  const strength = document.createElement("span");
  strength.className = "flm-strength";
  strength.textContent = `Strength ${row.strength ?? "—"}`;
  const status = document.createElement("span");
  status.className = "flm-status";
  status.textContent = ` · ${row.status || "unknown"}`;
  meta.append(strength, status);

  const time = document.createElement("div");
  time.className = "flm-time";
  const created = dateOf(row.createdAt);
  time.textContent = created ? created.toLocaleString() : "No timestamp";

  card.append(top, meta, time);
  return card;
}
