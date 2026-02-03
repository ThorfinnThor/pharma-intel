const OWNER = "ThorfinnThor";
const REPO = "pharma-intel";
const BRANCH = "data-snapshots";
const BASE_PATH = "exports/site";

const RAW_BASE = `https://raw.githubusercontent.com/${OWNER}/${REPO}/${BRANCH}/${BASE_PATH}`;
const CDN_BASE = `https://cdn.jsdelivr.net/gh/${OWNER}/${REPO}@${BRANCH}/${BASE_PATH}`;

async function fetchJsonWithFallback(path) {
  const urls = [
    `${RAW_BASE}/${path}?v=${Date.now()}`,
    `${CDN_BASE}/${path}`,
  ];
  let lastErr;
  for (const url of urls) {
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr;
}

function qs(key) {
  return new URLSearchParams(window.location.search).get(key);
}

function setStatus(msg) {
  document.getElementById("status").textContent = msg || "";
}

function kpiBox(label, value) {
  const div = document.createElement("div");
  div.className = "kpi";
  div.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
  return div;
}

function uniq(arr) {
  return [...new Set(arr)];
}

function formatIndications(indications) {
  const names = indications.map((x) => x.indication).filter(Boolean);
  return uniq(names).slice(0, 3).join("; ");
}

function safeText(x) {
  return (x === null || x === undefined) ? "" : String(x);
}

function formatChange(ch) {
  const et = safeText(ch.event_type);
  const p = ch.payload || {};
  if (et === "pipeline_ingested") {
    return `pipeline_ingested — as_of=${p.as_of_date || "?"} assets_seen=${p.assets_seen ?? "?"}`;
  }
  if (et === "asset_added") {
    return `asset_added — ${p.asset || ""}`;
  }
  if (et === "asset_indication_added") {
    return `indication_added — ${p.asset || ""}: ${p.indication || ""} (${p.stage || ""})`;
  }
  if (et === "asset_indication_removed") {
    return `indication_removed — ${p.asset || ""}: ${p.indication || ""} (${p.stage || ""})`;
  }
  if (et === "trials_ingested") {
    return `trials_ingested — seen=${p.trials_seen ?? "?"} inserted=${p.inserted ?? "?"} updated=${p.updated ?? "?"} status_changed=${p.status_changed ?? "?"} bad_aliases=${p.bad_aliases ?? "?"}`;
  }
  // fallback
  return `${et} ${ch.payload ? JSON.stringify(ch.payload) : ""}`.trim();
}

function compactRecentChanges(changes) {
  // Hide ultra-noisy types, and dedupe repeated trials_ingested
  const NOISE = new Set(["trial_assets_linked"]);
  let lastTrialsIngested = null;

  const out = [];
  for (const ch of changes || []) {
    if (NOISE.has(ch.event_type)) continue;
    if (ch.event_type === "trials_ingested") {
      lastTrialsIngested = ch; // keep only newest
      continue;
    }
    out.push(ch);
    if (out.length >= 40) break;
  }

  if (lastTrialsIngested) out.unshift(lastTrialsIngested);
  return out.slice(0, 40);
}

async function loadCompany() {
  const id = qs("id");
  if (!id) {
    document.getElementById("title").textContent = "Missing company id";
    return;
  }

  setStatus("Loading…");

  const page = await fetchJsonWithFallback(`${encodeURIComponent(id)}.json`);

  document.getElementById("title").textContent = `${page.company_name} (${page.company_id})`;
  document.getElementById("subtitle").textContent = `Generated: ${page.generated_at}`;

  // KPIs
  const k = page.kpis || {};
  const kpisEl = document.getElementById("kpis");
  kpisEl.innerHTML = "";
  kpisEl.appendChild(kpiBox("Assets", k.assets_total ?? 0));
  kpisEl.appendChild(kpiBox("Assets with linked trials", k.assets_with_trials ?? 0));
  kpisEl.appendChild(kpiBox("Trials", k.trials_total ?? 0));

  // Top assets
  const assetsBody = document.querySelector("#assetsTable tbody");
  assetsBody.innerHTML = "";
  for (const a of (page.top_assets || []).slice(0, 25)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${safeText(a.asset_name)}</td>
      <td>${safeText(a.highest_stage)}</td>
      <td class="num">${safeText(a.linked_trials_count)}</td>
      <td>${formatIndications(a.indications || [])}</td>
    `;
    assetsBody.appendChild(tr);
  }

  // Recent changes (cleaned)
  const changesEl = document.getElementById("changes");
  changesEl.innerHTML = "";
  const cleaned = compactRecentChanges(page.recent_changes || []);
  for (const ch of cleaned) {
    const li = document.createElement("li");
    const ts = safeText(ch.created_at || ch.occurred_at || "");
    li.innerHTML = `<code>${ts}</code> ${formatChange(ch)}`;
    changesEl.appendChild(li);
  }

  // Trials
  const trialsBody = document.querySelector("#trialsTable tbody");
  trialsBody.innerHTML = "";
  for (const t of (page.trials || []).slice(0, 50)) {
    const la = (t.linked_assets || []).slice(0, 6).join("; ");
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${safeText(t.nct_id)}</td>
      <td>${safeText(t.overall_status)}</td>
      <td>${safeText(t.phase)}</td>
      <td>${safeText(t.last_update_posted)}</td>
      <td>${safeText(la)}</td>
    `;
    trialsBody.appendChild(tr);
  }

  setStatus("");
}

document.getElementById("refreshBtn").addEventListener("click", () => {
  loadCompany().catch((e) => setStatus(`Error: ${e.message}`));
});

loadCompany().catch((e) => setStatus(`Error: ${e.message}`));
