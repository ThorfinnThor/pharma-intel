// Configure your repo here:
const OWNER = "ThorfinnThor";
const REPO = "pharma-intel";
const BRANCH = "data-snapshots";
const BASE_PATH = "exports/site";

// Primary: raw.githubusercontent.com (usually fastest, no auth)
const RAW_BASE = `https://raw.githubusercontent.com/${OWNER}/${REPO}/${BRANCH}/${BASE_PATH}`;
// Fallback: jsDelivr CDN (sometimes cached)
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

function setStatus(msg) {
  document.getElementById("status").textContent = msg || "";
}

function companyCard(c) {
  const a = document.createElement("a");
  a.className = "company-card";
  a.href = `./company.html?id=${encodeURIComponent(c.company_id)}`;

  const title = document.createElement("div");
  title.className = "title";
  title.textContent = `${c.company_name} (${c.company_id})`;

  const meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = `${c.assets_total} assets • ${c.trials_total} trials`;

  a.appendChild(title);
  a.appendChild(meta);
  return a;
}

async function loadIndex() {
  setStatus("Loading…");
  const companiesEl = document.getElementById("companies");
  const metaEl = document.getElementById("meta");
  companiesEl.innerHTML = "";
  metaEl.textContent = "";

  const index = await fetchJsonWithFallback("index.json");

  metaEl.textContent = `Generated: ${index.generated_at} • Source: ${OWNER}/${REPO}@${BRANCH}/${BASE_PATH}`;

  // sort: most assets first, then trials
  const companies = [...index.companies].sort((a, b) => {
    if (b.assets_total !== a.assets_total) return b.assets_total - a.assets_total;
    return (b.trials_total || 0) - (a.trials_total || 0);
  });

  for (const c of companies) {
    companiesEl.appendChild(companyCard(c));
  }

  setStatus("");
}

document.getElementById("refreshBtn").addEventListener("click", () => {
  loadIndex().catch((e) => setStatus(`Error: ${e.message}`));
});

loadIndex().catch((e) => setStatus(`Error: ${e.message}`));
