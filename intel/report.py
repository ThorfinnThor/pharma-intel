from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------- utilities ----------

def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    )
    return cur.fetchone() is not None


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def pick_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def safe_json_loads(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            x = x.decode("utf-8", errors="replace")
        except Exception:
            return str(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return s
    return x


# ---------- stage ranking ----------

STAGE_RANK = {
    # early
    "discovery": 10,
    "preclinical": 20,
    # clinical
    "phase 1": 30,
    "phase 1/2": 35,
    "phase 2": 40,
    "phase 2/3": 45,
    "phase 3": 50,
    # late
    "registration": 60,
    "filed": 60,
    "approved": 70,
}

def normalize_stage(stage: Optional[str]) -> Optional[str]:
    if not stage:
        return None
    s = str(stage).strip().lower()
    s = s.replace("phase i", "phase 1").replace("phase ii", "phase 2").replace("phase iii", "phase 3")
    s = s.replace("ph1", "phase 1").replace("ph2", "phase 2").replace("ph3", "phase 3")
    s = s.replace("registrat", "registration")
    # keep original-like capitalization for display later
    return s

def best_stage(stages: List[Optional[str]]) -> Optional[str]:
    best = None
    best_rank = -1
    for st in stages:
        n = normalize_stage(st)
        if not n:
            continue
        r = STAGE_RANK.get(n, 0)
        if r > best_rank:
            best_rank = r
            best = n
    return best


def display_stage(norm_stage: Optional[str]) -> str:
    if not norm_stage:
        return ""
    # Title-case for common phases
    if norm_stage.startswith("phase "):
        return norm_stage.title().replace("Phase ", "Phase ")
    if norm_stage == "registration":
        return "Registration"
    return norm_stage.title()


# ---------- DB fetchers ----------

def fetch_companies(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if table_exists(conn, "companies"):
        cols = get_columns(conn, "companies")
        id_col = pick_column(cols, ["id", "company_id"])
        name_col = pick_column(cols, ["name", "company_name"])
        if not id_col:
            raise RuntimeError("companies table exists but no id/company_id column found")
        q = f"SELECT {id_col} as company_id, {name_col or id_col} as company_name FROM companies ORDER BY company_id"
        rows = conn.execute(q).fetchall()
        return [{"company_id": r[0], "company_name": r[1]} for r in rows]

    if table_exists(conn, "assets"):
        cols = get_columns(conn, "assets")
        cid = pick_column(cols, ["company_id"])
        if not cid:
            return []
        rows = conn.execute(f"SELECT DISTINCT {cid} FROM assets ORDER BY {cid}").fetchall()
        return [{"company_id": r[0], "company_name": r[0]} for r in rows]

    return []


def fetch_assets_with_indications(conn: sqlite3.Connection, company_id: str) -> List[Dict[str, Any]]:
    if not table_exists(conn, "assets"):
        return []

    asset_cols = get_columns(conn, "assets")
    a_id = pick_column(asset_cols, ["id"])
    a_company = pick_column(asset_cols, ["company_id"])
    a_name = pick_column(asset_cols, ["name", "asset_name", "canonical_name"])
    if not (a_id and a_company and a_name):
        return []

    assets = conn.execute(
        f"SELECT {a_id} as asset_id, {a_name} as asset_name FROM assets WHERE {a_company}=? ORDER BY {a_name}",
        (company_id,),
    ).fetchall()

    out = [{"asset_id": r[0], "asset_name": r[1], "indications": []} for r in assets]

    # attach indications (supports either asset_indications or indications)
    ind_table = "asset_indications" if table_exists(conn, "asset_indications") else ("indications" if table_exists(conn, "indications") else None)
    if not ind_table:
        return out

    cols = get_columns(conn, ind_table)
    aid = pick_column(cols, ["asset_id"])
    indication = pick_column(cols, ["indication", "condition"])
    stage = pick_column(cols, ["stage", "phase"])
    ta = pick_column(cols, ["therapeutic_area", "ta"])
    as_of = pick_column(cols, ["as_of_date", "asof_date", "as_of"])

    if not (aid and indication):
        return out

    select_cols = [aid, indication]
    if stage: select_cols.append(stage)
    if ta: select_cols.append(ta)
    if as_of: select_cols.append(as_of)

    rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM {ind_table}").fetchall()

    asset_map = {a["asset_id"]: a for a in out}
    idx_a = 0
    idx_i = 1
    idx_s = select_cols.index(stage) if stage in select_cols else None
    idx_t = select_cols.index(ta) if ta in select_cols else None
    idx_d = select_cols.index(as_of) if as_of in select_cols else None

    for r in rows:
        asset_id = r[idx_a]
        if asset_id not in asset_map:
            continue
        ind = r[idx_i]
        if not ind:
            continue
        asset_map[asset_id]["indications"].append({
            "indication": ind,
            "stage": r[idx_s] if idx_s is not None else None,
            "therapeutic_area": r[idx_t] if idx_t is not None else None,
            "as_of_date": r[idx_d] if idx_d is not None else None,
        })

    # stable sort indications
    for a in out:
        a["indications"].sort(key=lambda x: (
            str(x.get("stage") or ""),
            str(x.get("therapeutic_area") or ""),
            str(x.get("indication") or ""),
            str(x.get("as_of_date") or ""),
        ))

    return out


def fetch_recent_changes(conn: sqlite3.Connection, company_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    if not table_exists(conn, "change_events"):
        return []

    cols = get_columns(conn, "change_events")
    cid = pick_column(cols, ["company_id"])
    etype = pick_column(cols, ["event_type", "type"])
    created = pick_column(cols, ["created_at", "ts", "timestamp"])
    payload = pick_column(cols, ["payload", "payload_json", "data"])
    asset_id = pick_column(cols, ["asset_id"])
    trial_id = pick_column(cols, ["trial_id"])

    if not (cid and etype):
        return []

    select_cols = [etype]
    if created: select_cols.append(created)
    if payload: select_cols.append(payload)
    if asset_id: select_cols.append(asset_id)
    if trial_id: select_cols.append(trial_id)

    order_by = created or etype

    q = f"""
    SELECT {', '.join(select_cols)}
    FROM change_events
    WHERE {cid}=?
    ORDER BY {order_by} DESC
    LIMIT ?
    """
    rows = conn.execute(q, (company_id, limit)).fetchall()

    i_type = 0
    i_created = select_cols.index(created) if created in select_cols else None
    i_payload = select_cols.index(payload) if payload in select_cols else None
    i_asset = select_cols.index(asset_id) if asset_id in select_cols else None
    i_trial = select_cols.index(trial_id) if trial_id in select_cols else None

    out = []
    for r in rows:
        out.append({
            "event_type": r[i_type],
            "created_at": r[i_created] if i_created is not None else None,
            "asset_id": r[i_asset] if i_asset is not None else None,
            "trial_id": r[i_trial] if i_trial is not None else None,
            "payload": safe_json_loads(r[i_payload]) if i_payload is not None else None,
        })
    return out


def fetch_trials(conn: sqlite3.Connection, company_id: str) -> List[Dict[str, Any]]:
    if not table_exists(conn, "trials"):
        return []

    tcols = get_columns(conn, "trials")
    t_id = pick_column(tcols, ["id"])
    t_company = pick_column(tcols, ["company_id"])
    nct = pick_column(tcols, ["nct_id", "nctId"])
    status = pick_column(tcols, ["overall_status", "status"])
    phase = pick_column(tcols, ["phase"])
    last_upd = pick_column(tcols, ["last_update_posted", "last_update"])
    title = pick_column(tcols, ["title"])
    if not (t_id and t_company and nct):
        return []

    select_cols = [t_id, nct]
    if title: select_cols.append(title)
    if status: select_cols.append(status)
    if phase: select_cols.append(phase)
    if last_upd: select_cols.append(last_upd)

    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM trials WHERE {t_company}=? ORDER BY {last_upd or nct} DESC",
        (company_id,),
    ).fetchall()

    i_id = 0
    i_nct = 1
    i_title = select_cols.index(title) if title in select_cols else None
    i_status = select_cols.index(status) if status in select_cols else None
    i_phase = select_cols.index(phase) if phase in select_cols else None
    i_last = select_cols.index(last_upd) if last_upd in select_cols else None

    out = []
    for r in rows:
        out.append({
            "trial_id": r[i_id],
            "nct_id": r[i_nct],
            "title": r[i_title] if i_title is not None else None,
            "overall_status": r[i_status] if i_status is not None else None,
            "phase": r[i_phase] if i_phase is not None else None,
            "last_update_posted": r[i_last] if i_last is not None else None,
            "linked_assets": [],
        })
    return out


def attach_trial_asset_links(conn: sqlite3.Connection, assets: List[Dict[str, Any]], trials: List[Dict[str, Any]]) -> None:
    if not (table_exists(conn, "trial_asset_links") and table_exists(conn, "assets")):
        return

    lcols = get_columns(conn, "trial_asset_links")
    trial_id_col = pick_column(lcols, ["trial_id"])
    asset_id_col = pick_column(lcols, ["asset_id"])
    if not (trial_id_col and asset_id_col):
        return

    asset_name_by_id = {a["asset_id"]: a["asset_name"] for a in assets}
    trial_by_id = {t["trial_id"]: t for t in trials}

    rows = conn.execute(f"SELECT {trial_id_col}, {asset_id_col} FROM trial_asset_links").fetchall()
    for tid, aid in rows:
        if tid in trial_by_id and aid in asset_name_by_id:
            trial_by_id[tid]["linked_assets"].append(asset_name_by_id[aid])

    for t in trials:
        t["linked_assets"] = sorted(set(t.get("linked_assets") or []), key=lambda s: s.lower())


# ---------- report builders ----------

def build_company_page(
    company_id: str,
    company_name: str,
    assets: List[Dict[str, Any]],
    changes: List[Dict[str, Any]],
    trials: List[Dict[str, Any]],
) -> Dict[str, Any]:
    # KPIs
    stages = []
    assets_by_stage = Counter()
    for a in assets:
        st = best_stage([i.get("stage") for i in (a.get("indications") or [])])
        stages.append(st)
        assets_by_stage[display_stage(st) or "Unspecified"] += 1

    trials_by_status = Counter()
    for t in trials:
        trials_by_status[(t.get("overall_status") or "Unspecified")] += 1

    # assets with trials
    assets_with_trials = set()
    if trials:
        linked_assets = set()
        for t in trials:
            for an in (t.get("linked_assets") or []):
                linked_assets.add(an.lower())
        for a in assets:
            if a["asset_name"].lower() in linked_assets:
                assets_with_trials.add(a["asset_name"].lower())

    # last change per asset (best effort)
    last_change_by_asset_id: Dict[Any, Any] = {}
    for ch in changes:
        aid = ch.get("asset_id")
        if aid is None:
            continue
        ts = ch.get("created_at")
        if aid not in last_change_by_asset_id:
            last_change_by_asset_id[aid] = ts

    # Top assets: highest stage first, then linked trials count, then name
    trial_count_by_asset_name = Counter()
    for t in trials:
        for an in (t.get("linked_assets") or []):
            trial_count_by_asset_name[an] += 1

    def asset_sort_key(a: Dict[str, Any]) -> Tuple[int, int, str]:
        st = best_stage([i.get("stage") for i in (a.get("indications") or [])])
        r = STAGE_RANK.get(normalize_stage(st) or "", 0)
        tc = trial_count_by_asset_name.get(a["asset_name"], 0)
        return (r, tc, a["asset_name"].lower())

    top_assets_sorted = sorted(assets, key=asset_sort_key, reverse=True)[:25]
    top_assets = []
    for a in top_assets_sorted:
        inds = a.get("indications") or []
        st = best_stage([i.get("stage") for i in inds])
        top_assets.append({
            "asset_id": a.get("asset_id"),
            "asset_name": a.get("asset_name"),
            "highest_stage": display_stage(st) or "Unspecified",
            "linked_trials_count": int(trial_count_by_asset_name.get(a.get("asset_name"), 0)),
            "indications": [
                {
                    "indication": i.get("indication"),
                    "stage": i.get("stage"),
                    "therapeutic_area": i.get("therapeutic_area"),
                    "as_of_date": i.get("as_of_date"),
                }
                for i in inds[:5]
            ],
            "last_change_at": last_change_by_asset_id.get(a.get("asset_id")),
        })

    page = {
        "generated_at": utc_now_iso(),
        "company_id": company_id,
        "company_name": company_name,
        "kpis": {
            "assets_total": len(assets),
            "assets_with_trials": len(assets_with_trials),
            "trials_total": len(trials),
            "trials_by_status": dict(trials_by_status),
            "assets_by_stage": dict(assets_by_stage),
        },
        "top_assets": top_assets,
        "recent_changes": changes[:50],
        "trials": trials[:200],  # cap
    }
    return page


def render_company_markdown(page: Dict[str, Any]) -> str:
    k = page["kpis"]
    lines = []
    lines.append(f"# {page['company_name']} ({page['company_id']})")
    lines.append("")
    lines.append(f"Generated: `{page['generated_at']}`")
    lines.append("")
    lines.append("## KPIs")
    lines.append("")
    lines.append(f"- Assets: **{k['assets_total']}**")
    lines.append(f"- Assets with linked trials: **{k['assets_with_trials']}**")
    lines.append(f"- Trials: **{k['trials_total']}**")
    lines.append("")
    lines.append("### Assets by stage")
    for stage, cnt in sorted(k["assets_by_stage"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"- {stage}: **{cnt}**")
    lines.append("")
    lines.append("### Trials by status")
    for status, cnt in sorted(k["trials_by_status"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"- {status}: **{cnt}**")
    lines.append("")
    lines.append("## Top assets")
    lines.append("")
    lines.append("| Asset | Highest stage | Linked trials | Example indications |")
    lines.append("|---|---:|---:|---|")
    for a in page["top_assets"][:25]:
        inds = a.get("indications") or []
        ind_txt = "; ".join([i.get("indication") or "" for i in inds[:3] if i.get("indication")]) or ""
        lines.append(f"| {a['asset_name']} | {a['highest_stage']} | {a['linked_trials_count']} | {ind_txt} |")
    lines.append("")
    lines.append("## Recent changes")
    lines.append("")
    for ch in page.get("recent_changes") or []:
        ts = ch.get("created_at") or ""
        et = ch.get("event_type") or ""
        payload = ch.get("payload")
        payload_s = json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else (payload or "")
        if len(payload_s) > 180:
            payload_s = payload_s[:180] + "…"
        lines.append(f"- `{ts}` **{et}** — {payload_s}")
    lines.append("")
    lines.append("## Trials (latest)")
    lines.append("")
    if not page.get("trials"):
        lines.append("_No trials table found in DB or no trials ingested yet._")
    else:
        lines.append("| NCT | Status | Phase | Last update | Linked assets |")
        lines.append("|---|---|---|---|---|")
        for t in page["trials"][:50]:
            nct = t.get("nct_id") or ""
            st = t.get("overall_status") or ""
            ph = t.get("phase") or ""
            lu = t.get("last_update_posted") or ""
            la = "; ".join(t.get("linked_assets") or [])
            lines.append(f"| {nct} | {st} | {ph} | {lu} | {la} |")
    lines.append("")
    return "\n".join(lines)


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate company intelligence pages (JSON + Markdown) from SQLite DB.")
    ap.add_argument("--db", default="data/intel.db", help="Path to SQLite DB (default: data/intel.db)")
    ap.add_argument("--outdir", default="exports/site", help="Output directory (default: exports/site)")
    ap.add_argument("--companies", nargs="*", default=None, help="Company IDs to export (default: all in DB)")
    ap.add_argument("--changes-limit", type=int, default=100, help="Recent changes per company (default: 100)")
    args = ap.parse_args()

    db_path = Path(args.db)
    outdir = Path(args.outdir)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    companies = fetch_companies(conn)
    if args.companies:
        wanted = set(args.companies)
        companies = [c for c in companies if c["company_id"] in wanted]

    index = {
        "generated_at": utc_now_iso(),
        "companies": [],
    }

    for c in companies:
        cid = c["company_id"]
        cname = c["company_name"]

        assets = fetch_assets_with_indications(conn, cid)
        changes = fetch_recent_changes(conn, cid, limit=args.changes_limit)
        trials = fetch_trials(conn, cid)
        attach_trial_asset_links(conn, assets, trials)

        page = build_company_page(cid, cname, assets, changes, trials)
        md = render_company_markdown(page)

        write_json(outdir / f"{cid}.json", page)
        write_text(outdir / f"{cid}.md", md)

        index["companies"].append({
            "company_id": cid,
            "company_name": cname,
            "assets_total": page["kpis"]["assets_total"],
            "trials_total": page["kpis"]["trials_total"],
            "path_json": f"{cid}.json",
            "path_md": f"{cid}.md",
        })

    write_json(outdir / "index.json", index)

    # Simple markdown index for humans
    md_lines = ["# Company Intelligence Index", "", f"Generated: `{index['generated_at']}`", ""]
    for c in index["companies"]:
        md_lines.append(f"- **{c['company_name']}** (`{c['company_id']}`): "
                        f"{c['assets_total']} assets, {c['trials_total']} trials — "
                        f"[md]({c['path_md']}) / [json]({c['path_json']})")
    write_text(outdir / "index.md", "\n".join(md_lines))

    conn.close()
    print(f"[report] wrote company pages to {outdir.resolve()}")


if __name__ == "__main__":
    main()
