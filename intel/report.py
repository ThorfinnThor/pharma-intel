from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from intel.sanitize import sanitize_asset_label, is_plausible_asset_label


STAGE_RANK = {
    "Discovery": 0,
    "Preclinical": 1,
    "Phase 1": 2,
    "Phase 2": 3,
    "Phase 3": 4,
    "Registration": 5,
    "Approved": 6,
    "Unknown": -1,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stage_rank(stage: str | None) -> int:
    if not stage:
        return -1
    return STAGE_RANK.get(stage.strip(), -1)


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def load_company(conn: sqlite3.Connection, company_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT id, name FROM companies WHERE id = ?", (company_id,)).fetchone()
    if not row:
        raise SystemExit(f"Unknown company_id in DB: {company_id}")
    return {"company_id": row["id"], "company_name": row["name"]}


def fetch_assets(conn: sqlite3.Connection, company_id: str) -> list[dict[str, Any]]:
    cols = table_columns(conn, "assets")
    name_col = "canonical_name" if "canonical_name" in cols else ("name" if "name" in cols else "asset_name")
    disclosed_col = "is_disclosed" if "is_disclosed" in cols else None

    sel = ["id", name_col]
    if disclosed_col:
        sel.append(disclosed_col)

    rows = conn.execute(
        f"SELECT {', '.join(sel)} FROM assets WHERE company_id = ?",
        (company_id,),
    ).fetchall()

    out = []
    for r in rows:
        raw = r[name_col]
        clean = sanitize_asset_label(raw) or raw
        if not is_plausible_asset_label(clean):
            continue
        if disclosed_col and int(r[disclosed_col]) == 0:
            continue
        out.append({"asset_id": r["id"], "asset_name": clean})
    return out


def fetch_indications(conn: sqlite3.Connection, asset_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not asset_ids:
        return {}

    q = f"""
    SELECT asset_id, indication, stage, therapeutic_area
    FROM asset_indications
    WHERE asset_id IN ({",".join(["?"] * len(asset_ids))})
    """
    rows = conn.execute(q, asset_ids).fetchall()

    m: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        m.setdefault(r["asset_id"], []).append(
            {
                "indication": r["indication"],
                "stage": r["stage"],
                "therapeutic_area": r["therapeutic_area"],
            }
        )
    return m


def fetch_linked_trial_counts(conn: sqlite3.Connection, company_id: str) -> dict[int, int]:
    q = """
    SELECT l.asset_id AS asset_id, COUNT(DISTINCT l.trial_id) AS n
    FROM trial_asset_links l
    JOIN trials t ON t.id = l.trial_id
    WHERE t.company_id = ?
    GROUP BY l.asset_id
    """
    rows = conn.execute(q, (company_id,)).fetchall()
    return {int(r["asset_id"]): int(r["n"]) for r in rows}


def pick_top_assets(
    assets: list[dict[str, Any]],
    indications_by_asset: dict[int, list[dict[str, Any]]],
    trial_counts: dict[int, int],
    limit: int = 25,
) -> list[dict[str, Any]]:
    enriched = []
    for a in assets:
        aid = a["asset_id"]
        inds = indications_by_asset.get(aid, [])
        highest = None
        best_rank = -1
        for ind in inds:
            rk = stage_rank(ind.get("stage"))
            if rk > best_rank:
                best_rank = rk
                highest = ind.get("stage")

        enriched.append(
            {
                "asset_id": aid,
                "asset_name": a["asset_name"],
                "highest_stage": highest or "Unknown",
                "linked_trials_count": int(trial_counts.get(aid, 0)),
                "indications": inds,
            }
        )

    enriched.sort(key=lambda x: (stage_rank(x["highest_stage"]), x["linked_trials_count"], x["asset_name"]), reverse=True)
    return enriched[:limit]


def fetch_recent_changes(conn: sqlite3.Connection, company_id: str, limit: int = 200) -> list[dict[str, Any]]:
    cols = table_columns(conn, "change_events")
    ts_col = "occurred_at" if "occurred_at" in cols else ("created_at" if "created_at" in cols else None)
    if not ts_col:
        ts_col = "occurred_at"

    rows = conn.execute(
        f"SELECT event_type, {ts_col} AS ts, payload FROM change_events WHERE company_id = ? ORDER BY {ts_col} DESC LIMIT ?",
        (company_id, limit),
    ).fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "event_type": r["event_type"],
                "created_at": r["ts"],
                "occurred_at": r["ts"],
                "payload": json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"],
            }
        )
    return out


def fetch_trials(conn: sqlite3.Connection, company_id: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, nct_id, overall_status, phase, last_update_posted
        FROM trials
        WHERE company_id = ?
        ORDER BY COALESCE(last_update_posted, '') DESC, id DESC
        LIMIT ?
        """,
        (company_id, limit),
    ).fetchall()

    trial_ids = [int(r["id"]) for r in rows]
    linked_assets: dict[int, list[str]] = {tid: [] for tid in trial_ids}

    if trial_ids:
        link_rows = conn.execute(
            f"""
            SELECT l.trial_id AS trial_id, a.canonical_name AS asset_name
            FROM trial_asset_links l
            JOIN assets a ON a.id = l.asset_id
            WHERE l.trial_id IN ({",".join(["?"] * len(trial_ids))})
            """,
            trial_ids,
        ).fetchall()

        for lr in link_rows:
            nm = sanitize_asset_label(lr["asset_name"]) or lr["asset_name"]
            if is_plausible_asset_label(nm):
                linked_assets[int(lr["trial_id"])].append(nm)

    out = []
    for r in rows:
        out.append(
            {
                "nct_id": r["nct_id"],
                "overall_status": r["overall_status"],
                "phase": r["phase"],
                "last_update_posted": r["last_update_posted"],
                "linked_assets": sorted(set(linked_assets.get(int(r["id"]), []))),
            }
        )
    return out


def write_company_md(page: dict[str, Any], outpath: Path) -> None:
    lines = []
    lines.append(f"# {page['company_name']} ({page['company_id']})")
    lines.append("")
    lines.append(f"Generated: `{page['generated_at']}`")
    lines.append("")
    k = page["kpis"]
    lines.append(f"- Assets: **{k['assets_total']}**")
    lines.append(f"- Assets with linked trials: **{k['assets_with_trials']}**")
    lines.append(f"- Trials: **{k['trials_total']}**")
    lines.append("")
    lines.append("## Top assets")
    lines.append("")
    lines.append("| Asset | Highest stage | Linked trials | Example indications |")
    lines.append("|---|---:|---:|---|")
    for a in page["top_assets"]:
        inds = [i.get("indication") for i in (a.get("indications") or []) if i.get("indication")]
        uniq = []
        for x in inds:
            if x not in uniq:
                uniq.append(x)
        example = "; ".join(uniq[:2])
        lines.append(f"| {a['asset_name']} | {a['highest_stage']} | {a['linked_trials_count']} | {example} |")

    outpath.write_text("\n".join(lines), encoding="utf-8")


def build_company_page(conn: sqlite3.Connection, company_id: str) -> dict[str, Any]:
    comp = load_company(conn, company_id)
    assets = fetch_assets(conn, company_id)
    inds = fetch_indications(conn, [a["asset_id"] for a in assets])
    trial_counts = fetch_linked_trial_counts(conn, company_id)

    top_assets = pick_top_assets(assets, inds, trial_counts, limit=25)

    assets_total = len(assets)
    assets_with_trials = sum(1 for a in assets if trial_counts.get(a["asset_id"], 0) > 0)
    trials_total = conn.execute("SELECT COUNT(*) AS n FROM trials WHERE company_id = ?", (company_id,)).fetchone()["n"]

    page = {
        "company_id": comp["company_id"],
        "company_name": comp["company_name"],
        "generated_at": now_iso(),
        "kpis": {
            "assets_total": int(assets_total),
            "assets_with_trials": int(assets_with_trials),
            "trials_total": int(trials_total),
        },
        "top_assets": top_assets,
        "recent_changes": fetch_recent_changes(conn, company_id, limit=200),
        "trials": fetch_trials(conn, company_id, limit=50),
    }
    return page


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/intel.db")
    ap.add_argument("--outdir", default="exports/site")
    ap.add_argument("--companies", nargs="*", default=None)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    conn = db_connect(args.db)

    if args.companies:
        companies = args.companies
    else:
        companies = [r["id"] for r in conn.execute("SELECT id FROM companies ORDER BY id").fetchall()]

    index = {"generated_at": now_iso(), "companies": []}

    for cid in companies:
        page = build_company_page(conn, cid)
        (outdir / f"{cid}.json").write_text(json.dumps(page, indent=2), encoding="utf-8")
        write_company_md(page, outdir / f"{cid}.md")

        index["companies"].append(
            {
                "company_id": page["company_id"],
                "company_name": page["company_name"],
                "assets_total": page["kpis"]["assets_total"],
                "trials_total": page["kpis"]["trials_total"],
            }
        )

    (outdir / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")

    # basic index.md
    md = ["# Pharma Intel", "", f"Generated: `{index['generated_at']}`", "", "## Companies", ""]
    for c in index["companies"]:
        md.append(f"- **{c['company_name']}** ({c['company_id']}): {c['assets_total']} assets, {c['trials_total']} trials")
    (outdir / "index.md").write_text("\n".join(md), encoding="utf-8")

    conn.close()


if __name__ == "__main__":
    main()
