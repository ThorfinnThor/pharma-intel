from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    )
    return cur.fetchone() is not None


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]  # name column


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


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fetch_companies(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    # Prefer companies table if present; else infer from assets.
    if table_exists(conn, "companies"):
        cols = get_columns(conn, "companies")
        id_col = pick_column(cols, ["id", "company_id"])
        name_col = pick_column(cols, ["name", "company_name"])
        if not id_col:
            raise RuntimeError("companies table exists but no id/company_id column found")
        q = f"SELECT {id_col} as company_id, {name_col or id_col} as company_name FROM companies ORDER BY company_id"
        rows = conn.execute(q).fetchall()
        return [{"company_id": r[0], "company_name": r[1]} for r in rows]

    # Infer from assets
    if table_exists(conn, "assets"):
        cols = get_columns(conn, "assets")
        cid = pick_column(cols, ["company_id"])
        if not cid:
            return []
        rows = conn.execute(f"SELECT DISTINCT {cid} FROM assets ORDER BY {cid}").fetchall()
        return [{"company_id": r[0], "company_name": r[0]} for r in rows]

    return []


def fetch_assets(conn: sqlite3.Connection, company_id: str) -> List[Dict[str, Any]]:
    if not table_exists(conn, "assets"):
        return []

    asset_cols = get_columns(conn, "assets")
    a_id = pick_column(asset_cols, ["id"])
    a_company = pick_column(asset_cols, ["company_id"])
    a_name = pick_column(asset_cols, ["name", "asset_name", "canonical_name"])

    if not (a_id and a_company and a_name):
        # Minimal fallback: dump whatever exists
        q = "SELECT * FROM assets"
        rows = conn.execute(q).fetchall()
        return [{"raw": list(r)} for r in rows]

    assets = conn.execute(
        f"SELECT {a_id} as asset_id, {a_name} as asset_name FROM assets WHERE {a_company}=? ORDER BY {a_name}",
        (company_id,),
    ).fetchall()

    out: List[Dict[str, Any]] = []
    for asset_id, asset_name in assets:
        out.append(
            {
                "asset_id": asset_id,
                "asset_name": asset_name,
                "aliases": [],
                "indications": [],  # list of {indication, stage, therapeutic_area, as_of_date}
            }
        )
    return out


def attach_aliases(conn: sqlite3.Connection, assets: List[Dict[str, Any]]) -> None:
    if not table_exists(conn, "asset_aliases"):
        return

    cols = get_columns(conn, "asset_aliases")
    aid = pick_column(cols, ["asset_id"])
    alias = pick_column(cols, ["alias"])
    if not (aid and alias):
        return

    asset_map = {a["asset_id"]: a for a in assets if "asset_id" in a}
    rows = conn.execute(f"SELECT {aid}, {alias} FROM asset_aliases").fetchall()
    for asset_id, a in rows:
        if asset_id in asset_map and a:
            asset_map[asset_id]["aliases"].append(a)

    # stable ordering
    for a in assets:
        a["aliases"] = sorted(set(a.get("aliases") or []), key=lambda s: s.lower())


def attach_indications(conn: sqlite3.Connection, assets: List[Dict[str, Any]]) -> None:
    # We support either "asset_indications" or "indications" depending on schema.
    table = None
    if table_exists(conn, "asset_indications"):
        table = "asset_indications"
    elif table_exists(conn, "indications"):
        table = "indications"
    else:
        return

    cols = get_columns(conn, table)
    aid = pick_column(cols, ["asset_id"])
    indication = pick_column(cols, ["indication", "condition"])
    stage = pick_column(cols, ["stage", "phase"])
    ta = pick_column(cols, ["therapeutic_area", "ta"])
    as_of = pick_column(cols, ["as_of_date", "asof_date", "as_of"])
    evidence_id = pick_column(cols, ["evidence_id"])

    if not (aid and indication):
        return

    asset_map = {a["asset_id"]: a for a in assets if "asset_id" in a}

    select_cols = [aid, indication]
    if stage:
        select_cols.append(stage)
    if ta:
        select_cols.append(ta)
    if as_of:
        select_cols.append(as_of)
    if evidence_id:
        select_cols.append(evidence_id)

    q = f"SELECT {', '.join(select_cols)} FROM {table}"
    rows = conn.execute(q).fetchall()

    # figure indexes
    idx_asset = 0
    idx_ind = 1
    idx_stage = select_cols.index(stage) if stage in select_cols else None
    idx_ta = select_cols.index(ta) if ta in select_cols else None
    idx_asof = select_cols.index(as_of) if as_of in select_cols else None

    for r in rows:
        asset_id = r[idx_asset]
        if asset_id not in asset_map:
            continue

        item = {
            "indication": r[idx_ind],
            "stage": r[idx_stage] if idx_stage is not None else None,
            "therapeutic_area": r[idx_ta] if idx_ta is not None else None,
            "as_of_date": r[idx_asof] if idx_asof is not None else None,
        }
        # skip empty indication rows
        if not item["indication"]:
            continue
        asset_map[asset_id]["indications"].append(item)

    # stable ordering per asset
    for a in assets:
        inds = a.get("indications") or []
        inds.sort(
            key=lambda x: (
                (x.get("stage") or ""),
                (x.get("therapeutic_area") or ""),
                (x.get("indication") or ""),
                (x.get("as_of_date") or ""),
            )
        )
        a["indications"] = inds


def fetch_changes(conn: sqlite3.Connection, company_id: str, limit: int = 2000) -> List[Dict[str, Any]]:
    # We support change_events table name; if not present, return empty.
    if not table_exists(conn, "change_events"):
        return []

    cols = get_columns(conn, "change_events")
    cid = pick_column(cols, ["company_id"])
    etype = pick_column(cols, ["event_type", "type"])
    created = pick_column(cols, ["created_at", "ts", "timestamp"])
    payload = pick_column(cols, ["payload", "payload_json", "data"])
    evidence_id = pick_column(cols, ["evidence_id"])
    asset_id = pick_column(cols, ["asset_id"])
    trial_id = pick_column(cols, ["trial_id"])

    if not (cid and etype):
        return []

    select_cols = [etype]
    if created:
        select_cols.append(created)
    if payload:
        select_cols.append(payload)
    if evidence_id:
        select_cols.append(evidence_id)
    if asset_id:
        select_cols.append(asset_id)
    if trial_id:
        select_cols.append(trial_id)

    order_by = created or etype
    q = f"""
        SELECT {', '.join(select_cols)}
        FROM change_events
        WHERE {cid}=?
        ORDER BY {order_by} DESC
        LIMIT ?
    """
    rows = conn.execute(q, (company_id, limit)).fetchall()

    # indexes
    i_type = 0
    i_created = select_cols.index(created) if created in select_cols else None
    i_payload = select_cols.index(payload) if payload in select_cols else None
    i_evid = select_cols.index(evidence_id) if evidence_id in select_cols else None
    i_asset = select_cols.index(asset_id) if asset_id in select_cols else None
    i_trial = select_cols.index(trial_id) if trial_id in select_cols else None

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "event_type": r[i_type],
                "created_at": r[i_created] if i_created is not None else None,
                "payload": safe_json_loads(r[i_payload]) if i_payload is not None else None,
                "evidence_id": r[i_evid] if i_evid is not None else None,
                "asset_id": r[i_asset] if i_asset is not None else None,
                "trial_id": r[i_trial] if i_trial is not None else None,
            }
        )
    return out


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def write_assets_csv(path: Path, company_id: str, assets: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    # One row per (asset, indication) to be analysis-friendly
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "asset_id", "asset_name", "aliases", "stage", "therapeutic_area", "as_of_date", "indication"])
        for a in assets:
            aliases = "; ".join(a.get("aliases") or [])
            inds = a.get("indications") or []
            if not inds:
                w.writerow([company_id, a.get("asset_id"), a.get("asset_name"), aliases, "", "", "", ""])
                continue
            for ind in inds:
                w.writerow(
                    [
                        company_id,
                        a.get("asset_id"),
                        a.get("asset_name"),
                        aliases,
                        ind.get("stage") or "",
                        ind.get("therapeutic_area") or "",
                        ind.get("as_of_date") or "",
                        ind.get("indication") or "",
                    ]
                )


def write_changes_csv(path: Path, company_id: str, changes: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "created_at", "event_type", "asset_id", "trial_id", "evidence_id", "payload_json"])
        for c in changes:
            payload = c.get("payload")
            payload_s = json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else (payload or "")
            w.writerow(
                [
                    company_id,
                    c.get("created_at") or "",
                    c.get("event_type") or "",
                    c.get("asset_id") or "",
                    c.get("trial_id") or "",
                    c.get("evidence_id") or "",
                    payload_s,
                ]
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Export company/asset intelligence snapshots from SQLite DB.")
    ap.add_argument("--db", default="data/intel.db", help="Path to SQLite DB (default: data/intel.db)")
    ap.add_argument("--outdir", default="exports", help="Output directory (default: exports)")
    ap.add_argument("--companies", nargs="*", default=None, help="Company IDs to export (default: all in DB)")
    ap.add_argument("--changes-limit", type=int, default=2000, help="Max change events per company (default: 2000)")
    args = ap.parse_args()

    db_path = Path(args.db)
    outdir = Path(args.outdir)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    ensure_dir(outdir)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row  # type: ignore

    companies = fetch_companies(conn)
    if args.companies:
        wanted = set(args.companies)
        companies = [c for c in companies if c["company_id"] in wanted]

    generated_at = utc_now_iso()

    summary: Dict[str, Any] = {"generated_at": generated_at, "db": str(db_path), "companies": []}

    for c in companies:
        cid = c["company_id"]
        cname = c["company_name"]

        assets = fetch_assets(conn, cid)
        attach_aliases(conn, assets)
        attach_indications(conn, assets)

        changes = fetch_changes(conn, cid, limit=args.changes_limit)

        company_obj = {
            "generated_at": generated_at,
            "company_id": cid,
            "company_name": cname,
            "asset_count": len(assets),
            "change_count": len(changes),
            "assets": assets,
        }

        # Write per-company outputs
        write_json(outdir / f"{cid}_assets.json", company_obj)
        write_assets_csv(outdir / f"{cid}_assets.csv", cid, assets)

        changes_obj = {
            "generated_at": generated_at,
            "company_id": cid,
            "company_name": cname,
            "changes": changes,
        }
        write_json(outdir / f"{cid}_changes.json", changes_obj)
        write_changes_csv(outdir / f"{cid}_changes.csv", cid, changes)

        summary["companies"].append(
            {"company_id": cid, "company_name": cname, "asset_count": len(assets), "change_count": len(changes)}
        )

    write_json(outdir / "summary.json", summary)
    conn.close()

    print(f"[export] wrote exports to {outdir.resolve()}")


if __name__ == "__main__":
    main()
