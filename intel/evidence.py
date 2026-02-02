from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
import datetime as dt

from loguru import logger

from .settings import settings


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_suffix(evidence_type: str, url: str) -> str:
    # lightweight content-type inference
    low = url.lower()
    if low.endswith(".pdf"):
        return "pdf"
    if low.endswith(".png"):
        return "png"
    if low.endswith(".jpg") or low.endswith(".jpeg"):
        return "jpg"
    if "json" in evidence_type:
        return "json"
    return "txt"


def store_bytes(company_id: str, evidence_type: str, source_url: str, data: bytes, meta: dict[str, Any] | None = None) -> tuple[str, Path, dict]:
    settings.evidence_root.mkdir(parents=True, exist_ok=True)
    h = sha256_bytes(data)
    suffix = _safe_suffix(evidence_type, source_url)
    rel = Path(company_id) / evidence_type
    out_dir = settings.evidence_root / rel
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{h[:12]}.{suffix}"
    out_path.write_bytes(data)

    meta_out = dict(meta or {})
    meta_out.update({"sha256": h, "bytes": len(data), "stored_path": str(out_path)})

    logger.info("Stored evidence {} {} -> {}", company_id, evidence_type, out_path)
    return h, out_path, meta_out


def store_json(company_id: str, evidence_type: str, source_url: str, obj: Any, meta: dict[str, Any] | None = None) -> tuple[str, Path, dict]:
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    return store_bytes(company_id, evidence_type, source_url, data, meta=meta)
