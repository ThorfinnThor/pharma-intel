from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Optional

import requests
from loguru import logger
from sqlalchemy.orm import Session

from .evidence import store_json
from .repo import add_evidence
from .settings import settings


_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:
    """Extract the first JSON object found in a model response."""
    if not text:
        return None
    m = _JSON_OBJECT_RE.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _prompt_hash(company_id: str, raw_label: str, context: str) -> str:
    h = hashlib.sha256()
    h.update(company_id.encode("utf-8"))
    h.update(b"\n")
    h.update(raw_label.encode("utf-8"))
    h.update(b"\n")
    h.update(context.encode("utf-8"))
    return h.hexdigest()


def _cache_path(prompt_hash: str) -> Path:
    root = Path("data/llm_cache/gemini_asset_clean")
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{prompt_hash}.json"


def _gemini_generate(prompt: str) -> str:
    """Call Gemini REST API and return the raw text output."""
    if not settings.gemini_api_key:
        raise RuntimeError("Gemini API key missing; set PHARMA_INTEL_GEMINI_API_KEY")

    model = settings.gemini_model
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    params = {"key": settings.gemini_api_key}
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 512,
        },
    }

    r = requests.post(url, params=params, json=payload, timeout=settings.gemini_timeout_s)
    r.raise_for_status()
    data = r.json()

    # Typical shape: candidates[0].content.parts[0].text
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        # Preserve some diagnostics without dumping huge payloads
        return json.dumps(data)[:2000]


def llm_classify_and_canonicalize_asset_label(
    session: Session,
    company_id: str,
    raw_label: str,
    *,
    context: str,
    source_url: str,
) -> Optional[dict[str, Any]]:
    """Optionally use Gemini to classify+canonicalize a borderline "asset label".

    Returns:
      - None if LLM cleaning is disabled
      - A dict:
          {"is_asset": bool, "canonical_name": str|None, "aliases": list[str], "evidence_id": int|None}

    Notes:
      - Output is cached on disk by a hash of inputs.
      - The returned decision is stored as evidence for auditability.
    """
    if not settings.llm_clean_enabled:
        return None

    raw_label = (raw_label or "").strip()
    if not raw_label:
        return {"is_asset": False, "canonical_name": None, "aliases": [], "evidence_id": None}

    context = (context or "").strip()
    ph = _prompt_hash(company_id, raw_label, context)
    cache_file = _cache_path(ph)

    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            return cached
        except Exception:
            # ignore bad cache and recompute
            pass

    prompt = f"""You are cleaning extracted pharma pipeline labels.

Task:
- Decide whether RAW_LABEL is a drug/program/intervention name (asset) or an indication/disease/other non-asset.
- If it is an asset: return a cleaned canonical name and 1-10 aliases that appear directly in the label.
- If it is NOT an asset: return is_asset=false.

Rules:
- Do NOT invent or guess new drug names.
- Only normalize/clean/trim strings that appear in the label.
- If uncertain, set is_asset=false.
- Output MUST be valid JSON and NOTHING ELSE.

Return JSON schema:
{{
  "is_asset": true|false,
  "canonical_name": "..." | null,
  "aliases": ["...", ...]
}}

RAW_LABEL: {raw_label}
CONTEXT (nearby lines from same PDF column):
{context}
"""

    try:
        raw_out = _gemini_generate(prompt)
    except Exception as e:
        logger.warning("Gemini call failed for label='{}' ({})", raw_label, e)
        return {"is_asset": False, "canonical_name": None, "aliases": [], "evidence_id": None}

    parsed = _extract_json_object(raw_out)
    if not isinstance(parsed, dict):
        logger.warning("Gemini returned non-JSON for label='{}': {}", raw_label, raw_out[:200])
        parsed = {"is_asset": False, "canonical_name": None, "aliases": []}

    # normalize fields
    is_asset = bool(parsed.get("is_asset"))
    canonical = parsed.get("canonical_name") if is_asset else None
    aliases = parsed.get("aliases") if is_asset else []

    if canonical is not None and not isinstance(canonical, str):
        canonical = None
    if not isinstance(aliases, list):
        aliases = []
    aliases = [a.strip() for a in aliases if isinstance(a, str) and a.strip()]

    result: dict[str, Any] = {
        "is_asset": is_asset,
        "canonical_name": canonical.strip() if isinstance(canonical, str) and canonical.strip() else None,
        "aliases": aliases,
        "evidence_id": None,
    }

    # Persist decision as evidence (audit trail)
    try:
        content_hash, path, meta = store_json(
            company_id,
            "llm_asset_clean",
            source_url,
            {"raw_label": raw_label, "context": context, "model": settings.gemini_model, "result": result},
            meta={"prompt_hash": ph},
        )
        ev = add_evidence(session, company_id, "llm_asset_clean", source_url, content_hash, str(path), meta=meta)
        result["evidence_id"] = int(ev.id)
        session.commit()
    except Exception as e:
        # Evidence is nice-to-have; do not block pipeline ingestion
        logger.warning("Failed to persist LLM cleaning evidence ({} )", e)
        try:
            session.rollback()
        except Exception:
            pass

    # write cache
    try:
        cache_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    return result
