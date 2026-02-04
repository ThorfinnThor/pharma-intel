from __future__ import annotations

import re
import io
import os
import datetime as dt
from typing import Any

import pdfplumber
import requests
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy.orm import Session

from ..http import get
from ..settings import settings
from ..evidence import store_bytes
from ..repo import add_evidence, ensure_company, upsert_asset, ensure_alias, replace_asset_indications, emit_change
from ..normalize import split_asset_aliases
from ..diff import latest_indications_before, current_indications_for_evidence, diff_sets
from ..sanitize import (
    sanitize_asset_label,
    sanitize_alias,
    sanitize_indication_text,
    is_plausible_asset_label,
    looks_like_indication_label,
)

# Optional (feature-flagged) LLM cleaner for borderline labels
try:
    from ..llm_clean import llm_classify_and_canonicalize_asset_label
except Exception:  # pragma: no cover
    llm_classify_and_canonicalize_asset_label = None  # type: ignore


JNJ_PIPELINE_PAGE = "https://www.investor.jnj.com/pipeline/development-pipeline/default.aspx"

Q4CDN_BASE = "https://s203.q4cdn.com/636242992/files/doc_financials"


def _iter_recent_quarters(n: int = 10) -> list[tuple[int, int]]:
    today = dt.datetime.utcnow().date()
    q = (today.month - 1) // 3 + 1
    y = today.year

    if q == 1:
        y -= 1
        q = 4
    else:
        q -= 1

    out: list[tuple[int, int]] = []
    for _ in range(max(1, n)):
        out.append((y, q))
        if q == 1:
            y -= 1
            q = 4
        else:
            q -= 1
    return out


def _candidate_jnj_pdf_urls(max_quarters: int = 10) -> list[str]:
    urls: list[str] = []
    for year, quarter in _iter_recent_quarters(max_quarters):
        yy = str(year)[2:]
        folder_variants = [
            f"{Q4CDN_BASE}/{year}/q{quarter}",
            f"{Q4CDN_BASE}/{year}/Q{quarter}",
        ]
        filename_variants = [
            f"JNJ-Pipeline-{quarter}Q{yy}.pdf",
            f"JNJ-Pipeline-{quarter}Q{year}.pdf",
        ]
        for folder in folder_variants:
            for fname in filename_variants:
                urls.append(f"{folder}/{fname}")
    return urls


def _url_looks_like_pdf(url: str) -> bool:
    headers = {
        "User-Agent": settings.http_user_agent,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Range": "bytes=0-1023",
    }
    timeout = min(int(settings.http_timeout_s), 15)
    r = None
    try:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code not in (200, 206):
            return False
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "pdf" in ctype:
            return True
        return url.lower().endswith(".pdf")
    except Exception:
        return False
    finally:
        try:
            if r is not None:
                r.close()
        except Exception:
            pass


def discover_jnj_pipeline_pdf_url(max_quarters: int = 10) -> str:
    candidates = _candidate_jnj_pdf_urls(max_quarters=max_quarters)
    for url in candidates:
        if _url_looks_like_pdf(url):
            logger.info("Discovered J&J pipeline PDF URL via q4cdn: {}", url)
            return url
    raise RuntimeError("Could not discover a J&J pipeline PDF URL from q4cdn candidates")


def _find_pdf_url(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        text = (a.get_text() or "").strip().lower()
        if href.lower().endswith(".pdf") and ("pipeline" in href.lower() or "pipeline" in text or "download report" in text):
            return href
    raise ValueError("Could not find pipeline PDF link on page")


def _parse_as_of_date_from_pdf_text(text: str) -> str | None:
    m = re.search(r"as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, re.IGNORECASE)
    if not m:
        return None
    try:
        d = dt.datetime.strptime(m.group(1), "%B %d, %Y").date()
        return d.isoformat()
    except Exception:
        return None


THERA_AREA_PAT = re.compile(r"^(Oncology|Immunology|Neuroscience|Select Other Areas)\b", re.IGNORECASE)


def _therapeutic_area_from_page_text(text: str) -> str | None:
    for line in (text or "").splitlines():
        line = line.strip()
        m = THERA_AREA_PAT.match(line)
        if m:
            return m.group(1).title()
    return None


def _extract_phase_columns(page) -> dict[str, tuple[float, float]]:
    words = page.extract_words(extra_attrs=["size"])
    header_words = [w for w in words if w["top"] < 90 and w["text"]]
    phases = []
    reg_x = None
    for w in header_words:
        t = w["text"].strip()
        if t.lower() == "phase":
            phases.append(w["x0"])
        if t.lower().startswith("registration"):
            reg_x = w["x0"]

    phases = sorted(phases)
    if len(phases) < 3 or reg_x is None:
        width = page.width
        left = width * 0.25
        right = width * 0.95
        colw = (right - left) / 4.0
        return {
            "Phase 1": (left + 0 * colw, left + 1 * colw),
            "Phase 2": (left + 1 * colw, left + 2 * colw),
            "Phase 3": (left + 2 * colw, left + 3 * colw),
            "Registration": (left + 3 * colw, right),
        }

    x1, x2, x3 = phases[:3]
    b12 = (x1 + x2) / 2
    b23 = (x2 + x3) / 2
    b3r = (x3 + reg_x) / 2
    left = x1 - 40
    right = page.width - 10
    return {
        "Phase 1": (left, b12),
        "Phase 2": (b12, b23),
        "Phase 3": (b23, b3r),
        "Registration": (b3r, right),
    }


def _group_words_to_lines(words: list[dict[str, Any]], y_tol: float = 3.0) -> list[dict[str, Any]]:
    words_sorted = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: list[list[dict[str, Any]]] = []
    for w in words_sorted:
        if not lines:
            lines.append([w])
            continue
        if abs(w["top"] - lines[-1][0]["top"]) <= y_tol:
            lines[-1].append(w)
        else:
            lines.append([w])

    out = []
    for ws in lines:
        ws = sorted(ws, key=lambda w: w["x0"])
        text = " ".join(w["text"] for w in ws).strip()
        if not text:
            continue
        avg_size = sum(float(w.get("size") or 0) for w in ws) / max(len(ws), 1)
        out.append({"text": text, "top": ws[0]["top"], "avg_size": avg_size})
    return out


def _is_asset_line(line: dict[str, Any], median_size: float) -> bool:
    raw = (line.get("text") or "").strip()
    if not raw:
        return False

    cleaned = sanitize_asset_label(raw)
    if not cleaned:
        return False

    # Hard reject: this is very likely an indication/disease label.
    if looks_like_indication_label(cleaned):
        return False

    # Reject non-plausible labels early.
    if not is_plausible_asset_label(cleaned):
        return False

    low = cleaned.lower()

    if low in {"pediatrics", "oncology", "immunology", "neuroscience"}:
        return False
    if low.startswith("*this is not") or low.startswith("strategic partnerships"):
        return False

    # explicit program codes
    if "jnj-" in low:
        return True

    # We treat font size as a supporting signal, not sufficient on its own.
    big_font = line["avg_size"] >= (median_size + 0.8)

    # Explicit patterns that strongly indicate assets
    if re.match(r"^.{2,60}\(.{2,60}\)$", cleaned) and re.search(r"[A-Za-z]", cleaned):
        return True

    # Single-token drug/program name (e.g., icotrokinra, nipocalimab)
    if " " not in cleaned and 4 <= len(cleaned) <= 28 and re.search(r"[a-z]", cleaned):
        if cleaned.lower() not in {"others", "other", "unknown", "undisclosed"}:
            # allow if it looks like a drug suffix or if it's in a big font
            if re.search(r"(mab|nib|ciclib|stat|navir|vir)$", cleaned, re.IGNORECASE) or big_font:
                return True

    # ALL CAPS brand is common
    if cleaned.isupper() and 3 <= len(cleaned) <= 45:
        return True

    # If it's big font and short, and not disease-like, treat as asset line.
    if big_font and 1 <= len(cleaned.split()) <= 3 and len(cleaned) <= 40:
        return True

    return False


def parse_jnj_pipeline_pdf(pdf_bytes: bytes) -> dict[str, Any]:
    rows: list[dict[str, str]] = []
    as_of_date = None

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        as_of_date = _parse_as_of_date_from_pdf_text(first_text)

        for p in pdf.pages:
            p_text = p.extract_text() or ""
            ta = _therapeutic_area_from_page_text(p_text)

            cols = _extract_phase_columns(p)
            words = p.extract_words(extra_attrs=["size"])
            body_words = [w for w in words if 90 <= w["top"] <= (p.height - 80) and w["text"].strip()]

            sizes = sorted(float(w.get("size") or 0) for w in body_words if w.get("size"))
            median = sizes[len(sizes) // 2] if sizes else 10.0

            for stage, (x0, x1) in cols.items():
                col_words = [w for w in body_words if (x0 <= w["x0"] < x1)]
                lines = _group_words_to_lines(col_words)

                current_asset: str | None = None
                indication_parts: list[str] = []

                def flush():
                    nonlocal current_asset, indication_parts
                    if not current_asset:
                        return
                    ind = sanitize_indication_text(" ".join(indication_parts).strip())
                    if not ind:
                        return
                    # drop absurdly long indications (usually PDF footer leakage)
                    if len(ind) > 220:
                        return
                    rows.append(
                        {
                            "asset_label": current_asset,
                            "stage": stage,
                            "indication": ind,
                            "therapeutic_area": ta or None,
                        }
                    )

                for ln in lines:
                    if _is_asset_line(ln, median):
                        flush()
                        cleaned = sanitize_asset_label(ln["text"])
                        if cleaned and is_plausible_asset_label(cleaned):
                            current_asset = cleaned
                            indication_parts = []
                        else:
                            current_asset = None
                            indication_parts = []
                    else:
                        if current_asset:
                            indication_parts.append(ln["text"].strip())

                flush()

    # remove junk rows where indication looks like footer
    cleaned_rows = []
    for r in rows:
        ind_low = (r["indication"] or "").lower()
        if ind_low.startswith("strategic partnerships"):
            continue
        if ind_low.startswith("*this is not"):
            continue
        cleaned_rows.append(r)

    return {"as_of_date": as_of_date, "rows": cleaned_rows}


def ingest_jnj_pipeline(session: Session, company_id: str = "jnj") -> int:
    ensure_company(session, company_id, "Johnson & Johnson")

    pdf_url = os.getenv("PHARMA_INTEL_JNJ_PIPELINE_PDF_URL")

    if not pdf_url:
        try:
            html = get(JNJ_PIPELINE_PAGE).text
            pdf_url = _find_pdf_url(html)
            if pdf_url.startswith("/"):
                pdf_url = "https://www.investor.jnj.com" + pdf_url
        except Exception as e:
            logger.warning(
                "Failed to fetch/parse J&J pipeline HTML ({}). Falling back to q4cdn discovery.",
                e,
            )
            pdf_url = discover_jnj_pipeline_pdf_url(max_quarters=10)

    pdf_bytes = get(pdf_url).content
    content_hash, path, meta = store_bytes(company_id, "pipeline_pdf", pdf_url, pdf_bytes, meta={"source": "jnj_q4_pipeline"})
    evidence = add_evidence(session, company_id, "pipeline_pdf", pdf_url, content_hash, str(path), meta=meta)

    parsed = parse_jnj_pipeline_pdf(pdf_bytes)
    as_of_date = parsed.get("as_of_date")
    rows: list[dict[str, str]] = parsed["rows"]
    logger.info("Parsed {} J&J pipeline rows (as_of={})", len(rows), as_of_date)

    by_asset: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        by_asset.setdefault(r["asset_label"], []).append(r)

    # Quality gate: warn if many labels look like indications (regression detector)
    suspicious = [a for a in by_asset.keys() if looks_like_indication_label(a)]
    if suspicious:
        frac = len(suspicious) / max(len(by_asset), 1)
        if frac >= 0.05:
            logger.warning(
                "JNJ pipeline parse quality: {} / {} labels look like indications ({}%). Sample: {}",
                len(suspicious),
                len(by_asset),
                int(frac * 100),
                suspicious[:10],
            )

    for asset_label, recs in by_asset.items():
        cleaned_label = sanitize_asset_label(asset_label)
        if not cleaned_label:
            continue

        # Heuristic rejection for disease-like labels (prevents garbage assets)
        if looks_like_indication_label(cleaned_label):
            # Optional: let an LLM rescue borderline cases (feature-flagged)
            if settings.llm_clean_enabled and llm_classify_and_canonicalize_asset_label is not None:
                ctx = "\n".join((r.get("indication") or "").strip() for r in recs[:3] if r.get("indication"))
                llm = llm_classify_and_canonicalize_asset_label(
                    session,
                    company_id,
                    cleaned_label,
                    context=ctx,
                    source_url=pdf_url,
                )
                if not llm or not llm.get("is_asset"):
                    continue
                cleaned_label = llm.get("canonical_name") or cleaned_label
                llm_aliases = llm.get("aliases") or []
            else:
                continue
        else:
            llm_aliases = []

        if not is_plausible_asset_label(cleaned_label):
            continue

        canonical, aliases = split_asset_aliases(cleaned_label)
        canonical = sanitize_asset_label(canonical) or canonical
        if not is_plausible_asset_label(canonical):
            continue

        # merge in optional LLM aliases (already constrained to label text)
        for a in llm_aliases:
            if a and a not in aliases:
                aliases.append(a)

        asset = upsert_asset(session, company_id, canonical)

        for a in aliases:
            aa = sanitize_alias(a)
            if aa and is_plausible_asset_label(aa):
                ensure_alias(session, asset.id, aa)

        indications = []
        for r in recs:
            indications.append(
                {
                    "indication": sanitize_indication_text(r["indication"]),
                    "stage": r["stage"],
                    "therapeutic_area": r.get("therapeutic_area"),
                }
            )

        old = latest_indications_before(session, asset.id, evidence.id)
        replace_asset_indications(session, asset.id, indications, evidence_id=evidence.id, as_of_date=as_of_date, therapeutic_area=None)
        new = current_indications_for_evidence(session, asset.id, evidence.id)
        added, removed = diff_sets(old, new)

        for (ind, stage, ta) in added:
            emit_change(session, company_id, "asset_indication_added", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence.id, asset_id=asset.id)
        for (ind, stage, ta) in removed:
            emit_change(session, company_id, "asset_indication_removed", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence.id, asset_id=asset.id)

    emit_change(session, company_id, "pipeline_ingested", {"as_of_date": as_of_date, "pdf_url": pdf_url, "assets_seen": len(by_asset)}, evidence_id=evidence.id)
    return len(by_asset)
