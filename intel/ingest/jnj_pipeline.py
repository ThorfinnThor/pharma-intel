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


JNICALL_PIPELINE_PAGE = "https://www.investor.jnj.com/pipeline/development-pipeline/default.aspx"

# J&J also hosts the pipeline PDF on its IR CDN (Q4 / s203.q4cdn.com). Investor-facing
# HTML pages are sometimes protected and may return 403 from hosted CI runners.
Q4CDN_BASE = "https://s203.q4cdn.com/636242992/files/doc_financials"


def _iter_recent_quarters(n: int = 10) -> list[tuple[int, int]]:
    """Return (year, quarter) pairs, starting from the previous quarter going backwards."""
    today = dt.datetime.utcnow().date()
    q = (today.month - 1) // 3 + 1
    y = today.year

    # start from previous quarter (pipeline PDFs are typically posted after quarter close)
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
    """Generate likely pipeline PDF URLs on the q4cdn host.

    Observed pattern (example):
      .../2025/q4/JNJ-Pipeline-4Q25.pdf
    """
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
    """Cheaply validate that a URL is an accessible PDF without downloading the whole file."""
    headers = {
        "User-Agent": settings.http_user_agent,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        # Try range request to avoid full download; many CDNs support 206.
        "Range": "bytes=0-1023",
    }
    timeout = min(int(settings.http_timeout_s), 15)
    try:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code not in (200, 206):
            return False
        ctype = (r.headers.get("Content-Type") or "").lower()
        # Some CDNs return octet-stream for PDFs; accept based on extension as well.
        if "pdf" in ctype:
            return True
        return url.lower().endswith(".pdf")
    except Exception:
        return False
    finally:
        try:
            r.close()  # type: ignore[name-defined]
        except Exception:
            pass


def discover_jnj_pipeline_pdf_url(max_quarters: int = 10) -> str:
    """Find the most recent accessible J&J pipeline PDF on q4cdn."""
    candidates = _candidate_jnj_pdf_urls(max_quarters=max_quarters)
    for url in candidates:
        if _url_looks_like_pdf(url):
            logger.info("Discovered J&J pipeline PDF URL via q4cdn: {}", url)
            return url
    raise RuntimeError("Could not discover a J&J pipeline PDF URL from q4cdn candidates")


def _find_pdf_url(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # J&J Q4 pages typically link to a q4cdn PDF for the report
    # We prefer a direct PDF link that looks like JNJ-Pipeline-*.pdf
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        text = (a.get_text() or "").strip().lower()
        if href.lower().endswith(".pdf") and ("pipeline" in href.lower() or "pipeline" in text or "download report" in text):
            return href
    raise ValueError("Could not find pipeline PDF link on page")


def _parse_as_of_date_from_pdf_text(text: str) -> str | None:
    # "Selected Innovative Medicines in Development as of January 21, 2026"
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
    # e.g., "Oncology (1 of 3)"
    for line in (text or "").splitlines():
        line = line.strip()
        m = THERA_AREA_PAT.match(line)
        if m:
            return m.group(1).title()
    return None


def _extract_phase_columns(page) -> dict[str, tuple[float, float]]:
    # Find the x positions of headers "Phase" (x3) and "Registration"
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
        # fallback approximate split
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
    # Determine boundary midpoints
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
    # words assumed already filtered to a column
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
    t = line["text"].strip()
    if not t:
        return False

    low = t.lower()

    if low in {"pediatrics", "oncology", "immunology", "neuroscience"}:
        return False
    if low.startswith("*this is not") or low.startswith("strategic partnerships"):
        return False

    if "jnj-" in low:
        return True

    # typical assets are visually larger in the PDF
    if line["avg_size"] >= (median_size + 0.6):
        return True

    # brand (generic)
    if re.match(r"^.{2,60}\(.{2,60}\)$", t) and re.search(r"[a-z]", t):
        return True

    # single token (e.g., icotrokinra)
    if " " not in t and 4 <= len(t) <= 25 and re.search(r"[a-z]", t):
        # try to avoid generic words like "others"
        if t.lower() not in {"others", "other", "unknown", "undisclosed"}:
            return True

    # all caps brand
    if t.isupper() and 3 <= len(t) <= 45:
        return True

    return False


def parse_jnj_pipeline_pdf(pdf_bytes: bytes) -> dict[str, Any]:
    '''
    Returns:
    {
      "as_of_date": "YYYY-MM-DD" | None,
      "rows": [ {asset_label, stage, indication, therapeutic_area}, ... ]
    }
    '''
    rows: list[dict[str, str]] = []
    as_of_date = None

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # first page often contains the as-of date in header
        first_text = pdf.pages[0].extract_text() or ""
        as_of_date = _parse_as_of_date_from_pdf_text(first_text)

        for p in pdf.pages:
            p_text = p.extract_text() or ""
            ta = _therapeutic_area_from_page_text(p_text)

            cols = _extract_phase_columns(p)
            words = p.extract_words(extra_attrs=["size"])

            # focus on body (skip header/footer)
            body_words = [w for w in words if 90 <= w["top"] <= (p.height - 80) and w["text"].strip()]

            # compute median size for heuristic
            sizes = sorted(float(w.get("size") or 0) for w in body_words if w.get("size"))
            median = sizes[len(sizes)//2] if sizes else 10.0

            for stage, (x0, x1) in cols.items():
                col_words = [w for w in body_words if (x0 <= w["x0"] < x1)]
                lines = _group_words_to_lines(col_words)

                current_asset: str | None = None
                indication_parts: list[str] = []

                for ln in lines:
                    if _is_asset_line(ln, median):
                        # flush previous
                        if current_asset and indication_parts:
                            rows.append({
                                "asset_label": current_asset,
                                "stage": stage,
                                "indication": " ".join(indication_parts).strip(),
                                "therapeutic_area": ta or None,
                            })
                        current_asset = ln["text"].strip()
                        indication_parts = []
                    else:
                        if current_asset:
                            indication_parts.append(ln["text"].strip())

                # flush end
                if current_asset and indication_parts:
                    rows.append({
                        "asset_label": current_asset,
                        "stage": stage,
                        "indication": " ".join(indication_parts).strip(),
                        "therapeutic_area": ta or None,
                    })

    # remove junk rows where indication looks like footer
    cleaned = []
    for r in rows:
        ind = r["indication"]
        if ind.lower().startswith("strategic partnerships"):
            continue
        if ind.lower().startswith("*this is not"):
            continue
        cleaned.append(r)

    return {"as_of_date": as_of_date, "rows": cleaned}


def ingest_jnj_pipeline(session: Session, company_id: str = "jnj") -> int:
    ensure_company(session, company_id, "Johnson & Johnson")

    # 1) Determine the pipeline PDF URL.
    #
    # Some investor relations sites (including investor.jnj.com) may return 403 to
    # GitHub-hosted runners or other datacenter IPs. The PDF itself is commonly
    # hosted on the q4cdn domain and is often accessible from CI.
    #
    # Priority order:
    #   (a) explicit override via env PHARMA_INTEL_JNJ_PIPELINE_PDF_URL
    #   (b) parse the investor pipeline page for a PDF link
    #   (c) discover the latest PDF on q4cdn using common naming patterns

    pdf_url = os.getenv("PHARMA_INTEL_JNJ_PIPELINE_PDF_URL")

    if not pdf_url:
        try:
            html = get(JNICALL_PIPELINE_PAGE).text
            pdf_url = _find_pdf_url(html)
            if pdf_url.startswith("/"):
                pdf_url = "https://www.investor.jnj.com" + pdf_url
        except Exception as e:
            logger.warning(
                "Failed to fetch/parse J&J pipeline HTML ({}). Falling back to q4cdn discovery.",
                e,
            )
            pdf_url = discover_jnj_pipeline_pdf_url(max_quarters=10)

    # 2) download PDF and store as evidence
    pdf_bytes = get(pdf_url).content
    content_hash, path, meta = store_bytes(company_id, "pipeline_pdf", pdf_url, pdf_bytes, meta={"source": "jnj_q4_pipeline"})
    evidence = add_evidence(session, company_id, "pipeline_pdf", pdf_url, content_hash, str(path), meta=meta)

    # 3) parse pipeline table from PDF
    parsed = parse_jnj_pipeline_pdf(pdf_bytes)
    as_of_date = parsed.get("as_of_date")
    rows: list[dict[str, str]] = parsed["rows"]
    logger.info("Parsed {} J&J pipeline rows (as_of={})", len(rows), as_of_date)

    # 4) upsert assets + indications (emit change events)
    # group by asset label
    by_asset: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        by_asset.setdefault(r["asset_label"], []).append(r)

    inserted_assets = 0

    for asset_label, recs in by_asset.items():
        canonical, aliases = split_asset_aliases(asset_label)
        asset = upsert_asset(session, company_id, canonical)
        for a in aliases:
            ensure_alias(session, asset.id, a)

        # build indications list for this snapshot
        indications = [{"indication": r["indication"], "stage": r["stage"], "therapeutic_area": r.get("therapeutic_area")} for r in recs]

        # diff vs prior snapshot for this asset
        old = latest_indications_before(session, asset.id, evidence.id)

        # replace snapshot indications for this evidence
        replace_asset_indications(session, asset.id, indications, evidence_id=evidence.id, as_of_date=as_of_date, therapeutic_area=None)

        new = current_indications_for_evidence(session, asset.id, evidence.id)
        added, removed = diff_sets(old, new)

        if not old and new:
            inserted_assets += 1
            emit_change(session, company_id, "asset_added", {"asset": canonical}, evidence_id=evidence.id, asset_id=asset.id)

        for (ind, stage, ta) in added:
            emit_change(session, company_id, "asset_indication_added", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence.id, asset_id=asset.id)

        for (ind, stage, ta) in removed:
            emit_change(session, company_id, "asset_indication_removed", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence.id, asset_id=asset.id)

    emit_change(session, company_id, "pipeline_ingested", {"as_of_date": as_of_date, "pdf_url": pdf_url, "assets_seen": len(by_asset)}, evidence_id=evidence.id)
    return len(by_asset)
