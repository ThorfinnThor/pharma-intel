from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy.orm import Session

from ..http import get
from ..evidence import store_bytes
from ..repo import (
    add_evidence,
    ensure_company,
    upsert_asset,
    ensure_alias,
    replace_asset_indications,
    emit_change,
)
from ..normalize import split_asset_aliases, dedupe_preserve
from ..diff import latest_indications_before, current_indications_for_evidence, diff_sets


IMMATICS_PIPELINE_PAGE = "https://immatics.com/our-pipeline/"


def _find_pipeline_image_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    # heuristic: find the largest 'pipeline' image (often uploaded as a dated PNG)
    candidates = []
    for img in soup.find_all("img"):
        src = img.get("src") or ""
        if not src:
            continue
        low = src.lower()
        if "pipeline" in low and (low.endswith(".png") or low.endswith(".jpg") or low.endswith(".jpeg")):
            candidates.append(src)
    if not candidates:
        # sometimes linked as an <a> around an image
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            low = href.lower()
            if "pipeline" in low and (low.endswith(".png") or low.endswith(".jpg") or low.endswith(".jpeg")):
                candidates.append(href)
    return candidates[0] if candidates else None


def _extract_asset_names_from_page_text(html: str) -> list[str]:
    # page text contains a paragraph enumerating key candidates (anzu-cel, IMA203CD8, IMA402, etc.)
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    # grab patterns: "anzu-cel (... IMA203)" ; "IMA203CD8" ; "IMA402"
    found = []
    # anzu-cel (.... IMA203)
    m = re.search(r"(anzu-cel\s*\(.*?IMA203\))", text, re.IGNORECASE)
    if m:
        found.append(m.group(1))
    for token in ["IMA203", "IMA203CD8", "IMA402", "IMA401", "mRNA-4203", "anzutresgene autoleucel", "anzu-cel"]:
        if re.search(rf"\b{re.escape(token)}\b", text):
            found.append(token)
    return dedupe_preserve(found)


def _load_curated_assets(curated_file: str | None) -> list[dict[str, Any]]:
    if not curated_file:
        return []
    path = Path(curated_file)
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text())
    return raw.get("assets", [])


def ingest_immatics_pipeline(session: Session, company_id: str = "immatics", curated_file: str | None = "configs/immatics_curated_assets.yaml") -> int:
    ensure_company(session, company_id, "Immatics")

    # 1) fetch pipeline page HTML evidence
    resp = get(IMMATICS_PIPELINE_PAGE)
    html = resp.text
    h_hash, h_path, h_meta = store_bytes(company_id, "pipeline_html", IMMATICS_PIPELINE_PAGE, html.encode("utf-8"), meta={"content_type": resp.headers.get("content-type")})
    html_ev = add_evidence(session, company_id, "pipeline_html", IMMATICS_PIPELINE_PAGE, h_hash, str(h_path), meta=h_meta)

    # 2) fetch pipeline image evidence if present
    img_url = _find_pipeline_image_url(html)
    img_ev = None
    if img_url:
        img_bytes = get(img_url).content
        i_hash, i_path, i_meta = store_bytes(company_id, "pipeline_image", img_url, img_bytes, meta={"source": "immatics_pipeline_image"})
        img_ev = add_evidence(session, company_id, "pipeline_image", img_url, i_hash, str(i_path), meta=i_meta)

    # 3) build asset list from curated seed + page text
    curated_assets = _load_curated_assets(curated_file)
    page_assets = _extract_asset_names_from_page_text(html)

    # curated assets are authoritative for indications/stages in this MVP
    assets_seen = set()

    evidence_for_indications = img_ev.id if img_ev else html_ev.id

    for ca in curated_assets:
        label = ca["name"]
        canonical, aliases = split_asset_aliases(label)
        asset = upsert_asset(session, company_id, canonical, modality=ca.get("modality"), target=ca.get("target"), is_disclosed=ca.get("is_disclosed", True))
        for a in aliases + ca.get("aliases", []):
            ensure_alias(session, asset.id, a)

        # diff indications
        old = latest_indications_before(session, asset.id, evidence_for_indications)

        indications = []
        for ind in ca.get("indications", []):
            indications.append({
                "indication": ind["indication"],
                "stage": ind.get("stage", "Unknown"),
                "therapeutic_area": ind.get("therapeutic_area"),
            })

        replace_asset_indications(session, asset.id, indications, evidence_id=evidence_for_indications, as_of_date=ca.get("as_of_date"), therapeutic_area=None)

        new = current_indications_for_evidence(session, asset.id, evidence_for_indications)
        added, removed = diff_sets(old, new)

        if not old and new:
            emit_change(session, company_id, "asset_added", {"asset": canonical}, evidence_id=evidence_for_indications, asset_id=asset.id)

        for (ind, stage, ta) in added:
            emit_change(session, company_id, "asset_indication_added", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence_for_indications, asset_id=asset.id)
        for (ind, stage, ta) in removed:
            emit_change(session, company_id, "asset_indication_removed", {"asset": canonical, "indication": ind, "stage": stage, "therapeutic_area": ta}, evidence_id=evidence_for_indications, asset_id=asset.id)

        assets_seen.add(canonical)

    # add extra aliases from page text (non-destructive)
    for token in page_assets:
        # attach to closest curated asset if obvious
        low = token.lower()
        if "anzu" in low or "ima203" in low:
            target_asset = "anzu-cel"
        elif "ima402" in low:
            target_asset = "IMA402"
        elif "ima401" in low:
            target_asset = "IMA401"
        elif "ima203cd8" in low:
            target_asset = "IMA203CD8"
        else:
            continue

        # find asset
        from sqlalchemy import select
        from .. import models
        asset = session.execute(select(models.Asset).where(models.Asset.company_id == company_id, models.Asset.canonical_name == target_asset)).scalar_one_or_none()
        if asset:
            ensure_alias(session, asset.id, token)

    emit_change(session, company_id, "pipeline_ingested", {"pipeline_page": IMMATICS_PIPELINE_PAGE, "pipeline_image": img_url, "assets_seen": len(assets_seen)}, evidence_id=evidence_for_indications)
    return len(assets_seen)
