from __future__ import annotations

import datetime as dt
from typing import Iterable, Any

from sqlalchemy import select, delete
from sqlalchemy.orm import Session
from loguru import logger

from . import models
from .normalize import norm_text


def ensure_company(session: Session, company_id: str, name: str) -> models.Company:
    c = session.get(models.Company, company_id)
    if not c:
        c = models.Company(id=company_id, name=name)
        session.add(c)
        session.commit()
    return c


def add_evidence(
    session: Session,
    company_id: str,
    evidence_type: str,
    source_url: str,
    content_hash: str,
    content_path: str,
    meta: dict[str, Any] | None = None,
    published_at: dt.datetime | None = None,
) -> models.Evidence:
    ev = models.Evidence(
        company_id=company_id,
        evidence_type=evidence_type,
        source_url=source_url,
        content_hash=content_hash,
        content_path=content_path,
        meta=meta or {},
        published_at=published_at,
    )
    session.add(ev)
    session.commit()
    session.refresh(ev)
    return ev


def upsert_asset(session: Session, company_id: str, canonical_name: str, *, modality: str | None = None, target: str | None = None, is_disclosed: bool = True) -> models.Asset:
    stmt = select(models.Asset).where(models.Asset.company_id == company_id, models.Asset.canonical_name == canonical_name)
    asset = session.execute(stmt).scalar_one_or_none()
    if asset is None:
        asset = models.Asset(company_id=company_id, canonical_name=canonical_name, modality=modality, target=target, is_disclosed=is_disclosed)
        session.add(asset)
        session.commit()
        session.refresh(asset)
        return asset

    changed = False
    if modality and asset.modality != modality:
        asset.modality = modality
        changed = True
    if target and asset.target != target:
        asset.target = target
        changed = True
    if asset.is_disclosed != is_disclosed:
        asset.is_disclosed = is_disclosed
        changed = True

    if changed:
        session.commit()
        session.refresh(asset)
    return asset


def ensure_alias(session: Session, asset_id: int, alias: str) -> None:
    alias_norm = norm_text(alias)
    stmt = select(models.AssetAlias).where(models.AssetAlias.asset_id == asset_id, models.AssetAlias.alias_norm == alias_norm)
    if session.execute(stmt).scalar_one_or_none():
        return
    session.add(models.AssetAlias(asset_id=asset_id, alias=alias, alias_norm=alias_norm))
    session.commit()


def replace_asset_indications(
    session: Session,
    asset_id: int,
    indications: list[dict[str, Any]],
    *,
    evidence_id: int,
    as_of_date: str | None,
    therapeutic_area: str | None,
) -> tuple[int, int]:
    '''
    For MVP simplicity: replace all indications for the asset in a given evidence snapshot.
    Returns (deleted_count, inserted_count).
    '''
    # delete existing indications that came from the same evidence_id
    del_stmt = delete(models.AssetIndication).where(models.AssetIndication.asset_id == asset_id, models.AssetIndication.evidence_id == evidence_id)
    res = session.execute(del_stmt)
    deleted = res.rowcount or 0

    inserted = 0
    for row in indications:
        ind = row["indication"].strip()
        stage = row["stage"].strip()
        ta = row.get("therapeutic_area") or therapeutic_area
        session.add(models.AssetIndication(
            asset_id=asset_id,
            indication=ind,
            stage=stage,
            therapeutic_area=ta,
            as_of_date=as_of_date,
            evidence_id=evidence_id,
        ))
        inserted += 1

    session.commit()
    return deleted, inserted


def start_run(session: Session, company_id: str, run_type: str) -> models.IngestionRun:
    r = models.IngestionRun(company_id=company_id, run_type=run_type, status="running")
    session.add(r)
    session.commit()
    session.refresh(r)
    return r


def finish_run(session: Session, run_id: int, status: str, notes: str | None = None) -> None:
    r = session.get(models.IngestionRun, run_id)
    if not r:
        return
    r.status = status
    r.notes = notes
    r.finished_at = dt.datetime.utcnow()
    session.commit()


def emit_change(session: Session, company_id: str, event_type: str, payload: dict[str, Any], *, evidence_id: int | None = None, asset_id: int | None = None, trial_id: int | None = None) -> models.ChangeEvent:
    ev = models.ChangeEvent(company_id=company_id, event_type=event_type, payload=payload, evidence_id=evidence_id, asset_id=asset_id, trial_id=trial_id)
    session.add(ev)
    session.commit()
    session.refresh(ev)
    logger.info("ChangeEvent {} {} {}", company_id, event_type, payload.get("key") or "")
    return ev
