from __future__ import annotations

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from .db import get_sessionmaker, init_db
from . import models


app = FastAPI(title="Pharma Intel MVP", version="0.1")


def get_db():
    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/companies")
def list_companies(db: Session = Depends(get_db)):
    rows = db.execute(select(models.Company)).scalars().all()
    return [{"id": c.id, "name": c.name} for c in rows]


@app.get("/companies/{company_id}/assets")
def list_assets(company_id: str, db: Session = Depends(get_db)):
    rows = db.execute(select(models.Asset).where(models.Asset.company_id == company_id).order_by(models.Asset.canonical_name)).scalars().all()
    return [
        {
            "id": a.id,
            "canonical_name": a.canonical_name,
            "modality": a.modality,
            "target": a.target,
            "is_disclosed": a.is_disclosed,
        }
        for a in rows
    ]


@app.get("/companies/{company_id}/assets/{asset_id}")
def get_asset(company_id: str, asset_id: int, db: Session = Depends(get_db)):
    asset = db.get(models.Asset, asset_id)
    if not asset or asset.company_id != company_id:
        raise HTTPException(status_code=404, detail="asset not found")

    aliases = db.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == asset_id)).scalars().all()
    inds = db.execute(select(models.AssetIndication).where(models.AssetIndication.asset_id == asset_id).order_by(models.AssetIndication.id.desc())).scalars().all()

    # related trials
    links = db.execute(select(models.TrialAssetLink).where(models.TrialAssetLink.asset_id == asset_id)).scalars().all()
    trial_ids = [l.trial_id for l in links]
    trials = []
    if trial_ids:
        trs = db.execute(select(models.Trial).where(models.Trial.id.in_(trial_ids))).scalars().all()
        trials = [{"id": t.id, "nct_id": t.nct_id, "title": t.title, "status": t.overall_status, "phase": t.phase} for t in trs]

    return {
        "id": asset.id,
        "canonical_name": asset.canonical_name,
        "modality": asset.modality,
        "target": asset.target,
        "aliases": [a.alias for a in aliases],
        "indications": [
            {
                "indication": i.indication,
                "stage": i.stage,
                "therapeutic_area": i.therapeutic_area,
                "as_of_date": i.as_of_date,
                "evidence_id": i.evidence_id,
            }
            for i in inds
        ],
        "trials": trials,
    }


@app.get("/companies/{company_id}/trials")
def list_trials(company_id: str, db: Session = Depends(get_db)):
    rows = db.execute(select(models.Trial).where(models.Trial.company_id == company_id).order_by(models.Trial.last_update_posted.desc().nullslast())).scalars().all()
    return [
        {
            "id": t.id,
            "nct_id": t.nct_id,
            "title": t.title,
            "status": t.overall_status,
            "phase": t.phase,
            "last_update_posted": t.last_update_posted,
            "source_url": t.source_url,
        }
        for t in rows
    ]


@app.get("/companies/{company_id}/changes")
def list_changes(company_id: str, limit: int = 200, db: Session = Depends(get_db)):
    rows = db.execute(select(models.ChangeEvent).where(models.ChangeEvent.company_id == company_id).order_by(models.ChangeEvent.occurred_at.desc()).limit(limit)).scalars().all()
    return [
        {
            "id": c.id,
            "event_type": c.event_type,
            "occurred_at": c.occurred_at.isoformat() + "Z",
            "payload": c.payload,
            "evidence_id": c.evidence_id,
            "asset_id": c.asset_id,
            "trial_id": c.trial_id,
        }
        for c in rows
    ]


@app.get("/evidence/{evidence_id}")
def get_evidence(evidence_id: int, db: Session = Depends(get_db)):
    ev = db.get(models.Evidence, evidence_id)
    if not ev:
        raise HTTPException(status_code=404, detail="evidence not found")
    return {
        "id": ev.id,
        "company_id": ev.company_id,
        "type": ev.evidence_type,
        "source_url": ev.source_url,
        "fetched_at": ev.fetched_at.isoformat() + "Z",
        "content_hash": ev.content_hash,
        "content_path": ev.content_path,
        "meta": ev.meta,
        "published_at": ev.published_at.isoformat() + "Z" if ev.published_at else None,
    }
