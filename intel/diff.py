from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import models


def _ind_key(ind: models.AssetIndication) -> tuple[str, str, str | None]:
    return (ind.indication.strip(), ind.stage.strip(), (ind.therapeutic_area or "").strip() or None)


def current_indications_for_evidence(session: Session, asset_id: int, evidence_id: int) -> set[tuple[str, str, str | None]]:
    stmt = select(models.AssetIndication).where(models.AssetIndication.asset_id == asset_id, models.AssetIndication.evidence_id == evidence_id)
    rows = session.execute(stmt).scalars().all()
    return {_ind_key(r) for r in rows}


def latest_indications_before(session: Session, asset_id: int, evidence_id: int) -> set[tuple[str, str, str | None]]:
    '''
    Get the most recent snapshot (by evidence_id) for this asset, excluding the provided evidence_id.
    '''
    stmt = (
        select(models.AssetIndication)
        .where(models.AssetIndication.asset_id == asset_id, models.AssetIndication.evidence_id != evidence_id)
        .order_by(models.AssetIndication.id.desc())
        .limit(5000)
    )
    rows = session.execute(stmt).scalars().all()
    if not rows:
        return set()
    # pick the latest evidence_id among those rows
    latest_evid = max(r.evidence_id for r in rows)
    return { _ind_key(r) for r in rows if r.evidence_id == latest_evid }


def diff_sets(old: set, new: set) -> tuple[set, set]:
    added = new - old
    removed = old - new
    return added, removed
