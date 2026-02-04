from __future__ import annotations

import argparse

from sqlalchemy import create_engine, select, update, delete
from sqlalchemy.orm import Session

from . import models
from .normalize import norm_text
from .sanitize import sanitize_asset_label, sanitize_alias, is_plausible_asset_label


def _db_url(db: str) -> str:
    if db.startswith("sqlite:"):
        return db
    return f"sqlite:///{db}"


def merge_assets(session: Session, src: models.Asset, dst: models.Asset) -> None:
    # Move indications
    session.execute(update(models.AssetIndication).where(models.AssetIndication.asset_id == src.id).values(asset_id=dst.id))

    # Move aliases (dedupe by alias_norm)
    existing_norms = {
        a.alias_norm
        for a in session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == dst.id)).scalars().all()
    }
    for a in session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == src.id)).scalars().all():
        if a.alias_norm in existing_norms:
            session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
        else:
            session.execute(update(models.AssetAlias).where(models.AssetAlias.id == a.id).values(asset_id=dst.id))
            existing_norms.add(a.alias_norm)

    # Move trial links, avoid UNIQUE(trial_id, asset_id) collisions
    for link in session.execute(select(models.TrialAssetLink).where(models.TrialAssetLink.asset_id == src.id)).scalars().all():
        exists = session.execute(
            select(models.TrialAssetLink.id).where(
                models.TrialAssetLink.trial_id == link.trial_id,
                models.TrialAssetLink.asset_id == dst.id,
            )
        ).first()
        if exists:
            session.execute(delete(models.TrialAssetLink).where(models.TrialAssetLink.id == link.id))
        else:
            session.execute(update(models.TrialAssetLink).where(models.TrialAssetLink.id == link.id).values(asset_id=dst.id))

    # Move change events
    session.execute(update(models.ChangeEvent).where(models.ChangeEvent.asset_id == src.id).values(asset_id=dst.id))

    # Delete src
    session.execute(delete(models.Asset).where(models.Asset.id == src.id))


def clean_company(session: Session, company_id: str) -> None:
    assets = session.execute(select(models.Asset).where(models.Asset.company_id == company_id)).scalars().all()

    for asset in assets:
        raw = asset.canonical_name
        cleaned = sanitize_asset_label(raw) or raw

        # If not plausible, hide it so reports never show it
        if not is_plausible_asset_label(cleaned):
            asset.is_disclosed = False
            continue

        # Rename/merge if sanitization changed name
        if cleaned != raw:
            existing = session.execute(
                select(models.Asset).where(models.Asset.company_id == company_id, models.Asset.canonical_name == cleaned)
            ).scalar_one_or_none()

            if existing and existing.id != asset.id:
                merge_assets(session, asset, existing)
                continue
            asset.canonical_name = cleaned

        # Clean aliases (drop garbage + dedupe)
        seen = set()
        aliases = session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == asset.id)).scalars().all()
        for a in aliases:
            new_alias = sanitize_alias(a.alias)
            if not new_alias or not is_plausible_asset_label(new_alias):
                session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                continue
            new_norm = norm_text(new_alias)
            if new_norm in seen:
                session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                continue
            seen.add(new_norm)
            a.alias = new_alias
            a.alias_norm = new_norm

    session.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/intel.db")
    ap.add_argument("--companies", nargs="*", default=None)
    args = ap.parse_args()

    engine = create_engine(_db_url(args.db), future=True)

    with Session(engine) as session:
        if args.companies:
            companies = args.companies
        else:
            companies = [c[0] for c in session.execute(select(models.Company.id)).all()]

        for cid in companies:
            print(f"[cleanup] company={cid}")
            clean_company(session, cid)

    print("[cleanup] done")


if __name__ == "__main__":
    main()
