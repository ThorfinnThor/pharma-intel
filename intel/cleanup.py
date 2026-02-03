from __future__ import annotations

import argparse
from typing import Optional

from sqlalchemy import create_engine, select, update, delete
from sqlalchemy.orm import Session

from . import models
from .normalize import norm_text
from .sanitize import sanitize_asset_label, sanitize_alias, is_plausible_asset_label


def _get_db_url(db_path: str) -> str:
    # Accept either a sqlite:/// URL or a file path
    if db_path.startswith("sqlite:"):
        return db_path
    return f"sqlite:///{db_path}"


def merge_assets(session: Session, src: models.Asset, dst: models.Asset) -> None:
    """
    Merge src asset into dst asset:
      - move indications
      - move aliases (dedup by alias_norm)
      - move trial links (dedup by (trial_id, asset_id))
      - move change events
      - delete src
    """
    # 1) indications -> dst
    session.execute(update(models.AssetIndication).where(models.AssetIndication.asset_id == src.id).values(asset_id=dst.id))

    # 2) aliases -> dst (dedupe by alias_norm)
    existing = {
        a.alias_norm
        for a in session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == dst.id)).scalars().all()
    }
    src_aliases = session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == src.id)).scalars().all()
    for a in src_aliases:
        if a.alias_norm in existing:
            session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
        else:
            session.execute(update(models.AssetAlias).where(models.AssetAlias.id == a.id).values(asset_id=dst.id))
            existing.add(a.alias_norm)

    # 3) trial links -> dst (avoid UNIQUE constraint)
    src_links = session.execute(select(models.TrialAssetLink).where(models.TrialAssetLink.asset_id == src.id)).scalars().all()
    for link in src_links:
        # if dst already has same (trial_id, dst.id), drop src link
        exists = session.execute(
            select(models.TrialAssetLink.id).where(models.TrialAssetLink.trial_id == link.trial_id, models.TrialAssetLink.asset_id == dst.id)
        ).first()
        if exists:
            session.execute(delete(models.TrialAssetLink).where(models.TrialAssetLink.id == link.id))
        else:
            session.execute(update(models.TrialAssetLink).where(models.TrialAssetLink.id == link.id).values(asset_id=dst.id))

    # 4) change events -> dst
    session.execute(update(models.ChangeEvent).where(models.ChangeEvent.asset_id == src.id).values(asset_id=dst.id))

    # 5) delete src asset
    session.execute(delete(models.Asset).where(models.Asset.id == src.id))


def clean_company(session: Session, company_id: str) -> None:
    assets = session.execute(select(models.Asset).where(models.Asset.company_id == company_id)).scalars().all()

    for asset in assets:
        raw = asset.canonical_name
        cleaned = sanitize_asset_label(raw) or raw

        # hide obvious junk
        if not is_plausible_asset_label(cleaned):
            if asset.is_disclosed:
                asset.is_disclosed = False
            continue

        # rename/merge if needed
        if cleaned != raw:
            existing = session.execute(
                select(models.Asset).where(models.Asset.company_id == company_id, models.Asset.canonical_name == cleaned)
            ).scalar_one_or_none()

            if existing and existing.id != asset.id:
                merge_assets(session, asset, existing)
                continue
            else:
                asset.canonical_name = cleaned

        # sanitize aliases
        aliases = session.execute(select(models.AssetAlias).where(models.AssetAlias.asset_id == asset.id)).scalars().all()
        seen = set()
        for a in aliases:
            new_alias = sanitize_alias(a.alias)
            if not new_alias:
                session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                continue
            new_norm = norm_text(new_alias)
            if new_norm in seen:
                session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                continue
            seen.add(new_norm)
            if a.alias != new_alias or a.alias_norm != new_norm:
                a.alias = new_alias
                a.alias_norm = new_norm

    session.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean/normalize assets and aliases in the DB.")
    ap.add_argument("--db", default="data/intel.db", help="Path to sqlite DB file (default: data/intel.db)")
    ap.add_argument("--companies", nargs="*", default=None, help="Company IDs to clean (default: all)")
    args = ap.parse_args()

    engine = create_engine(_get_db_url(args.db), future=True)

    with Session(engine) as session:
        if args.companies:
            companies = args.companies
        else:
            companies = [c[0] for c in session.execute(select(models.Company.id)).all()]

        for cid in companies:
            print(f"[cleanup] cleaning company={cid}")
            clean_company(session, cid)

    print("[cleanup] done")


if __name__ == "__main__":
    main()
