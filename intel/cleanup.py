from __future__ import annotations

import argparse
from collections import OrderedDict

from sqlalchemy import create_engine, select, update, delete
from sqlalchemy.orm import Session

from . import models
from .normalize import norm_text
from .sanitize import sanitize_asset_label, sanitize_alias, is_plausible_asset_label


# Drop known OCR garbage aliases (prevents junk alias like "actorXla")
DROP_ALIAS_NORMS = {"actorxla", "actorxia", "factorxia"}


def _db_url(db: str) -> str:
    if db.startswith("sqlite:"):
        return db
    return f"sqlite:///{db}"


def merge_assets(session: Session, src: models.Asset, dst: models.Asset) -> None:
    # Move indications
    session.execute(
        update(models.AssetIndication)
        .where(models.AssetIndication.asset_id == src.id)
        .values(asset_id=dst.id)
    )

    # Move aliases (dedupe by alias_norm)
    dst_aliases = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == dst.id)
    ).scalars().all()
    dst_norms = {a.alias_norm for a in dst_aliases}

    src_aliases = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == src.id)
    ).scalars().all()
    for a in src_aliases:
        if a.alias_norm in dst_norms:
            session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
        else:
            session.execute(
                update(models.AssetAlias)
                .where(models.AssetAlias.id == a.id)
                .values(asset_id=dst.id)
            )
            dst_norms.add(a.alias_norm)

    # Move trial links, avoid UNIQUE(trial_id, asset_id)
    src_links = session.execute(
        select(models.TrialAssetLink).where(models.TrialAssetLink.asset_id == src.id)
    ).scalars().all()

    for link in src_links:
        exists = session.execute(
            select(models.TrialAssetLink.id).where(
                models.TrialAssetLink.trial_id == link.trial_id,
                models.TrialAssetLink.asset_id == dst.id,
            )
        ).first()
        if exists:
            session.execute(delete(models.TrialAssetLink).where(models.TrialAssetLink.id == link.id))
        else:
            session.execute(
                update(models.TrialAssetLink)
                .where(models.TrialAssetLink.id == link.id)
                .values(asset_id=dst.id)
            )

    # Move change events
    session.execute(
        update(models.ChangeEvent)
        .where(models.ChangeEvent.asset_id == src.id)
        .values(asset_id=dst.id)
    )

    # Delete src
    session.execute(delete(models.Asset).where(models.Asset.id == src.id))


def rebuild_aliases_for_asset(session: Session, asset_id: int) -> None:
    """
    Safe approach:
    - Read all aliases
    - Sanitize + normalize
    - Keep one per alias_norm
    - DELETE all existing aliases for this asset
    - INSERT the unique sanitized set

    This completely eliminates UNIQUE(asset_id, alias_norm) collisions during cleanup.
    """
    rows = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == asset_id)
    ).scalars().all()

    # OrderedDict keeps first occurrence (stable)
    unique: "OrderedDict[str, str]" = OrderedDict()

    for a in rows:
        new_alias = sanitize_alias(a.alias)
        if not new_alias:
            continue
        if not is_plausible_asset_label(new_alias):
            continue
        new_norm = norm_text(new_alias)
        if new_norm in DROP_ALIAS_NORMS:
            continue
        if new_norm not in unique:
            unique[new_norm] = new_alias

    # Delete all current alias rows for the asset
    session.execute(delete(models.AssetAlias).where(models.AssetAlias.asset_id == asset_id))

    # Reinsert unique set
    for alias_norm, alias in unique.items():
        session.add(models.AssetAlias(asset_id=asset_id, alias=alias, alias_norm=alias_norm))


def clean_company(session: Session, company_id: str) -> None:
    # 1) sanitize/merge canonical names first
    assets = session.execute(
        select(models.Asset).where(models.Asset.company_id == company_id)
    ).scalars().all()

    for asset in assets:
        raw = asset.canonical_name
        cleaned = sanitize_asset_label(raw) or raw

        if not is_plausible_asset_label(cleaned):
            asset.is_disclosed = False
            continue

        if cleaned != raw:
            existing = session.execute(
                select(models.Asset).where(
                    models.Asset.company_id == company_id,
                    models.Asset.canonical_name == cleaned,
                )
            ).scalar_one_or_none()

            if existing and existing.id != asset.id:
                merge_assets(session, asset, existing)
                continue

            asset.canonical_name = cleaned

    session.flush()

    # 2) rebuild aliases for each surviving asset (this is the UNIQUE-safe part)
    assets2 = session.execute(
        select(models.Asset.id).where(models.Asset.company_id == company_id)
    ).all()

    for (asset_id,) in assets2:
        rebuild_aliases_for_asset(session, int(asset_id))

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
