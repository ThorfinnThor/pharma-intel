from __future__ import annotations

import argparse
from collections import defaultdict

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
    session.execute(
        update(models.AssetIndication)
        .where(models.AssetIndication.asset_id == src.id)
        .values(asset_id=dst.id)
    )

    # Move aliases (dedupe by alias_norm)
    dst_aliases = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == dst.id)
    ).scalars().all()
    existing_norms = {a.alias_norm for a in dst_aliases}

    src_aliases = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == src.id)
    ).scalars().all()

    for a in src_aliases:
        if a.alias_norm in existing_norms:
            session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
        else:
            session.execute(
                update(models.AssetAlias)
                .where(models.AssetAlias.id == a.id)
                .values(asset_id=dst.id)
            )
            existing_norms.add(a.alias_norm)

    # Move trial links, avoid UNIQUE(trial_id, asset_id) collisions
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

    # Delete src asset
    session.execute(delete(models.Asset).where(models.Asset.id == src.id))


def _dedupe_aliases_for_asset(session: Session, asset_id: int) -> None:
    """
    Ensure UNIQUE(asset_id, alias_norm) by deleting duplicates.
    Keep the lowest id for each alias_norm, delete the rest.
    """
    aliases = session.execute(
        select(models.AssetAlias).where(models.AssetAlias.asset_id == asset_id)
    ).scalars().all()

    by_norm: dict[str, list[models.AssetAlias]] = defaultdict(list)
    for a in aliases:
        by_norm[a.alias_norm].append(a)

    for norm, items in by_norm.items():
        if len(items) <= 1:
            continue
        items_sorted = sorted(items, key=lambda x: x.id)
        keep = items_sorted[0]
        for dup in items_sorted[1:]:
            session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == dup.id))


def clean_company(session: Session, company_id: str) -> None:
    assets = session.execute(
        select(models.Asset).where(models.Asset.company_id == company_id)
    ).scalars().all()

    # 1) Clean/merge asset canonical names first
    for asset in assets:
        raw = asset.canonical_name
        cleaned = sanitize_asset_label(raw) or raw

        # if implausible, hide
        if not is_plausible_asset_label(cleaned):
            asset.is_disclosed = False
            continue

        # if name changes, merge into existing canonical if present
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

    # 2) Clean aliases safely (avoid UNIQUE collisions)
    # Use no_autoflush so SQLAlchemy won't flush mid-loop and cause integrity errors.
    with session.no_autoflush:
        assets2 = session.execute(
            select(models.Asset).where(models.Asset.company_id == company_id)
        ).scalars().all()

        for asset in assets2:
            # First remove obvious garbage aliases
            aliases = session.execute(
                select(models.AssetAlias).where(models.AssetAlias.asset_id == asset.id)
            ).scalars().all()

            # We'll rebuild normalized values and dedupe by desired alias_norm in-memory.
            desired: dict[str, tuple[int, str, str]] = {}
            # desired[alias_norm] = (alias_id_to_keep, new_alias, new_norm)

            for a in aliases:
                new_alias = sanitize_alias(a.alias)
                if not new_alias or not is_plausible_asset_label(new_alias):
                    session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                    continue

                new_norm = norm_text(new_alias)

                # If another alias would map to the same norm, keep the smallest id, delete others.
                if new_norm in desired:
                    keep_id, _, _ = desired[new_norm]
                    if a.id < keep_id:
                        # delete previous keep and keep this one
                        session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == keep_id))
                        desired[new_norm] = (a.id, new_alias, new_norm)
                    else:
                        session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == a.id))
                else:
                    desired[new_norm] = (a.id, new_alias, new_norm)

            session.flush()

            # Now apply updates; before each update, make sure we don't collide with an existing row.
            # (Should be clean already, but this guarantees no UNIQUE errors.)
            for new_norm, (alias_id, new_alias, _) in desired.items():
                # if some other row still has same norm (shouldn't), delete it
                collision = session.execute(
                    select(models.AssetAlias.id).where(
                        models.AssetAlias.asset_id == asset.id,
                        models.AssetAlias.alias_norm == new_norm,
                        models.AssetAlias.id != alias_id,
                    )
                ).all()
                for (dup_id,) in collision:
                    session.execute(delete(models.AssetAlias).where(models.AssetAlias.id == dup_id))

                session.execute(
                    update(models.AssetAlias)
                    .where(models.AssetAlias.id == alias_id)
                    .values(alias=new_alias, alias_norm=new_norm)
                )

            session.flush()
            _dedupe_aliases_for_asset(session, asset.id)

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
