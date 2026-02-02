from __future__ import annotations

import datetime as dt
from sqlalchemy import (
    String, Integer, DateTime, ForeignKey, Text, Boolean, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from .db import Base


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)


class Evidence(Base):
    __tablename__ = "evidence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[str] = mapped_column(String(64), ForeignKey("companies.id"), index=True, nullable=False)

    evidence_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)

    # optional: date printed on a PDF, etc.
    published_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    content_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    content_path: Mapped[str] = mapped_column(Text, nullable=False)

    meta: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class Asset(Base):
    __tablename__ = "assets"
    __table_args__ = (
        UniqueConstraint("company_id", "canonical_name", name="uq_asset_company_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[str] = mapped_column(String(64), ForeignKey("companies.id"), index=True, nullable=False)

    canonical_name: Mapped[str] = mapped_column(String(255), nullable=False)
    modality: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target: Mapped[str | None] = mapped_column(String(128), nullable=True)

    is_disclosed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), onupdate=lambda: dt.datetime.utcnow(), nullable=False)

    aliases: Mapped[list["AssetAlias"]] = relationship(back_populates="asset", cascade="all, delete-orphan")
    indications: Mapped[list["AssetIndication"]] = relationship(back_populates="asset", cascade="all, delete-orphan")


class AssetAlias(Base):
    __tablename__ = "asset_aliases"
    __table_args__ = (
        UniqueConstraint("asset_id", "alias_norm", name="uq_alias_asset_norm"),
        Index("ix_alias_norm", "alias_norm"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(Integer, ForeignKey("assets.id"), index=True, nullable=False)

    alias: Mapped[str] = mapped_column(String(255), nullable=False)
    alias_norm: Mapped[str] = mapped_column(String(255), nullable=False)

    asset: Mapped["Asset"] = relationship(back_populates="aliases")


class AssetIndication(Base):
    __tablename__ = "asset_indications"
    __table_args__ = (
        Index("ix_asset_indication_asset", "asset_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(Integer, ForeignKey("assets.id"), nullable=False)

    indication: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g., Phase 1, Phase 2, Registration
    therapeutic_area: Mapped[str | None] = mapped_column(String(128), nullable=True)

    as_of_date: Mapped[str | None] = mapped_column(String(32), nullable=True)  # keep as ISO string for simplicity

    evidence_id: Mapped[int] = mapped_column(Integer, ForeignKey("evidence.id"), nullable=False)

    asset: Mapped["Asset"] = relationship(back_populates="indications")


class Trial(Base):
    __tablename__ = "trials"
    __table_args__ = (
        UniqueConstraint("company_id", "nct_id", name="uq_trial_company_nct"),
        Index("ix_trial_status", "overall_status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[str] = mapped_column(String(64), ForeignKey("companies.id"), index=True, nullable=False)

    nct_id: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)

    overall_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    phase: Mapped[str | None] = mapped_column(String(64), nullable=True)

    start_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_update_posted: Mapped[str | None] = mapped_column(String(32), nullable=True)

    lead_sponsor: Mapped[str | None] = mapped_column(Text, nullable=True)
    collaborators: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)

    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)

    evidence_id: Mapped[int] = mapped_column(Integer, ForeignKey("evidence.id"), nullable=False)

    interventions: Mapped[list["TrialIntervention"]] = relationship(back_populates="trial", cascade="all, delete-orphan")
    conditions: Mapped[list["TrialCondition"]] = relationship(back_populates="trial", cascade="all, delete-orphan")
    asset_links: Mapped[list["TrialAssetLink"]] = relationship(back_populates="trial", cascade="all, delete-orphan")


class TrialIntervention(Base):
    __tablename__ = "trial_interventions"
    __table_args__ = (Index("ix_trial_intervention_trial", "trial_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trial_id: Mapped[int] = mapped_column(Integer, ForeignKey("trials.id"), nullable=False)

    name: Mapped[str] = mapped_column(Text, nullable=False)
    intervention_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    trial: Mapped["Trial"] = relationship(back_populates="interventions")


class TrialCondition(Base):
    __tablename__ = "trial_conditions"
    __table_args__ = (Index("ix_trial_condition_trial", "trial_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trial_id: Mapped[int] = mapped_column(Integer, ForeignKey("trials.id"), nullable=False)

    condition: Mapped[str] = mapped_column(Text, nullable=False)

    trial: Mapped["Trial"] = relationship(back_populates="conditions")


class TrialAssetLink(Base):
    __tablename__ = "trial_asset_links"
    __table_args__ = (
        UniqueConstraint("trial_id", "asset_id", name="uq_trial_asset_link"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trial_id: Mapped[int] = mapped_column(Integer, ForeignKey("trials.id"), nullable=False)
    asset_id: Mapped[int] = mapped_column(Integer, ForeignKey("assets.id"), nullable=False)

    match_type: Mapped[str] = mapped_column(String(32), nullable=False)  # exact|fuzzy
    match_score: Mapped[int] = mapped_column(Integer, nullable=False)

    trial: Mapped["Trial"] = relationship(back_populates="asset_links")


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    __table_args__ = (Index("ix_ingestion_company_type", "company_id", "run_type"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[str] = mapped_column(String(64), ForeignKey("companies.id"), nullable=False)

    run_type: Mapped[str] = mapped_column(String(64), nullable=False)  # pipeline|trials
    started_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)  # running|ok|error
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class ChangeEvent(Base):
    __tablename__ = "change_events"
    __table_args__ = (Index("ix_change_company_time", "company_id", "occurred_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[str] = mapped_column(String(64), ForeignKey("companies.id"), nullable=False)

    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)

    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    evidence_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("evidence.id"), nullable=True)
    asset_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("assets.id"), nullable=True)
    trial_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("trials.id"), nullable=True)
