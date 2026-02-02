from __future__ import annotations

from sqlalchemy.orm import Session

from .jnj_pipeline import ingest_jnj_pipeline
from .immatics_pipeline import ingest_immatics_pipeline


def ingest_pipeline(session: Session, company_id: str) -> int:
    if company_id == "jnj":
        return ingest_jnj_pipeline(session, company_id="jnj")
    if company_id == "immatics":
        return ingest_immatics_pipeline(session, company_id="immatics")
    raise ValueError(f"No pipeline ingestor implemented for company_id={company_id}")
