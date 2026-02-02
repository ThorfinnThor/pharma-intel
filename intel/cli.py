from __future__ import annotations

import argparse
from loguru import logger

import uvicorn

from .db import init_db, get_sessionmaker
from .company_config import load_companies
from .ingest.pipeline import ingest_pipeline
from .ingest.ctg_trials import ingest_trials_for_company


def cmd_init_db(_args):
    init_db()
    logger.info("DB initialized")


def cmd_ingest_pipeline(args):
    init_db()
    companies = load_companies()
    if args.company not in companies:
        raise SystemExit(f"Unknown company_id: {args.company}. Known: {', '.join(companies)}")
    cfg = companies[args.company]

    SessionLocal = get_sessionmaker()
    with SessionLocal() as session:
        n = ingest_pipeline(session, cfg.company_id)
        logger.info("Pipeline ingested: {} assets (company={})", n, cfg.company_id)


def cmd_ingest_trials(args):
    init_db()
    companies = load_companies()
    if args.company not in companies:
        raise SystemExit(f"Unknown company_id: {args.company}. Known: {', '.join(companies)}")
    cfg = companies[args.company]

    SessionLocal = get_sessionmaker()
    with SessionLocal() as session:
        stats = ingest_trials_for_company(session, cfg.company_id, cfg.trial_sponsor_aliases)
        logger.info("Trials ingested: {}", stats)


def cmd_serve(args):
    init_db()
    uvicorn.run("intel.api:app", host=args.host, port=args.port, reload=args.reload)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pharma-intel-mvp")
    sub = p.add_subparsers(dest="cmd", required=True)

    s0 = sub.add_parser("init-db", help="Create DB tables")
    s0.set_defaults(func=cmd_init_db)

    s1 = sub.add_parser("ingest-pipeline", help="Ingest pipeline sources for a company")
    s1.add_argument("--company", required=True, help="company_id (e.g., jnj, immatics)")
    s1.set_defaults(func=cmd_ingest_pipeline)

    s2 = sub.add_parser("ingest-trials", help="Ingest ClinicalTrials.gov studies for pipeline assets")
    s2.add_argument("--company", required=True, help="company_id (e.g., jnj, immatics)")
    s2.set_defaults(func=cmd_ingest_trials)

    s3 = sub.add_parser("serve", help="Run the FastAPI service")
    s3.add_argument("--host", default="127.0.0.1")
    s3.add_argument("--port", default=8000, type=int)
    s3.add_argument("--reload", action="store_true")
    s3.set_defaults(func=cmd_serve)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
