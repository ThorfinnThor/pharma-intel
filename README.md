# Pharma Company / Asset Intelligence MVP (J&J + Immatics)

This repo is a **defensible, evidence-backed** company/asset intelligence MVP for:
- **Johnson & Johnson** (via J&J "Selected Innovative Medicines in Development" pipeline PDF)
- **Immatics** (via pipeline page + pipeline image + curated seed list)

It:
1) ingests **pipeline disclosures** into a normalized asset table,
2) ingests **ClinicalTrials.gov** trials for those assets,
3) produces a **change feed** (new assets/indications, stage changes, trial status changes),
4) serves results via a small **FastAPI** service.

> Design principle: every "asset is doing X" assertion links to a concrete `Evidence` object.

---

## Quickstart

### 0) Requirements
- Python 3.10+
- Linux/macOS/Windows

### 1) Install
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Initialize DB (SQLite)
```bash
python -m intel.cli init-db
```

### 3) Ingest pipelines (creates assets + evidence)
```bash
python -m intel.cli ingest-pipeline --company jnj
python -m intel.cli ingest-pipeline --company immatics
```

### 4) Ingest ClinicalTrials.gov (links trials to assets)
```bash
python -m intel.cli ingest-trials --company jnj
python -m intel.cli ingest-trials --company immatics
```

### 5) Start API
```bash
python -m intel.cli serve --host 0.0.0.0 --port 8000
```

Then open:
- http://localhost:8000/docs (Swagger UI)

---

## Key concepts

### Evidence objects
Every ingestion creates an evidence record:
- type: `pipeline_pdf`, `pipeline_html`, `pipeline_image`, `ctg_study_json`
- source_url
- content hash
- stored payload path under `data/evidence/`

### Assets
`Asset` is the canonical object.
`AssetAlias` captures synonyms/codes.
Assets can have multiple indications and stages over time.

### Change feed
After each ingestion run, the system emits `ChangeEvent` rows:
- asset added
- asset indication added/removed
- asset stage advanced/changed
- trial added
- trial status changed

---

## Configuration

See `configs/companies.yaml`.

For companies where the pipeline is **not machine-readable** (Immatics uses an image),
the MVP includes a curated seed in `configs/immatics_curated_assets.yaml` and still stores the
pipeline image as evidence.

---

## Notes on completeness vs. scalability

This MVP is intentionally conservative:
- Pipeline ingest is the **authoritative asset list**.
- Trials are fetched **asset-first** (query by intervention) to avoid pulling the entire
  sponsor universe (J&J alone can be massive).

When you scale to 50 companies, you mainly add configs + a pipeline parser per source pattern.

---

## License
MIT (for the code you generate/modify).
