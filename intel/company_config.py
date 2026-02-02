from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
import yaml
from pydantic import BaseModel, Field


class PipelineSource(BaseModel):
    type: Literal["html_pdf_link", "pdf", "html_image", "html_text"]
    url: str
    # optional hint/label (e.g., "development pipeline")
    label: str | None = None


class CompanyConfig(BaseModel):
    company_id: str
    name: str
    pipeline_sources: list[PipelineSource] = Field(default_factory=list)
    trial_sponsor_aliases: list[str] = Field(default_factory=list)

    # optional curated seed file (YAML)
    curated_assets_file: str | None = None


def load_companies(config_path: str | Path = "configs/companies.yaml") -> dict[str, CompanyConfig]:
    path = Path(config_path)
    raw = yaml.safe_load(path.read_text())
    companies = {}
    for item in raw.get("companies", []):
        cfg = CompanyConfig.model_validate(item)
        companies[cfg.company_id] = cfg
    return companies
