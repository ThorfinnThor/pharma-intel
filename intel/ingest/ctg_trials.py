from __future__ import annotations

import re
from typing import Any

from loguru import logger
from rapidfuzz import fuzz
from requests.exceptions import HTTPError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import models
from ..evidence import store_json
from ..http import get, polite_sleep
from ..normalize import norm_text
from ..repo import add_evidence, emit_change, finish_run, start_run, ensure_alias
from ..settings import settings
from ..sanitize import sanitize_alias, is_plausible_asset_label, looks_like_indication_label

CTG_STUDIES_ENDPOINT = "https://clinicaltrials.gov/api/v2/studies"

DEFAULT_ACTIVE_STATUSES = [
    "NOT_YET_RECRUITING",
    "RECRUITING",
    "ENROLLING_BY_INVITATION",
    "ACTIVE_NOT_RECRUITING",
]


# ---------------------------
# Helpers: sponsor filtering
# ---------------------------

def _company_alias_hit(s: str | None, sponsor_aliases: list[str]) -> bool:
    if not s:
        return False
    n = norm_text(s)
    return any(norm_text(a) in n for a in sponsor_aliases)


def _study_belongs_to_company(study: dict[str, Any], sponsor_aliases: list[str]) -> bool:
    ps = study.get("protocolSection") or {}
    sponsor_mod = ps.get("sponsorCollaboratorsModule") or {}
    lead = sponsor_mod.get("leadSponsor") or {}
    lead_name = lead.get("name")

    collabs = sponsor_mod.get("collaborators") or []
    collab_names = [c.get("name") for c in collabs if isinstance(c, dict)]

    if _company_alias_hit(lead_name, sponsor_aliases):
        return True
    return any(_company_alias_hit(cn, sponsor_aliases) for cn in collab_names)


# ---------------------------
# Helpers: core extraction
# ---------------------------

def _extract_trial_core(study: dict[str, Any]) -> dict[str, Any]:
    ps = study.get("protocolSection") or {}
    idm = ps.get("identificationModule") or {}
    stat = ps.get("statusModule") or {}
    design = ps.get("designModule") or {}
    sponsor_mod = ps.get("sponsorCollaboratorsModule") or {}
    cond_mod = ps.get("conditionsModule") or {}
    intr_mod = ps.get("armsInterventionsModule") or {}

    lead = (sponsor_mod.get("leadSponsor") or {}).get("name")
    collabs = sponsor_mod.get("collaborators") or []
    collaborators = [c.get("name") for c in collabs if isinstance(c, dict) and c.get("name")]

    phase = None
    phases = design.get("phases")
    if isinstance(phases, list) and phases:
        phase = ",".join(phases)

    interventions = intr_mod.get("interventions") or []
    interventions_out = []
    for it in interventions:
        if not isinstance(it, dict):
            continue
        name = it.get("name")
        if name:
            other = it.get("otherNames") or it.get("otherNamesList") or []
            other_names: list[str] = []
            if isinstance(other, list):
                other_names = [x for x in other if isinstance(x, str) and x.strip()]
            interventions_out.append({"name": name, "type": it.get("type"), "other_names": other_names})

    conditions = cond_mod.get("conditions") or []

    return {
        "nct_id": idm.get("nctId"),
        "title": idm.get("officialTitle") or idm.get("briefTitle"),
        "overall_status": stat.get("overallStatus"),
        "phase": phase,
        "start_date": (stat.get("startDateStruct") or {}).get("date"),
        "last_update_posted": (stat.get("lastUpdatePostDateStruct") or {}).get("date"),
        "lead_sponsor": lead,
        "collaborators": collaborators,
        "interventions": interventions_out,
        "conditions": conditions,
    }


# ---------------------------
# Query term hygiene (FIX)
# ---------------------------

_BAD_INTR_CHARS = r"""[\(\)\[\]\{\}"'<>]"""


def _sanitize_intr_term(term: str) -> str | None:
    """
    ClinicalTrials.gov v2 rejects certain malformed query.intr strings (e.g. stray ')').
    We sanitize gently:
      - strip whitespace
      - remove problematic bracket/quote chars anywhere
      - collapse spaces
      - strip trailing punctuation
    """
    t = (term or "").strip()
    if not t:
        return None

    # Remove problematic characters anywhere in the term
    t = re.sub(_BAD_INTR_CHARS, "", t)

    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # Strip trailing punctuation that often leaks from parsing
    t = t.strip(" ,;:.!/\\|")

    if len(t) < settings.min_alias_len_for_trial_search:
        return None
    return t


def _get_asset_alias_terms(session: Session, company_id: str) -> list[str]:
    stmt = select(models.AssetAlias.alias).join(models.Asset).where(models.Asset.company_id == company_id)
    raw_aliases = [r[0] for r in session.execute(stmt).all()]

    out: list[str] = []
    seen: set[str] = set()

    for a in raw_aliases:
        if not a:
            continue

        s = _sanitize_intr_term(a)
        if not s:
            continue

        low = s.lower()
        if "undisclosed" in low:
            continue
        if low in {"other", "others", "unknown"}:
            continue

        # Critical safety: do not query CTG with indication-like or otherwise implausible "asset" terms.
        # This prevents cascades like query_alias="of the Fetus and Newborn".
        if looks_like_indication_label(s) or not is_plausible_asset_label(s):
            continue

        k = norm_text(s)
        if k in seen:
            continue

        seen.add(k)
        out.append(s)

    out.sort(key=len)
    return out


# ---------------------------
# Alias bootstrapping safety
# ---------------------------

_BOOTSTRAP_STOP = {
    "placebo",
    "control",
    "vehicle",
    "saline",
    "standard of care",
    "soc",
    "best supportive care",
    "bsc",
}

_BOOTSTRAP_DRUG_LIKE = re.compile(r"(mab|nib|parib|ciclib|navir|vir|zumab|ximab|tinib|lisib)$", re.IGNORECASE)
_BOOTSTRAP_PROGRAM_CODE = re.compile(r"^jnj-\d{4,9}$", re.IGNORECASE)


def _bootstrap_ok(alias_token: str) -> bool:
    a = (alias_token or "").strip()
    if not a:
        return False
    low = a.lower()
    if low in _BOOTSTRAP_STOP:
        return False
    if len(a) < settings.min_alias_len_for_trial_search or len(a) > 40:
        return False
    if " " in a:
        return False

    # allow program codes and drug-like generics
    if _BOOTSTRAP_PROGRAM_CODE.match(a):
        return True
    if _BOOTSTRAP_DRUG_LIKE.search(a):
        return True

    # allow all-caps brand-like tokens (e.g., TREMFYA)
    if a.isupper() and 4 <= len(a) <= 20:
        return True

    return False


def _asset_ids_for_alias_term(session: Session, company_id: str, alias_term: str) -> list[int]:
    n = norm_text(alias_term)
    stmt = (
        select(models.AssetAlias.asset_id)
        .join(models.Asset)
        .where(models.Asset.company_id == company_id, models.AssetAlias.alias_norm == n)
    )
    return [int(r[0]) for r in session.execute(stmt).all()]


def _build_alias_index(session: Session, company_id: str) -> dict[str, int]:
    stmt = (
        select(models.AssetAlias.alias_norm, models.AssetAlias.asset_id)
        .join(models.Asset)
        .where(models.Asset.company_id == company_id)
    )
    idx: dict[str, int] = {}
    for norm, aid in session.execute(stmt).all():
        idx[norm] = aid
    return idx


# ---------------------------
# Intervention normalization
# ---------------------------

_DOSE_NOISE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:mg|mcg|ug|g|kg|iu|units|mg\/kg|mcg\/kg|ug\/kg)\b", re.IGNORECASE)
_ROUTE_NOISE = re.compile(
    r"\b(iv|i\.v\.|intravenous|sc|s\.c\.|subcutaneous|oral|po|p\.o\.|intramuscular|im|i\.m\.|infusion|inhaled|topical)\b",
    re.IGNORECASE,
)
_PAREN = re.compile(r"\([^)]*\)")
_SPLIT = re.compile(r"[+;/,]|\band\b|\bwith\b", re.IGNORECASE)


def _clean_intervention_string(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = _PAREN.sub(" ", s)
    s = _DOSE_NOISE.sub(" ", s)
    s = _ROUTE_NOISE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.strip(" -–—:;,.!\t")
    return s


def _intervention_candidate_terms(it: dict[str, Any]) -> list[str]:
    names: list[str] = []
    primary = (it.get("name") or "").strip()
    if primary:
        names.append(primary)
    for alt in (it.get("other_names") or []):
        if isinstance(alt, str) and alt.strip():
            names.append(alt.strip())

    out: list[str] = []
    seen: set[str] = set()

    for n in names:
        cleaned = _clean_intervention_string(n)
        if not cleaned:
            continue
        parts = [p.strip() for p in _SPLIT.split(cleaned) if p.strip()]
        for p in parts:
            if len(p) < settings.min_alias_len_for_trial_search:
                continue
            k = norm_text(p)
            if k in seen:
                continue
            seen.add(k)
            out.append(p)
    return out


# ---------------------------
# Trial↔asset linking (idempotent)
# ---------------------------

def _link_assets_for_trial(session: Session, company_id: str, trial: models.Trial, interventions: list[dict[str, Any]]) -> int:
    """
    Build links trial_id -> asset_id.
    We de-duplicate by asset_id before insert (avoids UNIQUE constraint failures).
    """
    alias_idx = _build_alias_index(session, company_id)

    session.query(models.TrialAssetLink).filter(models.TrialAssetLink.trial_id == trial.id).delete()

    def _rank(mt: str) -> int:
        return 2 if mt == "exact" else 1

    def _choose_better(existing: tuple[str, int] | None, candidate: tuple[str, int]) -> tuple[str, int]:
        if existing is None:
            return candidate
        if _rank(candidate[0]) > _rank(existing[0]):
            return candidate
        if _rank(candidate[0]) < _rank(existing[0]):
            return existing
        return candidate if candidate[1] > existing[1] else existing

    best_for_asset: dict[int, tuple[str, int]] = {}

    alias_items = list(alias_idx.items())

    for it in interventions:
        terms = _intervention_candidate_terms(it)
        if not terms:
            continue

        for term in terms:
            n = norm_text(term)
            if not n:
                continue

            if n in alias_idx:
                aid = alias_idx[n]
                best_for_asset[aid] = _choose_better(best_for_asset.get(aid), ("exact", 100))
                continue

            best_aid: int | None = None
            best_score = 0

            for alias_norm, aid in alias_items:
                if abs(len(alias_norm) - len(n)) > 14:
                    continue
                sc = max(
                    fuzz.token_set_ratio(n, alias_norm),
                    fuzz.partial_ratio(n, alias_norm),
                )
                if sc > best_score:
                    best_score = sc
                    best_aid = aid

            if best_aid is not None and best_score >= settings.fuzzy_threshold:
                best_for_asset[best_aid] = _choose_better(best_for_asset.get(best_aid), ("fuzzy", int(best_score)))

    for aid, (mt, sc) in best_for_asset.items():
        session.add(models.TrialAssetLink(trial_id=trial.id, asset_id=aid, match_type=mt, match_score=sc))

    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise

    return len(best_for_asset)


# ---------------------------
# Main ingestion
# ---------------------------

def ingest_trials_for_company(
    session: Session,
    company_id: str,
    sponsor_aliases: list[str],
    *,
    statuses: list[str] | None = None,
) -> dict[str, Any]:
    statuses = statuses or DEFAULT_ACTIVE_STATUSES

    run = start_run(session, company_id, "trials")
    run_id = int(run.id)
    session.commit()

    try:
        alias_terms = _get_asset_alias_terms(session, company_id)
        logger.info("CTG: querying {} alias terms for {}", len(alias_terms), company_id)

        seen_nct: set[str] = set()
        inserted = 0
        updated = 0
        status_changed = 0
        bad_aliases = 0

        for alias in alias_terms:
            bootstrap_asset_ids = _asset_ids_for_alias_term(session, company_id, alias)

            # Only attempt alias bootstrapping when the query alias itself is plausible.
            do_bootstrap = bool(bootstrap_asset_ids) and (not looks_like_indication_label(alias)) and is_plausible_asset_label(alias)

            # norm -> {alias: str, ncts: set[str]}
            bootstrap_counts: dict[str, dict[str, Any]] = {}

            params = {
                "query.intr": alias,
                "pageSize": settings.ctg_page_size,
                "countTotal": "false",
                "format": "json",
                "filter.overallStatus": ",".join(statuses),
                "sort": "LastUpdatePostDate:desc",
            }

            page = 0
            page_token = None

            while True:
                if page_token:
                    params["pageToken"] = page_token

                try:
                    resp = get(CTG_STUDIES_ENDPOINT, params=params).json()
                except HTTPError as e:
                    code = getattr(getattr(e, "response", None), "status_code", None)
                    if code == 400:
                        bad_aliases += 1
                        logger.warning("CTG: 400 Bad Request for query.intr='{}' -> skipping this alias", alias)
                        break
                    raise

                studies = resp.get("studies") or []
                for study in studies:
                    core = _extract_trial_core(study)
                    nct = core.get("nct_id")
                    if not nct:
                        continue
                    if nct in seen_nct:
                        continue
                    if not _study_belongs_to_company(study, sponsor_aliases):
                        continue

                    seen_nct.add(nct)

                    h, p, meta = store_json(
                        company_id,
                        "ctg_study_json",
                        f"{CTG_STUDIES_ENDPOINT}?nct={nct}",
                        study,
                        meta={"query_intr": alias},
                    )
                    ev = add_evidence(session, company_id, "ctg_study_json", f"{CTG_STUDIES_ENDPOINT}?nct={nct}", h, str(p), meta=meta)

                    existing = session.execute(
                        select(models.Trial).where(models.Trial.company_id == company_id, models.Trial.nct_id == nct)
                    ).scalar_one_or_none()

                    if existing is None:
                        tr = models.Trial(
                            company_id=company_id,
                            nct_id=nct,
                            title=core.get("title"),
                            overall_status=core.get("overall_status"),
                            phase=core.get("phase"),
                            start_date=core.get("start_date"),
                            last_update_posted=core.get("last_update_posted"),
                            lead_sponsor=core.get("lead_sponsor"),
                            collaborators=core.get("collaborators") or [],
                            source_url=f"https://clinicaltrials.gov/study/{nct}",
                            evidence_id=ev.id,
                        )
                        session.add(tr)
                        session.commit()
                        session.refresh(tr)
                        inserted += 1
                        emit_change(session, company_id, "trial_added", {"nct_id": nct, "title": tr.title}, evidence_id=ev.id, trial_id=tr.id)
                    else:
                        tr = existing
                        old_status = tr.overall_status
                        new_status = core.get("overall_status")
                        if new_status and old_status != new_status:
                            tr.overall_status = new_status
                            status_changed += 1
                            emit_change(
                                session,
                                company_id,
                                "trial_status_changed",
                                {"nct_id": nct, "from": old_status, "to": new_status},
                                evidence_id=ev.id,
                                trial_id=tr.id,
                            )

                        tr.title = tr.title or core.get("title")
                        tr.phase = core.get("phase") or tr.phase
                        tr.last_update_posted = core.get("last_update_posted") or tr.last_update_posted
                        tr.evidence_id = ev.id
                        session.commit()
                        updated += 1

                    # refresh conditions/interventions
                    session.query(models.TrialIntervention).filter(models.TrialIntervention.trial_id == tr.id).delete()
                    session.query(models.TrialCondition).filter(models.TrialCondition.trial_id == tr.id).delete()
                    session.commit()

                    for it in core.get("interventions") or []:
                        session.add(models.TrialIntervention(trial_id=tr.id, name=it["name"], intervention_type=it.get("type")))

                        # Accumulate bootstrap candidates for this query alias.
                        if do_bootstrap:
                            for term in _intervention_candidate_terms(it):
                                cand = sanitize_alias(term)
                                if not cand:
                                    continue
                                if not _bootstrap_ok(cand):
                                    continue
                                kn = norm_text(cand)
                                if kn not in bootstrap_counts:
                                    bootstrap_counts[kn] = {"alias": cand, "ncts": {nct}}
                                else:
                                    ncts = bootstrap_counts[kn].setdefault("ncts", set())
                                    if isinstance(ncts, set):
                                        ncts.add(nct)

                    for c in core.get("conditions") or []:
                        session.add(models.TrialCondition(trial_id=tr.id, condition=c))
                    session.commit()

                    linked = _link_assets_for_trial(session, company_id, tr, core.get("interventions") or [])
                    if linked:
                        emit_change(session, company_id, "trial_assets_linked", {"nct_id": nct, "linked_assets": linked}, evidence_id=ev.id, trial_id=tr.id)

                page_token = resp.get("nextPageToken")
                page += 1
                polite_sleep()

                if not page_token:
                    break
                if page >= settings.ctg_max_pages_per_query:
                    logger.warning("CTG: hit max pages cap for alias '{}' ({} pages)", alias, page)
                    break

            # After we've processed all pages for this query alias, add any high-confidence generic aliases.
            if do_bootstrap and bootstrap_counts:
                winners: list[str] = []
                for v in bootstrap_counts.values():
                    ncts = v.get("ncts")
                    if isinstance(ncts, set) and len(ncts) >= 2:
                        tok = v.get("alias")
                        if isinstance(tok, str):
                            winners.append(tok)

                for alias_token in winners:
                    if len(alias_token) < settings.min_alias_len_for_trial_search:
                        continue
                    if norm_text(alias_token) == norm_text(alias):
                        continue
                    for aid in bootstrap_asset_ids:
                        try:
                            ensure_alias(session, aid, alias_token)
                        except Exception:
                            continue
                    emit_change(
                        session,
                        company_id,
                        "trial_alias_bootstrapped",
                        {"query_alias": alias, "added_alias": alias_token, "assets": bootstrap_asset_ids},
                    )

        emit_change(
            session,
            company_id,
            "trials_ingested",
            {
                "trials_seen": len(seen_nct),
                "inserted": inserted,
                "updated": updated,
                "status_changed": status_changed,
                "bad_aliases": bad_aliases,
            },
        )
        finish_run(session, run_id, "ok", notes=f"seen={len(seen_nct)} inserted={inserted} updated={updated} bad_aliases={bad_aliases}")
        session.commit()

        return {
            "seen": len(seen_nct),
            "inserted": inserted,
            "updated": updated,
            "status_changed": status_changed,
            "bad_aliases": bad_aliases,
        }

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        try:
            finish_run(session, run_id, "error", notes=str(e))
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
        raise
