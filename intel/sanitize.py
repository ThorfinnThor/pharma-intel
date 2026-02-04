from __future__ import annotations

import re
from typing import Optional

_WS = re.compile(r"\s+")
_LEADING_BULLETS = re.compile(r"^[\u2022\-\*\•\·\u00b7]+\s*")

# Conservative whitelist for asset labels
_ALLOWED = re.compile(r"[^A-Za-z0-9\-\+\./\(\) ]+")

# Match "system)" or "system )" or "SYSTEM )" etc.
_PREFIX_NOISE = re.compile(r"^\s*(system|platform)\s*\)\s*", re.IGNORECASE)

STOP_ASSET_EXACT = {
    "indications",
    "indication",
    "delivery",
    "intravesical delivery",
    "system",
    "platform",
    "mechanism",
    "target",
    "targets",
    "oncology",
    "immunology",
    "neuroscience",
    "select other areas",
    "pediatrics",
    "colitis",
}

# Common disease/indication terms that frequently get mis-extracted as "assets" from PDFs.
# This list is intentionally broad; the logic that uses it is conservative (it won't block
# clear drug/program patterns like JNJ-#### or -mab/-nib single tokens).
DISEASE_KEYWORDS = {
    # generic
    "disease",
    "disorder",
    "syndrome",
    "condition",
    "pediatric",
    "pediatrics",
    "adult",
    "neonatal",
    "fetal",
    "pregnancy",
    "warm autoimmune",
    "autoimmune",
    # hematology
    "anemia",
    "thrombocytopenia",
    "hemolytic",
    "myeloma",
    "leukemia",
    "lymphoma",
    "aplastic",
    "neutropenia",
    # oncology
    "cancer",
    "carcinoma",
    "tumor",
    "tumour",
    "sarcoma",
    "melanoma",
    "metastatic",
    "solid tumor",
    "solid tumour",
    "colorectal",
    "prostate",
    "breast",
    "ovarian",
    "lung",
    "bladder",
    "renal",
    "hepatocellular",
    "glioblastoma",
    "acute myeloid",
    "multiple myeloma",
    # neuro/psych
    "depression",
    "major depressive",
    "ideation",
    "suicidal",
    "polyneuropathy",
    "demyelinating",
    "alzheimer",
    "parkinson",
    # immunology/gastro
    "colitis",
    "ulcerative",
    "psoriasis",
    "arthritis",
    "lupus",
    "asthma",
    "dermatitis",
    # infectious / misc
    "hypertension",
    "diabetes",
    "obesity",
    "infection",
}

# Tokens that commonly appear in pipeline rows but are not part of an intervention name.
ROUTE_FORM_TOKENS = {
    "iv",
    "sc",
    "im",
    "po",
    "oral",
    "tablet",
    "capsule",
    "solution",
    "suspension",
    "injection",
    "infusion",
    "subcutaneous",
    "intravenous",
    "intramuscular",
    "intravesical",
    "delivery",
    "system",
}

CORP_TOKENS = {
    "plc",
    "biosciences",
    "therapeutics",
    "pharma",
    "pharmaceutical",
    "corporation",
    "gmbh",
    "ltd",
    "inc",
    "ag",
}

# strings that appear as disclaimers in indications and should be cut off
IND_CUTOFF_PATTERNS = [
    r"\b(inclusion in|inclusion of)\b",
    r"\bthrough clinical trials\b",
    r"\bto the best of the company'?s knowledge\b",
    r"\bthe company assumes no obligation\b",
]


def _collapse_spaced_letters(s: str) -> str:
    """
    Fix OCR-like patterns: 'a c t o r X I a' -> 'actorXIa'
    Only triggers when many single-character tokens are present.
    """
    tokens = s.split()
    if len(tokens) < 6:
        return s
    singles = sum(1 for t in tokens if len(t) == 1 and re.match(r"[A-Za-z0-9]", t))
    if singles >= 5 and singles / max(len(tokens), 1) >= 0.6:
        return "".join(tokens)
    return s


def _strip_unbalanced_parens(s: str) -> str:
    # Remove extra closing parens at end
    while s.endswith(")") and s.count("(") < s.count(")"):
        s = s[:-1].rstrip()
    # Remove extra leading closing parens too: "autoleucel)" / ")Something"
    while s.startswith(")") and s.count("(") < s.count(")"):
        s = s[1:].lstrip()
    # Remove extra opening parens at start
    while s.startswith("(") and s.count("(") > s.count(")"):
        s = s[1:].lstrip()
    return s


def sanitize_asset_label(raw: str) -> Optional[str]:
    if raw is None:
        return None

    s = str(raw)
    s = s.replace("\u00a0", " ")  # nbsp
    s = _LEADING_BULLETS.sub("", s.strip())
    s = _WS.sub(" ", s).strip()

    # Remove known prefix noise like "system) "
    s = _PREFIX_NOISE.sub("", s)

    # Strip odd characters
    s = _ALLOWED.sub("", s)
    s = _WS.sub(" ", s).strip()

    # Fix OCR spacing and parentheses issues
    s = _collapse_spaced_letters(s)
    s = _strip_unbalanced_parens(s)

    s = _WS.sub(" ", s).strip()
    return s or None


def sanitize_alias(raw: str) -> Optional[str]:
    return sanitize_asset_label(raw)


def is_plausible_asset_label(label: str) -> bool:
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    # block exact stopwords
    if low in STOP_ASSET_EXACT:
        return False

    # if it contains partner/corporate tokens, it's almost certainly not an asset label
    if re.search(r"\b(" + "|".join(map(re.escape, CORP_TOKENS)) + r")\b", low):
        return False

    # must contain at least one letter/digit
    if not re.search(r"[A-Za-z0-9]", s):
        return False

    # absurdly long labels are usually PDF garbage/partner blocks
    if len(s) > 70:
        return False

    # too many words is rarely an asset label (except JNJ-#### codes)
    words = s.split()
    if len(words) > 6 and "jnj-" not in low:
        return False

    if low in {"others", "other", "unknown", "undisclosed"}:
        return False

    # Reject labels that look like indications/diseases.
    # We only do this when the label is *not* a clear program code or drug-like single token.
    if looks_like_indication_label(s):
        return False

    return True


_DRUG_SUFFIX = re.compile(
    r"(mab|nib|ciclib|stat|navir|vir|prazole|oxetine|afil|imumab|zumab|ximab|tinib|parib|lisib)$",
    re.IGNORECASE,
)


def looks_like_indication_label(label: str) -> bool:
    """Heuristic: does a label look like a disease/indication rather than an asset?"""
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    # program codes should survive
    if "jnj-" in low:
        return False

    # All-caps short brands are likely assets
    if s.isupper() and 3 <= len(s) <= 45:
        return False

    # Single-token drug-like names (e.g., icotrokinra, nipocalimab) should survive
    if " " not in s:
        if _DRUG_SUFFIX.search(s):
            return False

    # If it contains lots of route/formulation tokens, it's not a clean asset label
    tokens = re.findall(r"[A-Za-z]+", low)
    if tokens:
        route_hits = sum(1 for t in tokens if t in ROUTE_FORM_TOKENS)
        if route_hits >= 2:
            return True

    # Disease keyword hit: conservative application
    # - stronger if multi-word or contains commas/semicolons (typical in indications)
    hit = False
    for kw in DISEASE_KEYWORDS:
        if kw in low:
            hit = True
            break
    if not hit:
        return False

    # Single short token with disease keyword is unlikely; treat as indication if it's clearly disease-like
    words = s.split()
    if len(words) == 1 and len(s) <= 12:
        # "Cancer" / "Colitis" etc.
        return True

    # multi-word Title Case diseases (e.g., "Hemolytic Anemia")
    if len(words) >= 2:
        return True

    return False


def sanitize_indication_text(raw: str) -> str:
    s = (raw or "").replace("\u00a0", " ")
    s = _WS.sub(" ", s).strip()

    low = s.lower()
    for pat in IND_CUTOFF_PATTERNS:
        m = re.search(pat, low)
        if m:
            s = s[: m.start()].rstrip(" ;,-")
            break

    return s.strip()
