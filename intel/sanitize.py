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
    "intravesical delivery system",
    "delivery system",
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
    r"\b(pipeline is based on|pipeline reflects|pipeline reflects the current)\b",
    r"\b(inclusion in|inclusion of)\b",
    r"\bthrough clinical trials\b",
    r"\bto the best of the company'?s knowledge\b",
    r"\bjohnson\s+assumes\s+no\s+obligation\b",
    r"\bthe company assumes no obligation\b",
    r"\bthis pipeline is not\b",
    r"\bforward-looking\s+statements\b",
    r"\bstrategic partnerships\b",
]

# If an indication contains these phrases, it's almost certainly a footer/disclaimer
IND_DROP_IF_CONTAINS = [
    "pipeline is based",
    "the company assumes no obligation",
    "johnson assumes no obligation",
    "forward-looking statements",
    "strategic partnerships",
]


# Common disease/indication terms that frequently get mis-extracted as "assets" from PDFs.
DISEASE_KEYWORDS = {
    "disease",
    "disorder",
    "syndrome",
    "condition",
    "pediatric",
    "pediatrics",
    "neonatal",
    "fetal",
    "pregnancy",
    "hemolytic",
    "anemia",
    "thrombocytopenia",
    "polyneuropathy",
    "demyelinating",
    "cancer",
    "carcinoma",
    "tumor",
    "tumour",
    "sarcoma",
    "melanoma",
    "leukemia",
    "lymphoma",
    "myeloma",
    "colorectal",
    "prostate",
    "bladder",
    "lung",
    "breast",
    "ovarian",
    "renal",
    "hepatocellular",
    "colitis",
    "ulcerative",
    "psoriasis",
    "arthritis",
    "crohn",
    "lupus",
    "asthma",
    "dermatitis",
    "hypertension",
    # common PDF truncation where leading 'h' is dropped
    "ypertension",
    # frequent disease fragments in the J&J pipeline PDF
    "pulmonary",
    "arterial",
    "diabetes",
    "depression",
    "major depressive",
    "suicidal",
    "ideation",
    "leprosy",
}

_DOSE_OR_DIGIT = re.compile(r"\d|\b(mg|mcg|ug|g|kg|iu|units|mg\/kg)\b", re.IGNORECASE)

# Trial/Study acronym pattern that should *not* be treated as an asset in the pipeline PDF.
# Examples: ORIGAMI-2, MajesTEC-4, SunRISE-3, ICONIC-CD, ENERGY (often trial name)
_TRIAL_ACRONYM = re.compile(r"^[A-Za-z][A-Za-z0-9]{2,20}(?:-[A-Za-z0-9]{1,6})+$")

_DRUG_SUFFIX = re.compile(
    r"(mab|nib|parib|ciclib|stat|navir|vir|prazole|oxetine|afil|zumab|ximab|tinib|lisib)$",
    re.IGNORECASE,
)

# Detect camelCase / truncated disease fragments (e.g. "PulmonaryArterialH")
_CAMEL_CASE = re.compile(r"[a-z][A-Z]")
_ENDS_WITH_SINGLE_CAP = re.compile(r".*[a-z][A-Z]$")

# Split common glued lowercase words in indications ("Leprosyunderreview...")
_GLUED_WORDS = [
    "under",
    "review",
    "by",
    "of",
    "and",
    "in",
    "with",
    "for",
    "to",
    "from",
    "at",
    "risk",
    "non",
    "muscle",
    "invasive",
]


def _collapse_spaced_letters(s: str) -> str:
    """Fix OCR-like patterns: 'L e p r o s y' -> 'Leprosy'."""
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

    # unwrap pure parenthetical labels: "(PROTOSAR)" -> "PROTOSAR"
    m = re.match(r"^\(([^\)]+)\)\s*$", s)
    if m:
        s = m.group(1).strip()

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


def is_trial_acronym(label: str) -> bool:
    if not label:
        return False
    s = label.strip()
    # allow JNJ-#### codes (assets) even though they match the hyphen pattern
    if s.lower().startswith("jnj-"):
        return False
    # Typical trial/study tags are short and hyphenated
    if _TRIAL_ACRONYM.match(s) and len(s) <= 22:
        return True
    return False


def looks_like_indication_label(label: str) -> bool:
    """Heuristic: does a label look like a disease/indication rather than an asset?"""
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    # Truncation fragments like "PulmonaryArterialH" should never be assets.
    if _CAMEL_CASE.search(s) and any(k in low for k in ("pulmonary", "arterial", "hypertension", "ypertension")):
        return True
    if _ENDS_WITH_SINGLE_CAP.match(s) and any(k in low for k in ("pulmonary", "arterial", "hypertension", "ypertension")):
        return True

    # program codes survive
    if low.startswith("jnj-"):
        return False

    # Single-token drug-like names survive
    if " " not in s and _DRUG_SUFFIX.search(s):
        return False

    # If it looks like a trial acronym, treat as non-asset (pipeline uses these as callouts)
    if is_trial_acronym(s):
        return True

    # disease keyword hit?
    # Be robust to a common PDF truncation where the first character is dropped.
    hit = False
    for kw in DISEASE_KEYWORDS:
        if kw in low:
            hit = True
            break
        if len(kw) >= 7 and kw[1:] in low:
            hit = True
            break
    if not hit:
        return False

    # disease keyword + any digit/dose token is almost certainly an indication fragment
    if _DOSE_OR_DIGIT.search(low):
        return True

    # Multi-word Title Case diseases (e.g. "Hemolytic Anemia")
    if len(s.split()) >= 2:
        return True

    # Single keyword like "Leprosy" / "Cancer" etc.
    if len(s) <= 16:
        return True

    return False


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

    # reject disease-like labels (this is what fixes your screenshots)
    if looks_like_indication_label(s):
        return False

    # reject pure trial acronyms
    if is_trial_acronym(s):
        return False

    return True


def indication_is_footer_noise(text: str) -> bool:
    low = (text or "").lower()
    return any(k in low for k in IND_DROP_IF_CONTAINS)


def sanitize_indication_text(raw: str) -> str:
    s = (raw or "").replace("\u00a0", " ")
    s = _WS.sub(" ", s).strip()

    # Collapse OCR spaced letters (e.g. "L e p r o s y")
    s = _collapse_spaced_letters(s)

    # De-glue camelCase and acronym boundaries (SIRTUROLeprosy...)
    if " " not in s and len(s) >= 25:
        s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
        s = re.sub(r"([A-Z]{2,})([A-Z][a-z])", r"\1 \2", s)

    # De-glue common lowercase runs (Leprosyunderreviewby...)
    if " " not in s and len(s) >= 20:
        for w in _GLUED_WORDS:
            s = re.sub(rf"(?i)([a-z])({re.escape(w)})([a-z])", r"\1 \2 \3", s)

    s = _WS.sub(" ", s).strip()
    low = s.lower()

    # Drop entire line if it looks like a footer/disclaimer
    if indication_is_footer_noise(s):
        return ""

    # Cut off at first disclaimer phrase
    for pat in IND_CUTOFF_PATTERNS:
        m = re.search(pat, low)
        if m:
            s = s[: m.start()].rstrip(" ;,-")
            break

    return s.strip()
