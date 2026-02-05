from __future__ import annotations

import re
from typing import Optional

_WS = re.compile(r"\s+")
_LEADING_BULLETS = re.compile(r"^[\u2022\-\*\•\·\u00b7]+\s*")

# Conservative whitelist for asset labels
_ALLOWED = re.compile(r"[^A-Za-z0-9\-\+\./\(\) ]+")

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
    "select other",
    "other areas",
    "pediatrics",
    "pediatric",
}

# New: route/procedure fragments that should never be assets
STOP_ASSET_CONTAINS = {
    "subcutaneous",
    "intravenous",
    "intramuscular",
    "oral",
    "injection",
    "infusion",
    "induction",
    "maintenance",
    "loading dose",
    "placebo",
    "double-blind",
    "randomized",
    "multicenter",
    "multicentre",
    "placebo-controlled",
    "controlled study",
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

IND_DROP_IF_CONTAINS = [
    "pipeline is based",
    "the company assumes no obligation",
    "johnson assumes no obligation",
    "forward-looking statements",
    "strategic partnerships",
]

# Disease/indication terms that commonly leak into the asset column
DISEASE_KEYWORDS = {
    "disease",
    "disorder",
    "syndrome",
    "condition",
    "pediatric",
    "pediatrics",
    "neonatal",
    "newborn",
    "fetal",
    "fetus",
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
    # pipeline truncation (leading 'h' dropped)
    "ypertension",
    "pulmonary",
    "arterial",
    "diabetes",
    "depression",
    "major depressive",
    "suicidal",
    "ideation",
    "leprosy",
}

# New: target/mechanism keywords that must not become assets
TARGET_KEYWORDS = {
    "factor",
    "xi",
    "xia",
    "xla",
    "cd",
    "il-",
    "jak",
    "tnf",
    "tgf",
    "vegf",
    "pd-1",
    "pd-l1",
    "ctla-4",
}

_DOSE_OR_DIGIT = re.compile(r"\d|\b(mg|mcg|ug|g|kg|iu|units|mg\/kg|mcg\/kg|ug\/kg)\b", re.IGNORECASE)

# Trial acronym pattern like ORIGAMI-2 / MajesTEC-4 / SunRISE-3 / ICONIC-CD
_TRIAL_ACRONYM = re.compile(r"^[A-Za-z][A-Za-z0-9]{2,20}(?:-[A-Za-z0-9]{1,6})+$")

# Program / phase fragment like "1-3PLs"
_PHASE_FRAGMENT = re.compile(r"^\d+\s*-\s*\d+\s*pls?$", re.IGNORECASE)

_DRUG_SUFFIX = re.compile(
    r"(mab|nib|parib|ciclib|stat|navir|vir|prazole|oxetine|afil|zumab|ximab|tinib|lisib)$",
    re.IGNORECASE,
)

_CAMEL_CASE = re.compile(r"[a-z][A-Z]")
_ENDS_WITH_SINGLE_CAP = re.compile(r".*[a-z][A-Z]$")

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
    """
    Fix OCR-like patterns: 'L e p r o s y' -> 'Leprosy'.
    Also kill very short spaced-letter junk like 'i o n' (returning 'ion' is worse than dropping later).
    """
    tokens = s.split()
    if not tokens:
        return s

    # If it's just 2-4 single letters ("i o n"), collapse; later sanitize_indication_text can drop if too short.
    if 2 <= len(tokens) <= 4 and all(len(t) == 1 and re.match(r"[A-Za-z]", t) for t in tokens):
        return "".join(tokens)

    if len(tokens) < 6:
        return s

    singles = sum(1 for t in tokens if len(t) == 1 and re.match(r"[A-Za-z0-9]", t))
    if singles >= 5 and singles / max(len(tokens), 1) >= 0.6:
        return "".join(tokens)
    return s


def _strip_unbalanced_parens(s: str) -> str:
    while s.endswith(")") and s.count("(") < s.count(")"):
        s = s[:-1].rstrip()
    while s.startswith(")") and s.count("(") < s.count(")"):
        s = s[1:].lstrip()
    while s.startswith("(") and s.count("(") > s.count(")"):
        s = s[1:].lstrip()
    return s


def sanitize_asset_label(raw: str) -> Optional[str]:
    if raw is None:
        return None

    s = str(raw)
    s = s.replace("\u00a0", " ")
    s = _LEADING_BULLETS.sub("", s.strip())
    s = _WS.sub(" ", s).strip()

    # unwrap "(PROTOSAR)" -> "PROTOSAR"
    m = re.match(r"^\(([^\)]+)\)\s*$", s)
    if m:
        s = m.group(1).strip()

    s = _PREFIX_NOISE.sub("", s)
    s = _ALLOWED.sub("", s)
    s = _WS.sub(" ", s).strip()

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
    if s.lower().startswith("jnj-"):
        return False
    if _TRIAL_ACRONYM.match(s) and len(s) <= 22:
        return True
    return False


def looks_like_indication_label(label: str) -> bool:
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    # Truncation fragments like "PulmonaryArterialH"
    if _CAMEL_CASE.search(s) and any(k in low for k in ("pulmonary", "arterial", "hypertension", "ypertension")):
        return True
    if _ENDS_WITH_SINGLE_CAP.match(s) and any(k in low for k in ("pulmonary", "arterial", "hypertension", "ypertension")):
        return True

    if low.startswith("jnj-"):
        return False

    # route/procedure fragments
    if any(tok in low for tok in STOP_ASSET_CONTAINS):
        return True

    # very common phrase fragments
    if low.startswith("of the ") or low.startswith("in the ") or low.startswith("for the "):
        return True

    if " " not in s and _DRUG_SUFFIX.search(s):
        return False

    if is_trial_acronym(s):
        return True

    # "1-3PLs" kind of fragments
    if _PHASE_FRAGMENT.match(low.replace(" ", "")):
        return True

    # disease keyword hit (robust to leading char drop)
    hit = False
    for kw in DISEASE_KEYWORDS:
        if kw in low:
            hit = True
            break
        if len(kw) >= 7 and kw[1:] in low:
            hit = True
            break

    if not hit:
        # also catch obvious fetus/newborn phrase without exact keyword match
        if "fetus" in low or "newborn" in low:
            hit = True

    if not hit:
        return False

    if _DOSE_OR_DIGIT.search(low):
        return True

    if len(s.split()) >= 2:
        return True

    if len(s) <= 16:
        return True

    return False


def is_plausible_asset_label(label: str) -> bool:
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    if low in STOP_ASSET_EXACT:
        return False

    if any(tok in low for tok in STOP_ASSET_CONTAINS):
        return False

    # reject fragments like "of the Fetus and Newborn"
    if low.startswith("of the ") or low.startswith("in the ") or low.startswith("for the "):
        return False

    # reject factor/target-like strings ("factor XIa" corrupted to "actorXla")
    if any(k in low for k in TARGET_KEYWORDS) and not low.startswith("jnj-") and not _DRUG_SUFFIX.search(s):
        return False

    if re.search(r"\b(" + "|".join(map(re.escape, CORP_TOKENS)) + r")\b", low):
        return False

    if not re.search(r"[A-Za-z0-9]", s):
        return False

    if len(s) > 70:
        return False

    words = s.split()
    if len(words) > 6 and "jnj-" not in low:
        return False

    if low in {"others", "other", "unknown", "undisclosed"}:
        return False

    if looks_like_indication_label(s):
        return False

    if is_trial_acronym(s):
        return False

    if _PHASE_FRAGMENT.match(low.replace(" ", "")):
        return False

    return True


def indication_is_footer_noise(text: str) -> bool:
    low = (text or "").lower()
    return any(k in low for k in IND_DROP_IF_CONTAINS)


def sanitize_indication_text(raw: str) -> str:
    s = (raw or "").replace("\u00a0", " ")
    s = _WS.sub(" ", s).strip()

    s = _collapse_spaced_letters(s)

    # If it becomes extremely short nonsense (e.g. "ion" from "i o n"), drop it.
    if len(s) <= 3 and s.isalpha():
        return ""

    # De-glue camelCase and acronym boundaries
    if " " not in s and len(s) >= 25:
        s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
        s = re.sub(r"([A-Z]{2,})([A-Z][a-z])", r"\1 \2", s)

    # De-glue common lowercase runs
    if " " not in s and len(s) >= 20:
        for w in _GLUED_WORDS:
            s = re.sub(rf"(?i)([a-z])({re.escape(w)})([a-z])", r"\1 \2 \3", s)

    s = _WS.sub(" ", s).strip()
    low = s.lower()

    if indication_is_footer_noise(s):
        return ""

    for pat in IND_CUTOFF_PATTERNS:
        m = re.search(pat, low)
        if m:
            s = s[: m.start()].rstrip(" ;,-")
            break

    return s.strip()
