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

    return True


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
