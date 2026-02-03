from __future__ import annotations

import re
from typing import Optional


_WS = re.compile(r"\s+")
_LEADING_BULLETS = re.compile(r"^[\u2022\-\*\•\·\u00b7]+\s*")
_ALLOWED = re.compile(r"[^A-Za-z0-9\-\+\./\(\) ]+")

# things that frequently appear as headings/labels in the J&J PDF and are NOT assets
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

# corporate/partner noise (if you see these in a supposed "asset", it's probably not an asset)
CORP_TOKENS = {
    "plc", "inc", "ltd", "corp", "corporation", "ag", "gmbh",
    "therapeutics", "biosciences", "pharma", "pharmaceutical", "biotech",
}

# disclaimer-ish text that sometimes leaks into indications
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
    # remove extra closing parens at end if unbalanced
    while s.endswith(")") and s.count("(") < s.count(")"):
        s = s[:-1].rstrip()
    # remove extra opening parens at start if unbalanced
    while s.startswith("(") and s.count("(") > s.count(")"):
        s = s[1:].lstrip()
    return s


def sanitize_asset_label(raw: str) -> Optional[str]:
    """
    Clean an extracted asset label.
    Returns None if empty after cleaning.
    """
    if raw is None:
        return None

    s = str(raw)
    s = s.replace("\u00a0", " ")  # nbsp
    s = _LEADING_BULLETS.sub("", s.strip())
    s = _WS.sub(" ", s).strip()

    # remove common prefix noise like "system)" or "platform)"
    s = re.sub(r"^(system|platform)\)\s*", "", s, flags=re.IGNORECASE)

    # remove junk characters, keep a conservative whitelist
    s = _ALLOWED.sub("", s)
    s = _WS.sub(" ", s).strip()

    # collapse OCR spaced letters if needed
    s = _collapse_spaced_letters(s)

    # strip unbalanced parentheses
    s = _strip_unbalanced_parens(s)

    # final normalize whitespace
    s = _WS.sub(" ", s).strip()

    return s or None


def sanitize_alias(raw: str) -> Optional[str]:
    """
    Slightly looser than asset_label; still removes obvious junk.
    """
    s = sanitize_asset_label(raw)
    if not s:
        return None
    # aliases like "TALVEY + TECVAYLI" are okay, keep plus
    return s


def is_plausible_asset_label(label: str) -> bool:
    """
    Heuristics to reject headings/footers/partner lists misread as assets.
    """
    if not label:
        return False

    s = label.strip()
    low = s.lower()

    if low in STOP_ASSET_EXACT:
        return False

    # obvious partner lists / garbage
    if ";" in s:
        return False

    # too many words is almost never an asset name (except codes like JNJ-xxxx)
    words = s.split()
    if len(words) > 8 and "jnj-" not in low:
        return False

    # corporate tokens + punctuation often indicates partner block, not asset
    if any(tok in low for tok in CORP_TOKENS) and ("jnj-" not in low):
        return False

    # must contain at least one letter or digit
    if not re.search(r"[A-Za-z0-9]", s):
        return False

    # avoid super long garbage
    if len(s) > 80:
        return False

    return True


def sanitize_indication_text(raw: str) -> str:
    s = (raw or "").replace("\u00a0", " ")
    s = _WS.sub(" ", s).strip()

    # cut off disclaimer-ish tails if they leak into indication blocks
    low = s.lower()
    for pat in IND_CUTOFF_PATTERNS:
        m = re.search(pat, low)
        if m:
            s = s[: m.start()].rstrip(" ;,-")
            break

    return s.strip()
