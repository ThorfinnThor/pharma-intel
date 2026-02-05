from __future__ import annotations

import re


_ws = re.compile(r"\s+")
_punct = re.compile(r"[^a-z0-9\-\+\./ ]+")


def norm_text(s: str) -> str:
    s = s.strip().lower()
    s = _ws.sub(" ", s)
    s = _punct.sub("", s)
    return s.strip()


def split_asset_aliases(asset_label: str) -> tuple[str, list[str]]:
    '''
    Heuristic alias splitter:
    - "BRAND (generic)" -> canonical = BRAND, aliases include BRAND and generic
    - "JNJ-1900 (NBTXR3)" -> canonical = JNJ-1900, aliases include JNJ-1900 and NBTXR3
    - "TALVEY + TECVAYLI" -> canonical = TALVEY + TECVAYLI, aliases include both TALVEY and TECVAYLI
    '''
    label = asset_label.strip()

    aliases: list[str] = [label]

    # combo separators (+, /, ;, ,)
    combo_parts: list[str] = [label]
    for sep in ["+", "/", ";", ","]:
        if sep in label:
            next_parts: list[str] = []
            for part in combo_parts:
                if sep in part:
                    next_parts.extend([p.strip() for p in part.split(sep) if p.strip()])
                else:
                    next_parts.append(part)
            combo_parts = next_parts

    # split on " with " for regimen strings
    expanded: list[str] = []
    for part in combo_parts:
        if re.search(r"\bwith\b", part, flags=re.IGNORECASE):
            expanded.extend([p.strip() for p in re.split(r"\bwith\b", part, flags=re.IGNORECASE) if p.strip()])
        else:
            expanded.append(part)

    for p in expanded:
        if p and p not in aliases:
            aliases.append(p)

    # parenthetical
    m = re.match(r"^(.*?)\((.*?)\)\s*$", label)
    if m:
        outer = m.group(1).strip()
        inner = m.group(2).strip()
        canonical = outer if outer else label
        # inner might include multiple terms
        for part in re.split(r"[;/,]", inner):
            part = part.strip()
            if part:
                aliases.append(part)
        # keep full outer too
        if outer and outer not in aliases:
            aliases.append(outer)
        return canonical, dedupe_preserve(aliases)

    # Unbalanced parenthetical at end: "INLEXZO (gemcitabine"
    m2 = re.match(r"^(.*?)\(([^)]{2,80})\s*$", label)
    if m2:
        outer = m2.group(1).strip()
        inner = m2.group(2).strip()
        canonical = outer if outer else label
        if outer and outer not in aliases:
            aliases.append(outer)
        for part in re.split(r"[;/,]", inner):
            part = part.strip()
            if part:
                aliases.append(part)
        return canonical, dedupe_preserve(aliases)

    return label, dedupe_preserve(aliases)


def dedupe_preserve(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        k = norm_text(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out
