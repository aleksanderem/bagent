"""Deterministic body-area extractor for taxonomy disambiguation.

Stage-5 (2026-05-17) building block: every service name AND every
Booksy canonical_name gets a frozenset of body-area tokens via
deterministic regex matching. Two sets are compatible when they
overlap OR one of them is empty (generic, area-agnostic).

The bramka exists because the LLM disambiguation step (gpt-4o-mini)
empirically rationalizes high-confidence WRONG matches when the
candidate set lacks a same-area option. Example: "Thunder Całe ciało
1 zabieg" got booksy_tid=233 (Depilacja twarzy) with confidence 0.9
because the top-30 ANN list had no "całe ciało" candidate — LLM
locked onto "depilacja" match in method, ignored area mismatch.

Body-area enum is intentionally tight — covers Beauty4ever's cennik
(the salon currently in scope) plus the top Booksy canonical_names
that share these areas. Add new tags only when you've actually seen
them missed in production traces.

This module has NO LLM, NO async, NO I/O. Pure regex + unidecode.
Unit-test friendly.
"""

from __future__ import annotations

import re
import unicodedata


_POLISH_NON_NFKD = str.maketrans({"ł": "l", "Ł": "L"})


def _ascii_lower(text: str) -> str:
    """Normalize Polish diacritics + lowercase. NFKD strips combining
    accents (ą→a, ć→c, ę→e, ń→n, ó→o, ś→s, ź→z, ż→z) but ł/Ł are
    indivisible single-character glyphs in Unicode, so they survive
    NFKD untouched. Translate them manually before the strip.
    Without this, regex `cale\\s+cialo` never matches "całe ciało".
    """
    if not text:
        return ""
    pre = text.translate(_POLISH_NON_NFKD)
    nfkd = unicodedata.normalize("NFKD", pre)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower()


# Ordered list of (tag, list_of_regex_patterns). Earlier entries match
# first and are skipped on later entries IF a more-specific bikini or
# leg subarea has already been captured for this exact span. We use
# simple substring matching with word boundaries to keep behavior
# predictable. Patterns are applied to the ASCII-folded lowercase
# version of the text.
_BODY_AREA_PATTERNS: list[tuple[str, list[str]]] = [
    # Specific bikini variants — must match BEFORE generic "bikini"
    ("bikini_brazylijskie", [r"bikini\s+brazyl"]),
    ("bikini_pelne",        [r"bikini\s+pelne", r"bikini\s+pelny", r"bikini\s+pelna"]),
    ("bikini_podstawowe",   [r"bikini\s+podstawow"]),
    ("bikini_glebokie",     [r"bikini\s+glebok"]),
    ("bikini",              [r"\bbikini\b"]),  # generic fallback
    # Whole body — broadest possible single tag
    ("cale_cialo",          [r"cale\s+cialo", r"calym\s+cialem", r"\bsylwetka\b",
                             r"full\s+body", r"pelna\s+depilacja"]),
    # Klatka piersiowa + sutki (treat brodawki sutkowe as klatka subarea)
    ("klatka_piersiowa",    [r"klatka\s+piersiow", r"klatki\s+piersiow",
                             r"klatce\s+piersiow", r"brodawki\s+sutkow"]),
    # Twarz cluster
    ("wasik",               [r"\bwasik\b", r"\bwasika\b"]),
    ("broda",               [r"\bbroda\b", r"\bbrody\b", r"podbrod"]),
    ("baczki",              [r"\bbaczki\b", r"\bbaczek\b"]),
    ("miedzy_brwiami",      [r"miedzy\s+brwiami", r"linia\s+biala"]),
    ("twarz",               [r"\btwarz", r"\bface\b", r"\blicowa\b"]),
    # Środek tułowia
    ("dekolt",              [r"\bdekolt"]),
    ("kark_szyja",          [r"\bkark", r"\bszyj"]),
    ("plecy",               [r"\bplec[yo]\b", r"\bplecow\b"]),
    ("brzuch",              [r"\bbrzuch", r"\bbrzucha\b", r"\bboczki\b"]),
    ("pachy",               [r"\bpach\w*"]),
    # Ręce
    ("przedramiona",        [r"\bprzedrami"]),
    ("ramiona",             [r"\bramion", r"\bbark\b", r"\bbarkow\b"]),
    ("palce_rak",           [r"palc\w*\s+(?:u\s+)?rak", r"palc\w*\s+(?:u\s+)?dloni"]),
    ("dlonie",              [r"\bdloni\w*"]),
    ("rece",                [r"\brece\b", r"\brak\b", r"\brak[uo]\b"]),
    # Nogi
    ("uda",                 [r"\buda\b", r"\budo\b", r"\budach\b"]),
    ("lydki",               [r"\blydk\w*", r"\bpodudz\w*"]),
    ("kostki",              [r"\bkostk\w*"]),
    ("palce_stop",          [r"palc\w*\s+(?:u\s+)?stop"]),
    ("stopy",               [r"\bstop[ya]\b", r"\bstopach\b"]),
    ("nogi",                [r"\bnogi\b", r"\bnog\b", r"\bnogach\b"]),  # generic fallback
    # Okolice intymne — distinct from bikini (bikini = depilacja stref bikini)
    ("okolice_intymne",     [r"intymn"]),
    # Pośladki
    ("posladki",            [r"\bposladk", r"\bposladki\b"]),
]


def extract_body_areas(text: str) -> frozenset[str]:
    """Extract body-area tokens from text. Empty set means generic
    (area-agnostic) — service or candidate that doesn't specify any
    body region.

    Multi-area entries are normal (e.g. "Mała partia: bikini, pachy,
    twarz, stopy, dłonie" returns five tags). Compatibility check is
    a set-overlap, so multi-area services match candidates touching
    ANY of their areas.
    """
    norm = _ascii_lower(text)
    if not norm:
        return frozenset()
    found: set[str] = set()
    for tag, patterns in _BODY_AREA_PATTERNS:
        for pat in patterns:
            if re.search(pat, norm):
                found.add(tag)
                break
    return frozenset(found)


def areas_compatible(
    svc_areas: frozenset[str], cand_areas: frozenset[str]
) -> bool:
    """Return True iff service can plausibly be classified under candidate.

    Rules:
      1. If candidate has NO area markers → generic Booksy tid (e.g.
         "Depilacja laserowa"), accept any service. The Rule 3 LLM
         will still need to confirm method match.
      2. If service has NO area markers → service name is too generic
         to area-filter (e.g. just "Manicure"); accept any candidate.
      3. If both have area markers → require non-empty overlap.

    Note: this is the FILTER, not the final verdict. A service that
    passes this filter still goes through Rule 3 LLM disambiguation.
    A service that fails this filter falls through to Rule 4 (embedding
    inherit) or Rule 2 (salon-defined synthetic).
    """
    if not cand_areas:
        return True
    if not svc_areas:
        return True
    return bool(svc_areas & cand_areas)


def filter_candidates_by_area(
    svc_name: str,
    candidates: list[dict],
) -> tuple[list[dict], frozenset[str], list[dict]]:
    """Filter ANN candidate list by area compatibility with service name.

    Returns:
      (kept, svc_areas, dropped) where:
        - kept: list of candidates passing the area gate (preserves order)
        - svc_areas: extracted area set for the service (for logging)
        - dropped: list of candidates filtered out (for trace visibility)

    Each candidate dict must have a `canonical_name` (or `name`) field
    to extract its area set from.
    """
    svc_areas = extract_body_areas(svc_name)
    kept: list[dict] = []
    dropped: list[dict] = []
    for c in candidates:
        cand_text = (c.get("canonical_name") or c.get("name") or "")
        cand_areas = extract_body_areas(cand_text)
        if areas_compatible(svc_areas, cand_areas):
            kept.append(c)
        else:
            # Annotate the dropped candidate with WHY for trace inspector
            dropped_entry = dict(c)
            dropped_entry["_dropped_reason"] = (
                f"area_mismatch: svc_areas={sorted(svc_areas)} vs "
                f"cand_areas={sorted(cand_areas)}"
            )
            dropped.append(dropped_entry)
    return kept, svc_areas, dropped
