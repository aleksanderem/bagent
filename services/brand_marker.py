"""Brand/device marker extractor for taxonomy consistency clustering.

Stage-5 commit 2 building block: services from the same salon that
share (brand_marker, body_area_set) MUST resolve to the same
taxonomy decision. The brand marker is what distinguishes
"Thunder Całe ciało" from "Onda Całe ciało" — both are full-body
treatments but with different devices/methods/marks, and they
should NOT collapse to one tid even if their area sets coincide.

This module returns a SINGLE canonical brand string per service or
None if no brand marker was found (service is generic).

Enum is tight on purpose — covers what's currently observed in
Beauty4ever's cennik and the most common Polish beauty/medical-
aesthetics device brands. Extend ONLY when you see a marker missed
in production traces.
"""

from __future__ import annotations

import re

from services.body_area_taxonomy import _ascii_lower


# Ordered by specificity. First match wins. The patterns match on the
# ASCII-folded lowercase form so they don't need Polish character
# variants.
_BRAND_PATTERNS: list[tuple[str, str]] = [
    # Laser depilacja / resurfacing devices
    ("thunder",        r"\bthunder\b"),
    ("primex",         r"\bprimex\b"),
    ("pro_xn",         r"\bpro\s*xn\b"),
    ("soprano",        r"\bsoprano\b"),
    ("vectus",         r"\bvectus\b"),
    ("motus",          r"\bmotus\b"),
    ("clarity_ii",     r"\bclarity\s*ii?\b"),
    # 2026-05-17 — brand markers found in Beauty4ever audit 34 cennik but
    # missing from initial enum. Without these, brand-aware variant
    # clustering can't separate e.g. Red Touch dłonie from Laser diodowy
    # dłonie → false pricing comparisons.
    ("red_touch",      r"\bred\s*touch\b"),
    ("prx_t33",        r"\bprx\s*[-]?\s*t\s*33\b|\bprx\s*t33\b"),
    ("x_wave",         r"\bx\s*[-]?\s*wave\b"),
    ("estgen",         r"\bestgen\b|\best\s*gen\b"),
    ("aquashine",      r"\baquashine\b"),
    ("picosure",       r"\bpicosure\b"),
    ("tixel",          r"\btixel\b"),
    # Modeling sylwetki / RF / HiFEM / HiFU
    ("onda",           r"\bonda\b"),
    ("hifem",          r"\bhifem\b"),
    ("emsculpt",       r"\bemsculpt\b"),
    ("cooltech",       r"\bcooltech\b"),
    ("coolsculpting",  r"\bcoolsculpting\b"),
    ("ultraformer",    r"\bultraformer\b"),
    ("hifu",           r"\bhifu\b"),
    ("endermolab",     r"\bendermolab\b|\bendermolog\w*"),
    # Skin treatments / mezoterapia
    ("dermapen",       r"\bdermapen\b|\bdermapen\s*4\b"),
    ("dermaroller",    r"\bdermaroller\b"),
    ("hydrafacial",    r"\bhydrafacial\b|\bhydra\s*facial\b"),
    ("oxybrasion",     r"\boxybrasion\b|\boxy\s*brasion\b"),
    ("ipl",            r"\bipl\b"),
    ("shr",            r"\bshr\b"),
    # Laser tatto/skin
    ("nd_yag",         r"\bnd[: ]?yag\b|\bnd\s*yag\b"),
    ("aleksandryt",    r"aleksandry\w*"),
    ("diodowa",        r"diodow\w*"),
    ("alma",           r"\balma\b"),
    ("fotona",         r"\bfotona\b"),
    # Infusion / IV drip
    ("kroplowka_nad",  r"kroplowk\w*\s+nad\b|\bnad\b\s+kroplowk"),
    ("kroplowka",      r"\bkroplowk\w*"),
    ("iv_drip",        r"\biv[\s\-]*drip\b"),
    # Manicure systems
    ("japonski",       r"\bjapons\w*"),
]


def extract_brand_marker(*texts: str) -> str | None:
    """Extract a canonical brand marker from any of the provided texts.

    Pass service name FIRST, then category_name as fallback. The first
    pattern that matches in ANY of the joined texts wins. Returns None
    if no marker found — service is generic-method and clusters by
    body area only.

    Example:
      >>> extract_brand_marker("Thunder - Całe ciało 1 zabieg",
      ...                      "DEPILACJA LASEROWA THUNDER KOBIETA")
      'thunder'
      >>> extract_brand_marker("Manicure hybrydowy")
      None
    """
    haystack = " ".join(_ascii_lower(t or "") for t in texts)
    if not haystack.strip():
        return None
    for tag, pattern in _BRAND_PATTERNS:
        if re.search(pattern, haystack):
            return tag
    return None
