"""Deterministic treatment-method extractor for taxonomy consistency
keying.

Stage-5 commit 3 (anchors prep): the consistency anchors table is
keyed on `(brand_marker, method_marker, body_area_set)`. Without
method as an explicit axis, anchors collapse different treatments
into one key — e.g. Thunder pachy (laser) and Wax cukrowy pachy
(non-laser) would both key on `(*, ['pachy'])` and one anchor would
overwrite the other. Even worse: MiniMax in commit-2 trace empirically
picked Booksy tid 236 ('Depilacja pastą cukrową' — wax) for a
Thunder cluster, confusing method. Anchor lookup MUST disambiguate.

Method axes (broad enough to cover Booksy taxonomy):
  - laser     : laser depilation devices (Thunder, Onda, Diodowa,
                Aleksandryt, ND:YAG, SHR, IPL, Vectus, Soprano, ...)
  - wax       : tradycyjny wosk
  - cukrowa   : pasta cukrowa
  - rf        : radiofrekwencja (Onda RF, Endermolab RF, ...)
  - hifu      : ultrasound
  - hifem     : high-intensity focused electromagnetic (Emsculpt)
  - mezoterapia : igłowa/bezigłowa
  - hydrafacial : Hydrafacial-style oczyszczanie
  - kroplowka : IV-drip
  - microneedling : Dermapen, Dermaroller, microneedle RF
  - manicure  : manicure / pedicure
  - generic   : none of the above (treat as method-agnostic)

NO LLM. Pure regex. Unit-test friendly. Returns ONE canonical method
string per service — first match wins.
"""

from __future__ import annotations

import re

from services.body_area_taxonomy import _ascii_lower


# Method patterns. Ordered most-specific first. Each tag has list of
# regex patterns that match on the ascii-folded lowercase haystack.
_METHOD_PATTERNS: list[tuple[str, list[str]]] = [
    # Devices / explicit laser brands → laser
    ("laser", [
        r"\blaser\w*",
        r"\bthunder\b",
        r"\bipl\b",
        r"\bshr\b",
        r"\bdiodow\w*",
        r"aleksandry\w*",
        r"\bnd[: ]?yag\b|\bnd\s*yag\b",
        r"\bvectus\b",
        r"\bsoprano\b",
        r"\bmotus\b",
        r"\bclarity\s*ii?\b",
        r"\bpicosure\b",
        r"\bfotona\b",
        r"\btixel\b",
        r"\bprimex\b",  # IPL hybrid device
        r"\bpro\s*xn\b",  # laser line
        r"\bdepilacja\s+laserow",
    ]),
    # Sugar paste (specific, Polish word for sugaring)
    ("cukrowa", [r"cukrow\w*", r"\bsugaring\b"]),
    # Classic wax — keyword "wosk"
    ("wax", [r"\bwosk\w*", r"\bwax\b", r"depilacja\s+wosk"]),
    # HiFEM / EMS body sculpting
    ("hifem", [r"\bhifem\b", r"\bemsculpt\b", r"\bems\b\b"]),
    # HiFU ultrasound
    ("hifu", [r"\bhifu\b", r"ultraforme\w*"]),
    # Microneedling (Dermapen, microneedle RF)
    ("microneedling", [
        r"\bdermapen\b",
        r"\bdermaroller\b",
        r"mikroig\w*",
        r"microneedl\w*",
    ]),
    # Mezoterapia (osobno od microneedling — igłowa / bezigłowa)
    ("mezoterapia", [r"mezoterap\w*"]),
    # Hydrafacial / oxybrazja peelingi
    ("hydrafacial", [
        r"\bhydra\s*facial\b",
        r"\boxybrazj\w*",
        r"\boxybrasion\b",
    ]),
    # Kroplówki / IV-drip
    ("kroplowka", [r"\bkroplowk\w*", r"\biv[\s\-]?drip\b"]),
    # Radiofrekwencja — Onda (jako produkt Cinetics) używa RF, nie lasera;
    # body sculpting urządzenia ogólnie radiofrekwencyjne.
    ("rf", [
        r"\brf\b",
        r"radiofrekwencj\w*",
        r"endermolab\w*",
        r"\bonda\b",
        r"\bcooltech\b",
        r"\bcoolsculpting\b",
    ]),
    # Manicure / pedicure
    ("manicure", [
        r"\bmanicure\b",
        r"\bpedicure\b",
        r"\bmanikiur\w*",
        r"\bpedikiur\w*",
        r"hybrydow\w*",
    ]),
]


def extract_method_marker(*texts: str) -> str:
    """Extract canonical method tag. Returns 'generic' when no marker
    is found — those services aren't method-specific (e.g. generic
    skincare consultations).
    """
    haystack = " ".join(_ascii_lower(t or "") for t in texts)
    if not haystack.strip():
        return "generic"
    for tag, patterns in _METHOD_PATTERNS:
        for pat in patterns:
            if re.search(pat, haystack):
                return tag
    return "generic"
