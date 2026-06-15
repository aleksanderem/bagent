"""Dźwignia 1 (2026-06-15 precision fix): alias-match confidence must rank
by specificity so the branded/specific method wins is_primary over a generic
descriptor word.

Root cause this guards: "estGen® - Krem aktywny" matched BOTH `estgen`
(branded) and `krem` (generic). With flat 1.0 confidence the is_primary
trigger could pick `krem`, poisoning the structured/method pricing tiers.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from services.method_classifier import (
    ALIAS_CONF_BRANDED,
    ALIAS_CONF_GENERIC,
    ALIAS_CONF_SPECIFIC,
    MethodClassifier,
    MethodRow,
    _alias_match_confidence,
)


def test_branded_alias_is_highest():
    assert _alias_match_confidence("estgen", "estgen", "estgen") == ALIAS_CONF_BRANDED


def test_generic_short_single_word_is_lowest():
    assert _alias_match_confidence(None, "krem", "krem") == ALIAS_CONF_GENERIC


def test_multi_token_canonical_is_specific():
    # canonical has "_" → it's a real compound method, not a bare descriptor.
    assert _alias_match_confidence(None, "mezoterapia_iglowa", "mezo") == ALIAS_CONF_SPECIFIC


def test_long_single_word_alias_is_specific():
    # len>=6 single word (e.g. "radiofrekwencja") is specific enough.
    assert _alias_match_confidence(None, "lifting", "radiofrekwencja") == ALIAS_CONF_SPECIFIC


def _row(mid, canon, brand, aliases):
    return MethodRow(
        id=mid, canonical_name=canon, display_name=canon, category="inny",
        method_type="substance", brand_family=brand, aliases=aliases,
        source="curated" if brand else "llm_inferred",
    )


def test_classify_by_alias_multi_branded_beats_generic():
    """The actual estGen-Krem regression: branded estgen outranks generic krem."""
    clf = MethodClassifier(supabase=MagicMock())
    clf._methods = {
        81: _row(81, "estgen", "estgen", ["estgen"]),
        99: _row(99, "krem", None, ["krem"]),
    }
    clf._alias_index = [("estgen", 81), ("krem", 99)]

    matches = clf._classify_by_alias_multi("estgen krem aktywny")
    by_id = {m.method_id: m for m in matches}

    assert set(by_id) == {81, 99}  # both still matched (multi-match preserved)
    assert by_id[81].confidence == ALIAS_CONF_BRANDED
    assert by_id[99].confidence == ALIAS_CONF_GENERIC
    # Branded ranks strictly higher → wins the is_primary trigger tiebreak.
    assert by_id[81].confidence > by_id[99].confidence
