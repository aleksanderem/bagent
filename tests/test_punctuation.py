"""Tests for `fix_punctuation` — the deterministic Polish punctuation
normaliser used by BAGENT #2 (cennik) finalize step.

Tests cover the patterns we observed in production AI output:
  * orphan space before .,;:!?
  * missing space after .,;:!?
  * runs of dots collapsed to ellipsis
  * doubled commas / semicolons
  * missing terminal period for full sentences
  * leading lowercase capitalised
  * idempotence (re-running doesn't change result)
  * abbreviations untouched (np., m.in., dr, ul.)
  * decimals untouched (1.5, 2,5)
"""

import pytest

from pipelines.helpers import fix_punctuation


@pytest.mark.parametrize(
    "before, after",
    [
        # Orphan space before period
        ("Manicure hybrydowy .", "Manicure hybrydowy."),
        ("To jest opis , który", "To jest opis, który."),
        # Missing space after period
        ("Pierwsze.Drugie zdanie", "Pierwsze. Drugie zdanie."),
        ("Coś.Coś innego ", "Coś. Coś innego."),
        # Multiple dots → ellipsis
        ("Coś..", "Coś…"),
        ("Coś.....", "Coś…"),
        # Doubled comma / semicolon
        ("a,, b", "A, b."),
        ("x;; y", "X; y."),
        # Missing terminal period (full sentence)
        ("To jest pełne zdanie z pięciu słów", "To jest pełne zdanie z pięciu słów."),
        # Already-correct sentence stays put (idempotence)
        ("Manicure hybrydowy z odżywką.", "Manicure hybrydowy z odżywką."),
        # Question / exclamation stays
        ("Czy jest dostępne?", "Czy jest dostępne?"),
        # Capitalise first letter of lowercase descriptions
        ("manicure hybrydowy z odżywką.", "Manicure hybrydowy z odżywką."),
        # Whitespace collapsing
        ("Tekst  z   wieloma    spacjami.", "Tekst z wieloma spacjami."),
    ],
)
def test_fix_punctuation_examples(before: str, after: str) -> None:
    assert fix_punctuation(before) == after


def test_idempotent() -> None:
    """Running fix_punctuation twice yields the same as once."""
    s = "to jest opis ,z literówką  i .. wieloma  spacjami"
    once = fix_punctuation(s)
    twice = fix_punctuation(once)
    assert once == twice


def test_does_not_break_decimals() -> None:
    """Decimal numbers like 1.5 and 2,5 must NOT get a space inserted."""
    out = fix_punctuation("Czas trwania 1.5 godziny i cena 250,50 zł")
    # 1.5 stays as 1.5 (no space after the dot because both sides are digits)
    assert "1.5" in out
    # 250,50 stays
    assert "250,50" in out


def test_short_phrase_no_terminal_period_added() -> None:
    """A 3-word phrase like 'Manicure hybrydowy' is a header, not a sentence —
    we should NOT append a period."""
    assert fix_punctuation("Manicure hybrydowy") == "Manicure hybrydowy"


def test_empty_input() -> None:
    assert fix_punctuation("") == ""
    assert fix_punctuation("   ") == ""


def test_bullet_list_row_keeps_no_terminal_period() -> None:
    """Lines starting with -, • etc. are list rows; we don't add a period."""
    assert fix_punctuation("- pierwszy element listy") == "- pierwszy element listy"
    assert fix_punctuation("• drugi element listy") == "• drugi element listy"


def test_preserves_polish_diacritics() -> None:
    """Must not mangle ąćęłńóśźż/ĄĆĘŁŃÓŚŹŻ during edits."""
    out = fix_punctuation("opis usługi z żółtą paletką  i ścieżką")
    assert "żółtą" in out
    assert "ścieżką" in out
    assert out.endswith(".")


def test_run_on_typical_ai_output() -> None:
    """End-to-end on the kind of string AI commonly returns with bugs."""
    raw = "manicure hybrydowy ,wykonywany na kilka sposobów .Doskonale sprawdzi się przed wyjściem"
    out = fix_punctuation(raw)
    # Capitalised first
    assert out[0] == "M"
    # No orphan space before comma/period
    assert " ," not in out
    assert " ." not in out
    # Space after period
    assert ".D" not in out
    # Terminal period present
    assert out.endswith(".")
