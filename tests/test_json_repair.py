"""Testy parsera naprawczego JSON — w tym DOSŁOWNE wzorce z produkcyjnych awarii
MiniMax M2.7 (2026-06-11): surowe \n w wartościach, polskie cytaty z prostym
zamknięciem, proza po bloku JSON, odpowiedzi ucięte przez max_tokens."""

from services.json_repair import parse_llm_json


def test_clean_json():
    assert parse_llm_json('{"a": 1, "b": [1, 2]}') == {"a": 1, "b": [1, 2]}


def test_markdown_fences():
    assert parse_llm_json('```json\n{"a": "x"}\n```') == {"a": "x"}


def test_prose_before_json():
    assert parse_llm_json('Oto wynik analizy:\n{"ok": true}') == {"ok": True}


def test_prose_after_json():
    text = '```json\n{"ok": true}\n```\nUwaga: priorytety nadałem według ważności.'
    assert parse_llm_json(text) == {"ok": True}


def test_think_block():
    assert parse_llm_json('<think>Przemyślmy {nie ten json}</think>\n{"a": 1}') == {"a": 1}


def test_raw_newline_inside_string_value():
    # Tryb awarii #3 — surowy \n wewnątrz wartości (invalid JSON)
    text = '{"alerts": [{"id": "a1", "meaning": "Konkurent buduje\nsiłę reputacji"}], "summary": "ok"}'
    result = parse_llm_json(text)
    assert result is not None
    assert result["summary"] == "ok"


def test_polish_quote_with_straight_close():
    # Tryb awarii #4 — DOSŁOWNY wzorzec z produkcji: „cytat" z prostym zamknięciem
    text = (
        '{"alerts": [{"id": "a1", "meaning": "Konkurent wycofał usługę „Ojciec + syn (włosy)"'
        ' za 110 zł – to nisza, którą warto rozważyć."}], "summary": "ok"}'
    )
    result = parse_llm_json(text)
    assert result is not None
    assert result["summary"] == "ok"
    meaning = result["alerts"][0]["meaning"]
    assert "Ojciec + syn" in meaning
    assert "za 110 zł" in meaning


def test_multiple_unescaped_quotes():
    text = (
        '{"a": "pakiet „manicure + zabezpieczenie" lub promocja",'
        ' "b": "cena „Broda + shaver" wzrosła", "c": 1}'
    )
    result = parse_llm_json(text)
    assert result is not None
    assert result["c"] == 1


def test_escaped_quotes_preserved():
    assert parse_llm_json('{"a": "powiedział \\"tak\\""}') == {"a": 'powiedział "tak"'}


def test_truncated_mid_string():
    text = '{"alerts": [{"id": "a1", "priority": "act", "meaning": "Konkurent obni'
    result = parse_llm_json(text)
    assert result is not None
    assert isinstance(result["alerts"], list)


def test_truncated_after_comma_in_array():
    assert parse_llm_json('{"items": [{"x": 1}, {"x": 2},') == {"items": [{"x": 1}, {"x": 2}]}


def test_truncated_nested_object():
    text = '{"summary": "ok", "focus": ["a", "b"], "alerts": [{"id": "x", "nested": {"k"'
    result = parse_llm_json(text)
    assert result is not None
    assert result["summary"] == "ok"


def test_garbage_returns_none():
    assert parse_llm_json("nie ma tu żadnego jsona") is None


def test_empty_returns_none():
    assert parse_llm_json("") is None


def test_top_level_array_returns_none():
    assert parse_llm_json("[1, 2, 3]") is None
