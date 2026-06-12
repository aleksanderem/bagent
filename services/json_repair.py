"""LLM JSON repair — port sprawdzonego algorytmu z BEAUTY_AUDIT (convex/strategy/parse.ts).

Realne tryby awarii MiniMax M2.7 potwierdzone produkcyjnie 2026-06-11 (mimo stop_reason
= end_turn):
  1. markdown fences (```json ... ```) oraz proza PRZED i PO bloku JSON,
  2. bloki <think>...</think> (modele reasoningowe),
  3. SUROWE znaki nowej linii/tab wewnątrz wartości stringów (nielegalne w JSON),
  4. polskie cytaty zamykane PROSTYM cudzysłowem — `„Ojciec + syn (włosy)"` — prosty `"`
     ucina string w połowie wartości,
  5. odpowiedź ucięta w połowie stringa/klucza/tablicy (max_tokens).

Algorytm (kolejność prób):
  a) zdejmij <think> + fences, utnij wszystko przed pierwszym '{',
  b) kandydat przycięty do OSTATNIEGO '}' (usuwa prozę po JSON) — raw → sanityzowany →
     z escapowanymi cudzysłowami,
  c) pełny tekst: sanityzacja znaków kontrolnych + escapowanie cudzysłowów,
  d) naprawa uciętych: domknięcie STOSOWE nawiasów (z domknięciem otwartego stringa);
     gdy ogon niedomykalny — iteracyjne przycinanie do ostatniego separatora
     strukturalnego (',' '{' '[') i powrót do (d); max 12 iteracji.

Zwraca dict albo None — wołający decyduje o fallbacku. Zero zależności poza stdlib.
Testy: tests/test_json_repair.py (w tym dosłowne próbki z produkcyjnych awarii).
"""

from __future__ import annotations

import json
import re

_MAX_REPAIR_ITERATIONS = 12
_CONTROL_CHARS = re.compile(r"[\x00-\x1f]")
_THINK_BLOCK = re.compile(r"<think>[\s\S]*?</think>")
_JSON_FENCE = re.compile(r"```(?:json)?\s*")
_TRAILING_COMMA = re.compile(r",\s*$")
_STRING_TERMINATORS = {",", "}", "]", ":", ""}


def _sanitize_control_chars(text: str) -> str:
    """Surowe znaki kontrolne → spacje (raw newline w stringu = invalid JSON;
    między tokenami whitespace jest neutralny, więc podmiana jest bezpieczna)."""
    return _CONTROL_CHARS.sub(" ", text)


def _escape_unescaped_quotes(text: str) -> str:
    """Escapuje nieescape'owane proste cudzysłowy WEWNĄTRZ wartości stringów.

    Heurystyka: '"' wewnątrz stringa jest terminatorem tylko wtedy, gdy następny
    niebiały znak jest strukturalny (, } ] : albo koniec tekstu); w przeciwnym
    razie to cudzysłów w treści → zamieniamy na \\".
    """
    out: list[str] = []
    in_string = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if not in_string:
            if ch == '"':
                in_string = True
            out.append(ch)
            i += 1
            continue
        if ch == "\\":
            out.append(ch)
            if i + 1 < n:
                out.append(text[i + 1])
                i += 2
            else:
                i += 1
            continue
        if ch == '"':
            j = i + 1
            while j < n and text[j] in " \t\r\n":
                j += 1
            nxt = text[j] if j < n else ""
            if nxt in _STRING_TERMINATORS:
                in_string = False
                out.append(ch)
            else:
                out.append('\\"')
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _close_with_stack(text: str) -> str:
    """Domyka niesparowane nawiasy w kolejności odwrotnej do otwarcia (stos);
    odpowiedź uciętą w środku stringa najpierw domyka cudzysłowem."""
    stack: list[str] = []
    in_string = False
    escape = False
    for ch in text:
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            stack.append("}")
        elif ch == "[":
            stack.append("]")
        elif ch in "}]" and stack:
            stack.pop()
    result = text
    if in_string:
        result += '"'
    while stack:
        result += stack.pop()
    return result


def _try_parse(text: str) -> dict | None:
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def parse_llm_json(text: str) -> dict | None:
    """Parsuje odpowiedź LLM do dict-a, naprawiając znane tryby awarii.

    Zwraca None gdy tekstu nie da się uratować — wołający decyduje o fallbacku
    (retry / inny provider / deterministyczny fallback).
    """
    if not text:
        return None

    # (a) <think> + fences + tekst przed pierwszym '{'
    cleaned = _JSON_FENCE.sub("", _THINK_BLOCK.sub("", text)).replace("```", "").strip()
    start = cleaned.find("{")
    if start == -1:
        return None
    cleaned = cleaned[start:]

    # (b) kandydat do ostatniego '}' — usuwa prozę doklejoną po JSON
    last_brace = cleaned.rfind("}")
    if last_brace != -1:
        sliced = cleaned[: last_brace + 1]
        sliced_sanitized = _sanitize_control_chars(sliced)
        for candidate in (sliced, sliced_sanitized, _escape_unescaped_quotes(sliced_sanitized)):
            parsed = _try_parse(candidate)
            if parsed is not None:
                return parsed

    # (c) pełny tekst po sanityzacji + escapowaniu cudzysłowów
    sanitized = _escape_unescaped_quotes(_sanitize_control_chars(cleaned))
    parsed = _try_parse(sanitized)
    if parsed is not None:
        return parsed

    # (d) naprawa uciętych odpowiedzi
    candidate = sanitized
    for _ in range(_MAX_REPAIR_ITERATIONS):
        repaired = _try_parse(_close_with_stack(_TRAILING_COMMA.sub("", candidate)))
        if repaired is not None:
            return repaired
        cut_at = max(candidate.rfind(","), candidate.rfind("{"), candidate.rfind("["))
        if cut_at <= 0:
            break
        candidate = candidate[:cut_at]
    return None
