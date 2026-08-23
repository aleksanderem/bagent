"""Etap 5 synthesis — limity schematu narzędzia + konfiguracja klienta.

Dwie niezależne wady (BEAUTY_AUDIT-mdpi):

(a) `_run_minimax_synthesis` podmieniał `client.client` na własnego
    `anthropic.AsyncAnthropic`, gubiąc przy tym nagłówek okna 1M
    (`anthropic-beta`), który ustawia konstruktor `MiniMaxClient`. Testy
    sprawdzają, że klient syntezy niesie nagłówek DOKŁADNIE wtedy, gdy niesie
    go MiniMaxClient (oba stany przełącznika `MINIMAX_1M_CONTEXT`), przy
    zachowaniu timeoutu 240 s i `max_retries=2`.

(b) Model potrafi wywołać `submit_competitor_insights` kilka razy; scalanie
    wywołań przekraczało limity `maxItems` ze schematu narzędzia i wpuszczało
    duplikaty. Testy sprawdzają przycięcie do limitu ODCZYTANEGO ZE SCHEMATU,
    deduplikację i brak regresji dla pojedynczego wywołania w limitach.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from pipelines.competitor_synthesis import (
    COMPETITOR_INSIGHTS_TOOL,
    _extract_insights_from_agent_result,
    _run_minimax_synthesis,
    _sanitize_insights,
)

# Limity czytane bezpośrednio ze schematu narzędzia — test nie zna liczb,
# tak samo jak nie ma ich znać kod produkcyjny.
_SCHEMA_PROPS = COMPETITOR_INSIGHTS_TOOL["input_schema"]["properties"]
_SWOT_PROPS = _SCHEMA_PROPS["swot"]["properties"]
_STRENGTHS_MAX = _SWOT_PROPS["strengths"]["maxItems"]
_RECS_MAX = _SCHEMA_PROPS["recommendations"]["maxItems"]

_VALID_IDS: dict[str, set[int]] = {
    "dimensional_score": {1001, 1002},
    "pricing_comparison": {456, 457},
    "service_gap": {301},
}
_VALID_COMPETITOR_IDS = {9411, 9471}

_DIM_REF = [{"type": "dimensional_score", "id": 1001}]
_PRICE_REF = [{"type": "pricing_comparison", "id": 456}]


def _bullet(text: str) -> dict[str, Any]:
    return {"text": text, "sourceDataPoints": list(_DIM_REF)}


def _rec(title: str) -> dict[str, Any]:
    return {
        "actionTitle": title,
        "actionDescription": f"Uzasadnienie dla: {title}",
        "category": "pricing",
        "impact": "high",
        "effort": "low",
        "confidence": 0.8,
        "sourceCompetitorIds": [9411],
        "sourceDataPoints": list(_PRICE_REF),
    }


def _tool_call(
    *,
    narrative: str | None = None,
    strengths: list[dict] | None = None,
    weaknesses: list[dict] | None = None,
    opportunities: list[dict] | None = None,
    threats: list[dict] | None = None,
    recommendations: list[dict] | None = None,
) -> SimpleNamespace:
    payload: dict[str, Any] = {
        "swot": {
            "strengths": strengths or [],
            "weaknesses": weaknesses or [],
            "opportunities": opportunities or [],
            "threats": threats or [],
        },
        "recommendations": recommendations or [],
    }
    if narrative is not None:
        payload["positioning_narrative"] = narrative
    return SimpleNamespace(name="submit_competitor_insights", input=payload)


def _agent_result(calls: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(
        tool_calls=calls,
        final_text="",
        total_steps=len(calls),
        total_input_tokens=0,
        total_output_tokens=0,
    )


def _sanitize(insights: dict[str, Any]) -> dict[str, Any]:
    return _sanitize_insights(
        insights,
        valid_ids=_VALID_IDS,
        valid_competitor_ids=_VALID_COMPETITOR_IDS,
    )


# ---------------------------------------------------------------------------
# (a) klient syntezy dziedziczy nagłówki z MiniMaxClient
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("enable_1m", [True, False])
async def test_synthesis_client_inherits_minimax_headers(
    monkeypatch: pytest.MonkeyPatch, enable_1m: bool,
) -> None:
    """Klient, z którym rusza agent loop syntezy, niesie nagłówek okna 1M
    dokładnie wtedy, gdy niesie go MiniMaxClient — i ma podniesiony timeout
    (240 s) oraz max_retries=2."""
    from config import settings
    from services.minimax import MiniMaxClient

    monkeypatch.setattr(settings, "minimax_1m_context", enable_1m)
    monkeypatch.setattr(settings, "minimax_api_key", "dummy-key-for-tests")
    monkeypatch.setattr(settings, "minimax_base_url", "https://minimax.invalid/anthropic")

    captured: dict[str, Any] = {}

    async def _fake_run_agent_loop(**kwargs: Any) -> SimpleNamespace:
        captured["client"] = kwargs["client"]
        return _agent_result([
            _tool_call(
                narrative="Pozycja salonu " + "x" * 90,
                strengths=[_bullet("Największe portfolio")],
            )
        ])

    monkeypatch.setattr("agent.runner.run_agent_loop", _fake_run_agent_loop)

    await _run_minimax_synthesis(context="ctx", tracer=None)

    synthesis_client = captured["client"].client
    reference = MiniMaxClient(
        settings.minimax_api_key,
        settings.minimax_base_url,
        settings.minimax_model,
    ).client

    expected_beta = reference.default_headers.get("anthropic-beta")
    assert synthesis_client.default_headers.get("anthropic-beta") == expected_beta
    # Sanity: przełącznik faktycznie steruje nagłówkiem w MiniMaxClient.
    assert (expected_beta is not None) is enable_1m
    # Autoryzacja też musi przetrwać klonowanie.
    assert synthesis_client.default_headers.get(
        "Authorization"
    ) == reference.default_headers.get("Authorization")
    # ...a podniesiony timeout i retry zostają.
    assert synthesis_client.timeout == httpx.Timeout(240.0, connect=15.0)
    assert synthesis_client.max_retries == 2
    assert str(synthesis_client.base_url).startswith("https://minimax.invalid")


# ---------------------------------------------------------------------------
# (b) limity ze schematu + deduplikacja
# ---------------------------------------------------------------------------


def test_merged_tool_calls_are_trimmed_to_schema_limits() -> None:
    """Dwa wywołania narzędzia z nadmiarem — po sanityzacji listy mieszczą się
    w limitach `maxItems` ze schematu."""
    per_call_strengths = _STRENGTHS_MAX + 1
    per_call_recs = _RECS_MAX  # dwa wywołania = 2x limit

    first = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[_bullet(f"Mocna A{i}") for i in range(per_call_strengths)],
        recommendations=[_rec(f"Akcja A{i}") for i in range(per_call_recs)],
    )
    second = _tool_call(
        strengths=[_bullet(f"Mocna B{i}") for i in range(per_call_strengths)],
        recommendations=[_rec(f"Akcja B{i}") for i in range(per_call_recs)],
    )

    merged = _extract_insights_from_agent_result(_agent_result([first, second]))
    assert len(merged["swot"]["strengths"]) == 2 * per_call_strengths
    assert len(merged["recommendations"]) == 2 * per_call_recs

    out = _sanitize(merged)

    assert len(out["swot"]["strengths"]) == _STRENGTHS_MAX
    assert len(out["recommendations"]) == _RECS_MAX
    # Prefiks, nie losowa próbka — kolejność modelu zachowana.
    assert out["swot"]["strengths"][0]["text"] == "Mocna A0"
    assert out["recommendations"][0]["actionTitle"] == "Akcja A0"


def test_trim_follows_schema_not_hardcoded_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zmiana `maxItems` w schemacie przesuwa limit — liczby nie są zaszyte."""
    monkeypatch.setitem(_SWOT_PROPS["strengths"], "maxItems", 2)

    call = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[_bullet(f"Mocna {i}") for i in range(6)],
    )
    out = _sanitize(_extract_insights_from_agent_result(_agent_result([call])))

    assert [b["text"] for b in out["swot"]["strengths"]] == ["Mocna 0", "Mocna 1"]


def test_no_trim_when_schema_has_no_max_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Brak `maxItems` dla danej listy = brak przycinania."""
    monkeypatch.delitem(_SCHEMA_PROPS["recommendations"], "maxItems")

    count = _RECS_MAX + 3
    call = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        recommendations=[_rec(f"Akcja {i}") for i in range(count)],
    )
    out = _sanitize(_extract_insights_from_agent_result(_agent_result([call])))

    assert len(out["recommendations"]) == count


def test_duplicates_across_calls_collapse_to_one() -> None:
    """Ten sam bullet SWOT / ta sama rekomendacja w dwóch wywołaniach →
    jedno wystąpienie (normalizacja: strip + casefold)."""
    first = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[_bullet("Największe portfolio"), _bullet("Wysokie oceny")],
        recommendations=[_rec("Obniż cenę endermologii")],
    )
    second = _tool_call(
        strengths=[_bullet("  NAJWIĘKSZE Portfolio  ")],
        recommendations=[_rec(" obniż cenę ENDERMOLOGII ")],
    )

    out = _sanitize(_extract_insights_from_agent_result(_agent_result([first, second])))

    assert [b["text"] for b in out["swot"]["strengths"]] == [
        "Największe portfolio",
        "Wysokie oceny",
    ]
    assert [r["actionTitle"] for r in out["recommendations"]] == [
        "Obniż cenę endermologii"
    ]


def test_duplicate_text_in_different_swot_lists_is_kept() -> None:
    """Deduplikacja jest w obrębie jednej listy — ten sam tekst jako mocna
    strona i jako szansa to dwa różne wnioski."""
    call = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[_bullet("Szeroka oferta")],
        opportunities=[_bullet("Szeroka oferta")],
    )
    out = _sanitize(_extract_insights_from_agent_result(_agent_result([call])))

    assert len(out["swot"]["strengths"]) == 1
    assert len(out["swot"]["opportunities"]) == 1


def test_trim_runs_after_dropping_bullets_without_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bullety bez ważnych referencji nie mogą zjadać limitu — najpierw
    odsiew, potem przycięcie."""
    monkeypatch.setitem(_SWOT_PROPS["strengths"], "maxItems", 2)

    ghost = {
        "text": "Halucynacja",
        "sourceDataPoints": [{"type": "dimensional_score", "id": 9999}],
    }
    call = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[ghost, ghost, _bullet("Realna A"), _bullet("Realna B")],
    )
    out = _sanitize(_extract_insights_from_agent_result(_agent_result([call])))

    assert [b["text"] for b in out["swot"]["strengths"]] == ["Realna A", "Realna B"]


def test_single_call_within_limits_unchanged() -> None:
    """Regresja: jedno wywołanie mieszczące się w limitach przechodzi
    sanityzację bez ucinania i bez zmian treści."""
    call = _tool_call(
        narrative="Pozycja salonu " + "x" * 90,
        strengths=[_bullet("Mocna 1"), _bullet("Mocna 2")],
        weaknesses=[_bullet("Słaba 1")],
        opportunities=[_bullet("Szansa 1")],
        threats=[_bullet("Zagrożenie 1")],
        recommendations=[_rec("Akcja 1"), _rec("Akcja 2"), _rec("Akcja 3")],
    )
    out = _sanitize(_extract_insights_from_agent_result(_agent_result([call])))

    assert out["swot"] == {
        "strengths": [
            {"text": "Mocna 1", "sourceDataPoints": _DIM_REF},
            {"text": "Mocna 2", "sourceDataPoints": _DIM_REF},
        ],
        "weaknesses": [{"text": "Słaba 1", "sourceDataPoints": _DIM_REF}],
        "opportunities": [{"text": "Szansa 1", "sourceDataPoints": _DIM_REF}],
        "threats": [{"text": "Zagrożenie 1", "sourceDataPoints": _DIM_REF}],
    }
    assert [r["actionTitle"] for r in out["recommendations"]] == [
        "Akcja 1", "Akcja 2", "Akcja 3",
    ]
    assert out["recommendations"][0] == {
        "actionTitle": "Akcja 1",
        "actionDescription": "Uzasadnienie dla: Akcja 1",
        "category": "pricing",
        "impact": "high",
        "effort": "low",
        "confidence": 0.8,
        "estimatedRevenueImpactGrosze": None,
        "sourceCompetitorIds": [9411],
        "sourceDataPoints": _PRICE_REF,
    }
    assert out["sanitizerDropped"] == {
        "swotDropped": 0,
        "recommendationsDropped": 0,
    }
