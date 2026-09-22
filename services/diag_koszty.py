"""Koszty i tokeny modeli za ostatnią dobę — sekcja autodiagnostyki „koszty”.

Panel admina pokazuje z tego, ile każdy mechanizm (audyt, synteza, destylacja…)
wywołał modelu, ile zjadł tokenów, ile to dolarów i ile wywołań padło. Dane
z jednego zapytania (`fn_ai_zuzycie_doba`, mig 199) — podsumowanie dobowe,
nie log wywołań.

Kwota bywa pusta i to poprawny stan: tokeny znamy zawsze, cenę tylko dla modeli
z wpisem w `services/ceny_modeli.py`. Wtedy w wierszu rośnie `bez_ceny`.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_RPC = "fn_ai_zuzycie_doba"
# Panel i tak pokazuje sumę; lista pozycji ma być czytelna, nie kompletna.
_MAX_POZYCJI = 12


def _num(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def zbierz(client: Any | None) -> dict[str, Any]:
    """Sekcja „koszty” do /api/internal/diag. `client` = klient Supabase albo None."""
    if client is None:
        return {"error": "brak połączenia z bazą"}
    try:
        rows = client.rpc(_RPC, {}).execute().data or []
    except Exception as exc:  # noqa: BLE001 — diagnostyka nie może wywrócić endpointu
        logger.warning("diag kosztów niedostępny (%s): %s", type(exc).__name__, str(exc)[:160])
        return {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}

    pozycje = [
        {
            "mechanizm": r.get("mechanizm") or "?",
            "dostawca": r.get("dostawca") or "?",
            "model": r.get("model") or "?",
            "wywolan": int(_num(r.get("wywolan"))),
            "bledow": int(_num(r.get("bledow"))),
            "tokeny_wej": int(_num(r.get("tokeny_wej"))),
            "tokeny_wyj": int(_num(r.get("tokeny_wyj"))),
            "usd": None if r.get("usd") is None else round(_num(r.get("usd")), 6),
            "bez_ceny": int(_num(r.get("bez_ceny"))),
            "usd_szacunek": bool(r.get("usd_szacunek")),
        }
        for r in rows
        if isinstance(r, dict)
    ]
    return {
        "pozycje": pozycje[:_MAX_POZYCJI],
        "mechanizmow": len(pozycje),
        "wywolan": sum(p["wywolan"] for p in pozycje),
        "bledow": sum(p["bledow"] for p in pozycje),
        "tokeny": sum(p["tokeny_wej"] + p["tokeny_wyj"] for p in pozycje),
        "usd": round(sum(p["usd"] or 0.0 for p in pozycje), 4),
        "bez_ceny": sum(p["bez_ceny"] for p in pozycje),
        "szacunek": any(p["usd_szacunek"] for p in pozycje),
    }
