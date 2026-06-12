"""Kontekst trójkąta produktów BooksyAudit (FUNNEL_AUDIT R1+R3).

Model biznesowy (właściciel, 2026-06-11): trzy RÓWNOWAŻNE produkty-wierzchołki —
audyt profilu z optymalizacją AI, raport konkurencji, monitoring konkurencji.
Każdy może być pierwszym zakupem i każdy dosprzedażą w dowolnym kierunku;
w centrum trójkąta płatna konsultacja. Każda treść generowana przez LLM ma
znać wierzchołki klienta: referować zamiast powtarzać, brakujące komunikować
merytorycznie przy konkretnym wniosku, przy komplecie proponować konsultację.

Moduł best-effort: każdy check w try/except → brak danych traktujemy jak
„nie wiadomo / nie ma" i NIE blokujemy pipeline'u (AI jest dodatkiem).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ProductContext:
    has_audit: bool
    has_competitor_report: bool
    has_monitoring: bool

    @property
    def has_all(self) -> bool:
        return self.has_audit and self.has_competitor_report and self.has_monitoring

    def missing_labels(self) -> list[str]:
        out: list[str] = []
        if not self.has_audit:
            out.append("audyt profilu z optymalizacją AI")
        if not self.has_competitor_report:
            out.append("raport konkurencji")
        if not self.has_monitoring:
            out.append("monitoring konkurencji")
        return out


async def load_product_context(service: Any, convex_user_id: str) -> ProductContext:
    """Ustala wierzchołki klienta z Supabase (service = SupabaseService).

    - audyt: audit_reports.convex_user_id
    - raport konkurencji: competitor_reports(status=completed) dla audytów usera
    - monitoring: monitoring_refresh_schedule.user_id (mirror watchlist z Convex)
    """
    has_audit = False
    has_competitor_report = False
    has_monitoring = False

    audit_ids: list[str] = []
    try:
        res = (
            service.client.table("audit_reports")
            .select("convex_audit_id")
            .eq("convex_user_id", convex_user_id)
            .limit(25)
            .execute()
        )
        audit_ids = [
            r["convex_audit_id"] for r in (res.data or []) if r.get("convex_audit_id")
        ]
        has_audit = len(audit_ids) > 0
    except Exception as e:  # noqa: BLE001
        logger.warning("product_context: audit check failed (best-effort): %s", e)

    if audit_ids:
        try:
            res = (
                service.client.table("competitor_reports")
                .select("id")
                .in_("convex_audit_id", audit_ids)
                .eq("status", "completed")
                .limit(1)
                .execute()
            )
            has_competitor_report = bool(res.data)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "product_context: competitor report check failed (best-effort): %s", e
            )

    try:
        res = (
            service.client.table("monitoring_refresh_schedule")
            .select("booksy_id")
            .eq("user_id", convex_user_id)
            .limit(1)
            .execute()
        )
        has_monitoring = bool(res.data)
    except Exception as e:  # noqa: BLE001
        logger.warning("product_context: monitoring check failed (best-effort): %s", e)

    return ProductContext(
        has_audit=has_audit,
        has_competitor_report=has_competitor_report,
        has_monitoring=has_monitoring,
    )


async def load_product_context_for_audit(
    service: Any, convex_audit_id: str
) -> ProductContext:
    """Jak load_product_context, ale wejściem jest audit_id (synteza konkurencji
    zna tylko convex_audit_id) — user wyciągany z audit_reports."""
    try:
        res = (
            service.client.table("audit_reports")
            .select("convex_user_id")
            .eq("convex_audit_id", convex_audit_id)
            .maybe_single()
            .execute()
        )
        user_id = (res.data or {}).get("convex_user_id") if res else None
    except Exception as e:  # noqa: BLE001
        logger.warning("product_context: user lookup failed (best-effort): %s", e)
        user_id = None
    if not user_id:
        return ProductContext(
            has_audit=False, has_competitor_report=False, has_monitoring=False
        )
    return await load_product_context(service, user_id)


def build_prompt_block(ctx: ProductContext) -> str:
    """Blok do doklejenia do promptów generujących treść dla klienta."""
    own = []
    own.append(f"- audyt profilu z optymalizacją AI: {'TAK' if ctx.has_audit else 'NIE'}")
    own.append(f"- raport konkurencji: {'TAK' if ctx.has_competitor_report else 'NIE'}")
    own.append(f"- monitoring konkurencji: {'TAK' if ctx.has_monitoring else 'NIE'}")

    if ctx.has_all:
        upsell_rule = (
            "Klient ma KOMPLET produktów — nie dosprzedawaj modułów. Jeśli wnioski "
            "na to zasługują, zakończ JEDNYM zdaniem sugerującym płatną konsultację "
            "strategiczną (wdrożenie wniosków z ekspertem)."
        )
    else:
        missing = ", ".join(ctx.missing_labels())
        upsell_rule = (
            f"Brakujące produkty klienta: {missing}. Tam, gdzie konkretny wniosek "
            "byłby precyzyjniejszy lub łatwiejszy do wdrożenia dzięki brakującemu "
            "produktowi, dodaj JEDNO zdanie, co ten produkt odblokowuje — "
            "merytorycznie, przy tym wniosku, nie jako osobna reklama. "
            "Maksymalnie 2 takie wzmianki w całej treści."
        )

    return f"""=== KONTEKST PRODUKTU (trójkąt BooksyAudit) ===
Trzy równoważne produkty: audyt profilu z optymalizacją AI, raport konkurencji, monitoring konkurencji. Klient posiada:
{chr(10).join(own)}
Zasady komunikacji:
1. NIE powtarzaj wniosków z produktów, które klient JUŻ MA — odwołuj się do nich krótko (np. "szczegóły cen w raporcie konkurencji").
2. {upsell_rule}
3. Zero nachalności: żadnych list produktów, cenników ani marketingowych formułek — wyłącznie merytoryczne wzmianki przy konkretnych wnioskach."""
