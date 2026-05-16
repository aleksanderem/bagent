"""Hidden service inference — przypisuje brand-only / niepoprawnie nazwanym
usługom (Thunder, Light&Bright, EMBODY, etc.) najlepiej pasującą kategorię
z taksonomii Booksy.

Po co
-----
Salon owner robi "Thunder - całe ciało" jako nazwa usługi. Booksy szuka
PO NAZWIE — klient wpisuje "depilacja Warszawa" i ten salon się NIE
pokazuje, bo "Thunder" nie zawiera słowa "depilacja". Empirycznie
zweryfikowane (booksy.com/pl-pl/s/depilacja/3_warszawa) — Beauty4ever ma
38 wystąpień "depilacja" w opisach + kategorię "🔲 DEPILACJA LASEROWA
THUNDER KOBIETA" i NIE jest w TOP 20 wyników.

W populacji ~15 634 salonów (28%) ma takich usług, średnio 7.4/salon,
łącznie ~115k. Dla tych services chcemy sugerować nazwy z prefixem
zawierającym realny `booksy_treatment_id` canonical name żeby weszły
do indeksu Booksy.

Pipeline
--------
1. Embedding match (RPC `match_booksy_taxonomy`) → top-30 candidates
   z rich centroidów (AVG(name_embedding) per tid).
2. LLM (MiniMax M2.7) wybiera 1 z top-30 mając nazwę + opis usługi
   + kontekst parent category.
3. Fallback: jeśli brak opisu / LLM unavailable → top-1 embedding
   z capped confidence 0.5 (świadomie niska).
4. Jeśli żaden krok nie da confidence ≥ 0.65 → metoda='unfixable',
   UI flaguje jako "potrzeba ręcznej weryfikacji".

Wejście: dict z `name`, `description`, `name_embedding`.
Wyjście: dict z `inferred_tid`, `inferred_canonical_name`,
`inferred_parent_name`, `confidence`, `method`, `reasoning`,
`candidate_count`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from services.minimax import MiniMaxClient, with_retry
from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# Threshold poniżej którego nie ufamy wynikowi (oznaczamy jako unfixable).
# Empirycznie dobrane: LLM zwraca 0.7-0.95 dla pewnych dopasowań, 0.4-0.6
# dla "może to ale nie jestem pewien". 0.65 to próg "trust it".
DEFAULT_MIN_CONFIDENCE = 0.65

# Embedding-only fallback (gdy brak opisu albo LLM padł) ma celowo
# capped confidence — pure embedding pokazał się słaby w POC (top-1 często
# źle dla brand-only services).
EMBEDDING_FALLBACK_MAX_CONFIDENCE = 0.5

# Top-K candidates z embedding RPC. 30 jest sweet spot:
# - Mniej (np. 10) → poprawna kategoria czasem poza shortlist
#   (POC: Thunder → "Depilacja ciała" na pozycji 22)
# - Więcej (np. 50) → token bloat dla LLM bez gain'u
DEFAULT_TOP_K = 30


_SYSTEM_PROMPT = """Jesteś ekspertem od taksonomii Booksy.pl — platformy rezerwacji usług beauty/wellness w Polsce.

Dostaniesz usługę z salonu beauty (nazwa + opis) i listę candidate'ów z oficjalnej taksonomii Booksy (kategoria + parent category). Twoja praca: wybrać JEDNĄ kategorię która najlepiej opisuje rzeczywistą procedurę.

Reguły:
- Jeśli usługa to depilacja laserowa (Thunder, Soprano, Diode, Alex, etc.) → wybierz kategorię z parent="Depilacja"
- Jeśli to IPL/fotoodmładzanie → wybierz "Fotoodmładzanie" lub podobne z parent="Medycyna Estetyczna"
- Jeśli to modelowanie sylwetki / cellulit / kawitacja → wybierz "Zabiegi na ciało i modelowanie sylwetki" lub podobne
- Jeśli żadna z candidate'ów dobrze nie pasuje → zwróć tid=null
- NIE wymyślaj tid'a spoza listy

Zwracaj WYŁĄCZNIE JSON: {"tid": int|null, "confidence": float, "reasoning": "string"}
- tid: id wybranej kategorii z listy (lub null)
- confidence: 0.0-1.0, twoja pewność
- reasoning: 1 krótkie zdanie po polsku co cię przekonało"""


def _build_user_prompt(
    service_name: str,
    service_description: str | None,
    candidates: list[dict[str, Any]],
) -> str:
    desc = (service_description or "").strip()[:500]
    cand_lines: list[str] = []
    for c in candidates:
        parent = c.get("parent_canonical_name")
        parent_str = f" — parent: {parent}" if parent else ""
        cand_lines.append(
            f"  tid={c['tid']}: {c['canonical_name']}{parent_str}"
        )
    cand_block = "\n".join(cand_lines)
    return f"""Usługa do skategoryzowania:
- Nazwa: {service_name}
- Opis: {desc or "(brak opisu)"}

Lista kategorii Booksy (top dopasowania embedding):
{cand_block}

Wybierz JEDNĄ kategorię która najlepiej oddaje rzeczywistą procedurę."""


async def match_taxonomy_candidates(
    supabase: SupabaseService,
    embedding: list[float] | None,
    *,
    top_k: int = DEFAULT_TOP_K,
) -> list[dict[str, Any]]:
    """Wraps `match_booksy_taxonomy` RPC. Returns top-k candidates or []."""
    if not embedding:
        return []

    def _do_call() -> Any:
        return supabase.client.rpc(
            "match_booksy_taxonomy",
            {"p_embedding": embedding, "p_top_k": top_k},
        ).execute()

    try:
        res = await asyncio.to_thread(_do_call)
        return list(res.data or [])
    except Exception as e:
        logger.warning("match_booksy_taxonomy RPC failed: %s", e)
        return []


def _unfixable_result(reason: str, candidate_count: int = 0) -> dict[str, Any]:
    return {
        "inferred_tid": None,
        "inferred_canonical_name": None,
        "inferred_parent_name": None,
        "confidence": 0.0,
        "method": "unfixable",
        "reasoning": reason,
        "candidate_count": candidate_count,
    }


def _embedding_fallback(
    candidates: list[dict[str, Any]],
    min_confidence: float,
    reason_prefix: str,
) -> dict[str, Any]:
    """Use top-1 embedding match with capped confidence."""
    if not candidates:
        return _unfixable_result(f"{reason_prefix} — brak kandydatów")
    top1 = candidates[0]
    sim = float(top1.get("similarity") or 0)
    capped_conf = min(EMBEDDING_FALLBACK_MAX_CONFIDENCE, sim)
    if capped_conf < min_confidence:
        return _unfixable_result(
            f"{reason_prefix} — top-1 embedding sim={sim:.3f} poniżej progu",
            candidate_count=len(candidates),
        )
    return {
        "inferred_tid": int(top1["tid"]),
        "inferred_canonical_name": top1["canonical_name"],
        "inferred_parent_name": top1.get("parent_canonical_name"),
        "confidence": capped_conf,
        "method": "embedding",
        "reasoning": f"{reason_prefix} — użyto top-1 embedding (sim={sim:.3f})",
        "candidate_count": len(candidates),
    }


async def infer_hidden_service_taxonomy(
    service: dict[str, Any],
    supabase: SupabaseService,
    llm: MiniMaxClient | None,
    *,
    top_k: int = DEFAULT_TOP_K,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> dict[str, Any]:
    """Infer Booksy taxonomy tid for a hidden (no-tid) service.

    Returns dict with: inferred_tid, inferred_canonical_name,
    inferred_parent_name, confidence, method, reasoning, candidate_count.
    """
    name = (service.get("name") or "").strip()
    description = service.get("description") or ""
    embedding = service.get("name_embedding")

    if not name or len(name) < 3:
        return _unfixable_result("Nazwa zbyt krótka (<3 chars)")

    candidates = await match_taxonomy_candidates(supabase, embedding, top_k=top_k)
    if not candidates:
        return _unfixable_result("Brak embedding albo RPC failure")

    # Bez opisu LLM nie da rady — fall back to top-1 embedding.
    if not description or len(description.strip()) < 30:
        return _embedding_fallback(
            candidates, min_confidence, "Brak opisu wystarczającej długości"
        )

    # Bez LLM client'u → embedding fallback.
    if llm is None:
        return _embedding_fallback(
            candidates, min_confidence, "LLM client unavailable"
        )

    # Główna ścieżka: LLM disambiguation.
    user_prompt = _build_user_prompt(name, description, candidates)
    try:
        async def _llm_call() -> dict[str, Any]:
            return await llm.generate_json(
                user_prompt, system=_SYSTEM_PROMPT, max_tokens=512
            )
        result = await with_retry(_llm_call, max_attempts=2, base_delay=1.0)
    except Exception as e:
        logger.warning("LLM disambiguation failed for %r: %s", name, e)
        return _embedding_fallback(
            candidates, min_confidence, f"LLM padł ({type(e).__name__})"
        )

    tid_raw = result.get("tid")
    confidence_raw = result.get("confidence") or 0.0
    reasoning = (result.get("reasoning") or "").strip()

    if tid_raw is None:
        return _unfixable_result(
            f"LLM uznał, że żaden candidate nie pasuje: {reasoning or 'no match'}",
            candidate_count=len(candidates),
        )

    try:
        tid = int(tid_raw)
        confidence = float(confidence_raw)
    except (ValueError, TypeError):
        return _unfixable_result(
            f"LLM zwrócił niepoprawne tid/confidence: tid={tid_raw}, conf={confidence_raw}",
            candidate_count=len(candidates),
        )

    if confidence < min_confidence:
        return _unfixable_result(
            f"LLM confidence {confidence:.2f} < {min_confidence}: {reasoning}",
            candidate_count=len(candidates),
        )

    match = next((c for c in candidates if c["tid"] == tid), None)
    if not match:
        # LLM hallucinated a tid outside candidate list — guard.
        logger.warning(
            "LLM hallucinated tid=%s outside candidates for %r", tid, name
        )
        return _embedding_fallback(
            candidates, min_confidence,
            f"LLM zwrócił tid={tid} poza listą kandydatów",
        )

    return {
        "inferred_tid": tid,
        "inferred_canonical_name": match["canonical_name"],
        "inferred_parent_name": match.get("parent_canonical_name"),
        "confidence": confidence,
        "method": "llm",
        "reasoning": reasoning,
        "candidate_count": len(candidates),
    }


async def infer_hidden_services_batch(
    services: list[dict[str, Any]],
    supabase: SupabaseService,
    llm: MiniMaxClient | None,
    *,
    concurrency: int = 4,
    top_k: int = DEFAULT_TOP_K,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[dict[str, Any]]:
    """Run infer_hidden_service_taxonomy for many services in parallel.

    Returns list of results (same order as input). Each service in `services`
    must have `name`, `description`, `name_embedding` keys.
    """
    if not services:
        return []
    sem = asyncio.Semaphore(concurrency)

    async def _one(svc: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            return await infer_hidden_service_taxonomy(
                svc, supabase, llm,
                top_k=top_k, min_confidence=min_confidence,
            )

    return await asyncio.gather(*(_one(s) for s in services))
