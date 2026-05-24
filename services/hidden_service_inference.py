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
import json
import logging
import os
import re
from typing import Any

from services.minimax import with_retry  # exception-class-aware retry helper
from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# Threshold poniżej którego nie ufamy wynikowi (oznaczamy jako unfixable).
# Empirycznie dobrane: LLM zwraca 0.7-0.95 dla pewnych dopasowań, 0.4-0.6
# dla "może to ale nie jestem pewien". 0.65 to próg "trust it".
DEFAULT_MIN_CONFIDENCE = 0.65

# Default semaphore size dla `infer_hidden_services_batch`. Override przez
# HIDDEN_SERVICES_CONCURRENCY env var. Podniesione z 4 → 15 na podstawie
# diagnozy 2026-05-25 (Beauty4ever audit 34, Task 3 plan 2026-05-24):
# - 137 LLM calls/run, identyczny prompt → bit-identyczne token counts
#   między runami, ZERO retries
# - wall-clock varies 47s → 192s (4× spread) — pure OpenAI gpt-4o-mini
#   API latency variance (avg call latency 1.4s → 5.6s w extremum)
# - 137 calls / Semaphore(4) ≈ 34 batches × per-call latency = wall-clock
# - gpt-4o-mini ma rate limit 10K+ RPM (Tier 2+), Semaphore(15) bezpieczne
# Cel: cut hidden_services.enrich z 86s baseline → ≤45s na bad API day.
DEFAULT_HIDDEN_SERVICES_CONCURRENCY = int(
    os.environ.get("HIDDEN_SERVICES_CONCURRENCY", "15")
)

# Embedding-only fallback (gdy brak opisu albo LLM padł) ma celowo
# capped confidence — pure embedding pokazał się słaby w POC (top-1 często
# źle dla brand-only services).
EMBEDDING_FALLBACK_MAX_CONFIDENCE = 0.5

# Top-K candidates z embedding RPC. 30 jest sweet spot:
# - Mniej (np. 10) → poprawna kategoria czasem poza shortlist
#   (POC: Thunder → "Depilacja ciała" na pozycji 22)
# - Więcej (np. 50) → token bloat dla LLM bez gain'u
DEFAULT_TOP_K = 30


class GeminiLLMClient:
    """Thin async wrapper. Mimo nazwy: domyślnie używa OpenAI gpt-4o-mini
    (a Gemini Flash via OpenAI-compat endpoint jako opcja gdy będziemy mieli
    paid Gemini key). Oba używają OpenAI SDK + response_format=json_object
    co eliminuje markdown wrapping + thinking blocks.

    Koszt OpenAI gpt-4o-mini ($0.15/$0.60 per 1M tok in/out) ~ $0.0002 per
    hidden service. Dla 79.90 PLN audytu z 7.4 avg hidden services = $0.0015
    per audyt — trywialnie.
    """

    DEFAULT_PROVIDER = "openai"  # 'openai' | 'gemini'

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        provider: str | None = None,
    ) -> None:
        from openai import AsyncOpenAI
        prov = (provider or self.DEFAULT_PROVIDER).lower()
        if prov == "gemini":
            base_url = "https://generativelanguage.googleapis.com/v1beta/openai/"
        else:
            base_url = None  # OpenAI default
        self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.provider = prov
        # Cumulative usage counters for observability (mig 121). Updated
        # after every successful generate_json call. Read by upstream
        # callers that hold a TraceWriter so per-pipeline LLM cost is
        # queryable without piping a tracer through every function call.
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0
        self.total_calls: int = 0

    async def generate_json(self, prompt: str, system: str, max_tokens: int = 512) -> dict[str, Any]:
        resp = await self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=max_tokens,
        )
        # Accumulate token usage for trace persistence (mig 121).
        usage_obj = getattr(resp, "usage", None)
        if usage_obj is not None:
            self.total_input_tokens += int(getattr(usage_obj, "prompt_tokens", 0) or 0)
            self.total_output_tokens += int(getattr(usage_obj, "completion_tokens", 0) or 0)
        self.total_calls += 1
        content = (resp.choices[0].message.content or "").strip()
        if not content:
            raise ValueError(f"{self.provider} returned empty content")
        # Defensive: strip markdown fence if model adds it despite response_format.
        if content.startswith("```"):
            m = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
            if m:
                content = m.group(1).strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON from {self.provider}: {content[:200]}"
            ) from e


_SYSTEM_PROMPT = """Jesteś ekspertem od taksonomii Booksy.pl. Kategoryzujesz usługi beauty/wellness.

Dostaniesz nazwę + opis usługi (i opcjonalnie kategorię nadaną przez salon w swoim cenniku) oraz listę kandydujących kategorii Booksy. Wybierz JEDNĄ z listy LUB zwróć tid=null gdy żadna nie pasuje rzeczywiście do procedury.

Reguły:
- depilacja laserowa (Thunder/Soprano/Diode/Alex/laser do włosów) → parent="Depilacja"
- IPL/fotoodmładzanie laserem → "Fotoodmładzanie" lub "Zabieg laserowy"
- modelowanie sylwetki/cellulit/kawitacja/EMS → "Zabiegi na ciało i modelowanie sylwetki"
- KROPLÓWKI / IV drips / wlewy dożylne / mezoterapia IV / NAD+ infuzje → tid=null (NIE Tlenoterapia, NIE Medycyna sportowa, NIE Fizjoterapia — to są CAŁKIEM RÓŻNE procedury, nawet jeśli embedding je do siebie zbliża)
- iniekcje pojedynczych substancji domięśniowo / dożylnie / podskórnie (witaminy, glutation, NAD+, kwas) bez wyraźnego kontekstu zabiegowego → tid=null
- żadna kategoria z listy NAPRAWDĘ nie pasuje (nie tylko "blisko") → tid=null
- NIE wymyślaj tid spoza listy
- KIEDY salon przypisał własną kategorię (np. "Kroplówki", "Stymulatory tkankowe") a ŻADNA z kategorii Booksy z listy nie jest tym samym co salonowa kategoria → tid=null. Zaufaj kategoryzacji salonu jako mocnemu sygnałowi że Booksy nie ma odpowiednika.
- Confidence: 0.9+ tylko gdy procedura JEDNOZNACZNIE pasuje. 0.7-0.85 dla rozsądnego matcha. Poniżej 0.7 lepiej zwrócić tid=null niż zgadywać.

WAŻNE: odpowiadaj WYŁĄCZNIE pojedynczym JSON-em, bez markdown, bez ```, bez komentarzy, bez tekstu przed/po. Format dokładnie:
{"tid":<int|null>,"confidence":<float 0-1>,"reasoning":"<1 krótkie zdanie po polsku>"}"""


def _build_user_prompt(
    service_name: str,
    service_description: str | None,
    candidates: list[dict[str, Any]],
    salon_category: str | None = None,
) -> str:
    desc = (service_description or "").strip()[:500]
    cand_lines: list[str] = []
    for c in candidates:
        parent = c.get("parent_canonical_name")
        parent_str = f" — parent: {parent}" if parent else ""
        sim = c.get("similarity")
        sim_str = f" (similarity: {sim:.3f})" if isinstance(sim, (int, float)) else ""
        cand_lines.append(
            f"  tid={c['tid']}: {c['canonical_name']}{parent_str}{sim_str}"
        )
    cand_block = "\n".join(cand_lines)
    salon_cat_line = ""
    if salon_category:
        salon_cat_line = (
            f"\n- Kategoria nadana przez salon w cenniku: "
            f"{salon_category} (mocna wskazówka — jeśli żadna kategoria "
            f"Booksy z listy nie odpowiada DOKŁADNIE temu co salon "
            f"nazwał, zwróć tid=null)"
        )
    return f"""Usługa do skategoryzowania:
- Nazwa: {service_name}
- Opis: {desc or "(brak opisu)"}{salon_cat_line}

Lista kategorii Booksy (top dopasowania embedding, posortowane malejąco po podobieństwie):
{cand_block}

Wybierz JEDNĄ kategorię która naprawdę oddaje procedurę. Jeśli wszystkie kandydatury są "blisko ale nie to" — np. salon ma Kroplówki a lista ma tylko Tlenoterapię i Medycynę sportową — zwróć tid=null. Lepsze null niż błędne dopasowanie."""


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
    llm: GeminiLLMClient | None,
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

    # Deterministic body-area gate (Stage-5 commit 1, 2026-05-17).
    # Filters ANN top-K candidates by area-set overlap with service name
    # before the LLM ever sees them. Eliminates the "Thunder Całe ciało
    # → Depilacja twarzy 0.9 confidence" failure mode at the source:
    # LLM cannot pick what isn't in the candidate list. Candidates with
    # NO area markers (generic "Depilacja laserowa") pass through.
    from services.body_area_taxonomy import (
        filter_candidates_by_area,
        extract_body_areas,
        areas_compatible,
    )
    kept, svc_areas, dropped_by_area = filter_candidates_by_area(name, candidates)
    if dropped_by_area:
        logger.info(
            "infer_hidden_service_taxonomy: area-gate filtered name=%r "
            "svc_areas=%s kept=%d dropped=%d (dropped_tids=%s)",
            name[:60], sorted(svc_areas), len(kept), len(dropped_by_area),
            [d.get("tid") for d in dropped_by_area[:5]],
        )
    if not kept:
        # Every candidate had an incompatible area set. Service is a real
        # mismatch with Booksy taxonomy — falls through to Rule 2/4.
        return _unfixable_result(
            f"Wszyscy kandydaci ANN top-{top_k} mają niezgodną okolicę "
            f"ciała vs serwis (svc_areas={sorted(svc_areas)}). "
            f"Service nie ma odpowiednika w Booksy — Rule 2 fires.",
            candidate_count=len(candidates),
        )
    candidates = kept

    # Hard embedding similarity floor — gdy NAJBLIŻSZY kandydat z Booksy
    # taxonomy ma cosine < 0.55 to service po prostu nie ma odpowiednika
    # w taksonomii. LLM disambiguation tutaj produkuje zaufanie 0.8-0.9
    # dla bzdurnych mapowań ("Kroplówka NAD VIP" → "Tlenoterapia") bo
    # wybiera top-1 z listy bez warunku faktycznego dopasowania.
    # Próg 0.55 zwalidowany empirycznie 2026-05-16 (Beauty4ever Kroplówki
    # mają similarity ~0.40-0.50 do najbliższego Booksy tid'a).
    top_sim = float(candidates[0].get("similarity") or 0.0)
    if top_sim < 0.55:
        logger.info(
            "infer_hidden_service_taxonomy: name=%r top embedding sim=%.3f "
            "< 0.55 floor — bypassing LLM, returning tid=null",
            name[:60], top_sim,
        )
        return _unfixable_result(
            f"Top embedding sim {top_sim:.3f} poniżej 0.55 floor — "
            f"brak realistycznego matcha w Booksy taxonomy",
            candidate_count=len(candidates),
        )

    # Bez opisu LLM nie da rady — fall back to top-1 embedding.
    if not description or len(description.strip()) < 30:
        return _embedding_fallback(
            candidates, min_confidence, "Brak opisu wystarczającej długości"
        )

    # Bez LLM client'u → hard fail. Embedding fallback ukrywało brak klucza
    # i potem cały etap LLM disambiguation produkował confidence z embedding
    # similarity zamiast z LLM reasoning — niewidoczna degradacja jakości.
    if llm is None:
        raise RuntimeError(
            "infer_hidden_service_taxonomy: llm client is None — provider "
            "init failed earlier (OPENAI/GEMINI). Fix the key config; do "
            "not run inference with silent embedding fallback."
        )

    # Główna ścieżka: LLM disambiguation. Przekazujemy salon's category_name
    # jeśli jest — silna wskazówka dla LLM żeby zwrócić tid=null gdy
    # salon ma własną kategorię która nie odpowiada żadnemu kandydatowi.
    salon_category = (service.get("category_name") or "").strip() or None
    user_prompt = _build_user_prompt(
        name, description, candidates, salon_category=salon_category,
    )
    # LLM call: hard-fail on persistent error after retry. Previous
    # embedding fallback hid Gemini 403/quota errors as low-confidence
    # embedding picks, polluting taxonomy without alerting anyone.
    async def _llm_call() -> dict[str, Any]:
        return await llm.generate_json(
            user_prompt, system=_SYSTEM_PROMPT, max_tokens=512
        )
    result = await with_retry(_llm_call, max_attempts=2, base_delay=1.0)

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

    # Post-LLM area gate. Even after pre-filtering candidates, the LLM
    # can still pick a candidate whose canonical_name has narrower or
    # disjoint area set if the service has multi-area mentions
    # (e.g. "Mała partia (bikini, pachy, twarz)" picks "Depilacja
    # twarzy" — half-right but not the holistic answer). Reject if
    # canonical_name area is disjoint from service area.
    cand_areas = extract_body_areas(match.get("canonical_name") or "")
    if not areas_compatible(svc_areas, cand_areas):
        logger.info(
            "infer_hidden_service_taxonomy: POST-LLM area-gate rejected "
            "name=%r → tid=%s canonical=%r (svc=%s vs cand=%s)",
            name[:60], tid, match.get("canonical_name"),
            sorted(svc_areas), sorted(cand_areas),
        )
        return _unfixable_result(
            f"LLM wybrał tid={tid} '{match.get('canonical_name')}' z area "
            f"{sorted(cand_areas)} ale serwis ma area {sorted(svc_areas)} "
            f"— rozjazd okolicy ciała, Rule 2 fires.",
            candidate_count=len(candidates),
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


_OAI_EMBED_CLIENT = None


def _get_openai_embed_client():
    """Lazy-cached OpenAI client for embedding generation in the synthetic-
    category path. Distinct from `taxonomy_inference._OAI_CLIENT` so we can
    raise on failure (taxonomy_inference's helper swallows errors to fall
    back to trigram).
    """
    global _OAI_EMBED_CLIENT
    if _OAI_EMBED_CLIENT is None:
        from openai import OpenAI
        _OAI_EMBED_CLIENT = OpenAI()
    return _OAI_EMBED_CLIENT


async def embed_short_text(text: str) -> list[float]:
    """Generate a single OpenAI text-embedding-3-small vector for `text`.

    Used by Rules 2/1 of `_resolve_service_taxonomy` when we need an
    embedding for a freshly created synthetic category (the salon's
    cennik category name or the LLM short-generated phrase).

    Raises on OpenAI failure — directive 2026-05-16: no graceful
    fallback in the synthetic-category path. Bugsink captures.
    """
    if not text or not text.strip():
        raise ValueError("embed_short_text: empty text")
    snippet = text.strip()[:512]
    oai = _get_openai_embed_client()

    def _do_call():
        return oai.embeddings.create(
            model="text-embedding-3-small",
            input=[snippet],
        )

    resp = await asyncio.to_thread(_do_call)
    data = list(resp.data or [])
    if not data:
        raise RuntimeError(
            f"embed_short_text: OpenAI returned no data for {text!r}"
        )
    emb = list(data[0].embedding or [])
    if not emb:
        raise RuntimeError(
            f"embed_short_text: OpenAI returned empty vector for {text!r}"
        )
    logger.debug(
        "embed_short_text: embedded %r (len=%d, dim=%d)",
        snippet[:40], len(snippet), len(emb),
    )
    return emb


_SHORT_CATEGORY_SYSTEM_PROMPT = """Jesteś ekspertem od taksonomii beauty/wellness.

Dostajesz nazwę usługi salonu kosmetycznego i tworzysz dla niej krótką (1-2 słowa)
kategorię, pod którą inny salon szukałby w cenniku „takich samych usług".

Reguły:
- Wynik 1-2 słowa po polsku, jak gotowy nagłówek kategorii cennika
- Pierwsze słowo wielka litera, reszta jak naturalna nazwa kategorii
- Skup się na PROCEDURZE, nie na brand-name (Thunder → Depilacja, EMBODY → Modelowanie)
- Jeśli usługa to konsultacja / pakiet / voucher: użyj „Konsultacja" / „Pakiet" / „Voucher"
- Pole `longer_description` to 1-2 zdania opisu używanego do embedding match (po polsku)

WAŻNE: odpowiadaj WYŁĄCZNIE pojedynczym JSON-em, bez markdown, bez ```, bez komentarzy:
{"category":"<1-2 słowa>","longer_description":"<1-2 zdania>"}"""


async def generate_short_category(
    service_name: str,
    price_pln: float | None,
    duration_min: int | None,
    category_name: str | None,
    llm: GeminiLLMClient,
) -> dict[str, Any]:
    """Generate a synthetic 1-2 word category name for an unmatched service
    via LLM short prompt. Used by Rule 1 of `_resolve_service_taxonomy`
    when neither salon-defined category nor Booksy LLM disambiguation
    nor embedding inheritance produced a hit.

    Returns `{"category": "<1-2 słowa>", "longer_description": "<1-2 zdania>"}`.

    Raises on LLM failure or invalid JSON. Directive 2026-05-16: no
    graceful fallback to "Inne" — caller must handle the exception
    (Bugsink captures it) so we surface root causes instead of
    silently poisoning the synthetic catalog with garbage entries.
    """
    if not service_name or len(service_name.strip()) < 2:
        raise ValueError(
            f"generate_short_category: service_name too short ({service_name!r})"
        )
    if llm is None:
        raise RuntimeError(
            "generate_short_category: no LLM client provided"
        )

    parts: list[str] = [f"Nazwa usługi: {service_name.strip()}"]
    if category_name:
        parts.append(f"Kategoria w cenniku salonu: {category_name.strip()}")
    if price_pln is not None:
        parts.append(f"Cena: {price_pln:.2f} zł")
    if duration_min is not None:
        parts.append(f"Czas trwania: {duration_min} min")
    user_prompt = (
        "\n".join(parts)
        + "\n\nWygeneruj JSON z polami `category` (1-2 słowa) "
        "i `longer_description` (1-2 zdania)."
    )

    result = await llm.generate_json(
        user_prompt,
        system=_SHORT_CATEGORY_SYSTEM_PROMPT,
        max_tokens=200,
    )
    category = (result.get("category") or "").strip()
    longer = (result.get("longer_description") or "").strip()
    if not category:
        raise ValueError(
            f"generate_short_category: LLM returned empty `category` for "
            f"{service_name!r} (raw={result!r})"
        )
    # Defensive: collapse to 1-3 words max (LLM occasionally returns 4-5
    # words despite the prompt — we accept up to 3 to avoid spurious
    # failures, but trim longer outputs).
    words = category.split()
    if len(words) > 3:
        category = " ".join(words[:3])
    return {
        "category": category,
        "longer_description": longer or category,
    }


async def infer_hidden_services_batch(
    services: list[dict[str, Any]],
    supabase: SupabaseService,
    llm: GeminiLLMClient | None,
    *,
    concurrency: int | None = None,
    top_k: int = DEFAULT_TOP_K,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[dict[str, Any]]:
    """Run infer_hidden_service_taxonomy for many services in parallel.

    Returns list of results (same order as input). Each service in `services`
    must have `name`, `description`, `name_embedding` keys.

    `concurrency=None` (default) uses `DEFAULT_HIDDEN_SERVICES_CONCURRENCY`
    (env var `HIDDEN_SERVICES_CONCURRENCY`, default 15). Caller może
    nadpisać explicit int dla testów (np. test_hidden_service_inference.py
    chce concurrency=1 dla deterministycznych testów).
    """
    if not services:
        return []
    effective_concurrency = (
        concurrency if concurrency is not None
        else DEFAULT_HIDDEN_SERVICES_CONCURRENCY
    )
    sem = asyncio.Semaphore(effective_concurrency)

    async def _one(svc: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            return await infer_hidden_service_taxonomy(
                svc, supabase, llm,
                top_k=top_k, min_confidence=min_confidence,
            )

    return await asyncio.gather(*(_one(s) for s in services))
