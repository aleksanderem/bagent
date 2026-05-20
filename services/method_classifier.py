"""Method classifier — map salon_scrape_services rows to canonical
treatment_methods (mig 095) via cascading alias → ANN → LLM strategy.

DICTIONARY-FIRST PRINCIPLE (2026-05-19 — user mandate):

Every method classification MUST resolve to a row in `treatment_methods`.
The classifier NEVER attaches an ad-hoc tag to a service that isn't
backed by a dictionary entry. When LLM cascade encounters a method
unknown to the dictionary, it MUST first propose the new entry, INSERT
it into `treatment_methods` (with `source='llm_inferred'`), and THEN
return the now-canonical match for downstream code. The mapping in
`service_method_classification` always points to an existing
`treatment_methods.id`.

This makes downstream scripts (pricing aggregation, competitor search,
opportunity detection, narrative synthesis) operate exclusively on the
dictionary — there is no second source of truth.

Cascade order:

  1. **In-process cache** — service_id → list[MethodMatch], O(1) lookup
  2. **DB cache** — service_method_classification rows, point lookup
  3. **Alias substring (MULTI-MATCH)** — finds ALL matching aliases in
     the service name (each match = separate MethodMatch). Sorted
     longest-alias-first so "PRO XN + Dermapen" produces both
     {pro_xn} and {dermapen} matches. Aliases include orthography
     variants (PRO XN / proxn / pro-xn) AND semantic synonyms
     (RF / radiofrekwencja, botoks / toksyna botulinowa). ~60-80%
     coverage.
  4. **Embedding ANN** — when alias matching returns 0 hits, fall
     through to RPC fn_classify_service_by_embedding. +15-20% coverage.
  5. **LLM fallback (DICTIONARY-EXTENDING)** — gpt-4o-mini structured
     JSON. Given service name + top ANN candidates + the constraint
     that output MUST reference dictionary entries, LLM either:
       a) picks one or more existing canonical_names from candidates,
       b) proposes new method(s) with full structure
          (display_name, category, method_type, brand_family,
          description, comprehensive aliases including synonyms in
          Polish + English), which we INSERT into treatment_methods
          before mapping the service to them.

All proposed_new entries go straight to `treatment_methods` with
`source='llm_inferred'`. Admin can later promote individual entries
to `source='curated'` after review.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

try:
    from openai import APIConnectionError, APITimeoutError, RateLimitError, InternalServerError
except Exception:  # pragma: no cover — older openai SDK shape
    class APIConnectionError(Exception): ...  # type: ignore[no-redef]
    class APITimeoutError(Exception): ...  # type: ignore[no-redef]
    class RateLimitError(Exception): ...  # type: ignore[no-redef]
    class InternalServerError(Exception): ...  # type: ignore[no-redef]

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


T = TypeVar("T")


# OpenAI transient-error retry policy (Faza F-r8, 2026-05-20). Workery
# Fazy G padły w nocy na nieobsłużonym APIConnectionError; teraz wrap'ujemy
# każdy LLM/embedding call w `_openai_call_with_retry` z exponential
# backoff + jitter. Failure modes obsłużone: connection drop, timeout,
# 429 rate limit, 5xx server error. Inne błędy (auth, invalid request)
# propagują natychmiast bo retry nic nie zmieni.
_OPENAI_RETRY_EXCEPTIONS: tuple[type[BaseException], ...] = (
    APIConnectionError,
    APITimeoutError,
    RateLimitError,
    InternalServerError,
)
_OPENAI_RETRY_MAX_ATTEMPTS = 5
_OPENAI_RETRY_BASE_DELAY = 2.0  # seconds; multiplied by 2^attempt + jitter


async def _openai_call_with_retry(
    fn: Callable[..., T],
    *args: Any,
    _retry_label: str = "openai",
    **kwargs: Any,
) -> T:
    """Run `fn(*args, **kwargs)` w `asyncio.to_thread` z exponential
    backoff retry dla transient OpenAI errors. Każda próba loguje WARN,
    final failure rzuca ostatni exception nadbieraj.

    Use case: chat.completions.create + embeddings.create w klasyfikatorze.
    Bez tego pojedynczy network blip kładzie cały worker (workery Fazy G
    2026-05-19 padły tak właśnie).
    """
    last_exc: BaseException | None = None
    for attempt in range(_OPENAI_RETRY_MAX_ATTEMPTS):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except _OPENAI_RETRY_EXCEPTIONS as e:
            last_exc = e
            if attempt == _OPENAI_RETRY_MAX_ATTEMPTS - 1:
                logger.error(
                    "[%s] OpenAI transient error after %d attempts — giving up: %s",
                    _retry_label, _OPENAI_RETRY_MAX_ATTEMPTS, e,
                )
                raise
            delay = _OPENAI_RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1.5)
            logger.warning(
                "[%s] OpenAI transient %s on attempt %d/%d — retrying in %.1fs: %s",
                _retry_label, type(e).__name__, attempt + 1,
                _OPENAI_RETRY_MAX_ATTEMPTS, delay, e,
            )
            await asyncio.sleep(delay)
    # Unreachable — final iteration above re-raises, but mypy needs explicit
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"[{_retry_label}] retry loop exited without result")


# Confidence thresholds (tunable per category if needed via overrides)
ALIAS_EXACT_CONFIDENCE = 1.0
ANN_ACCEPT_THRESHOLD = 0.75
ANN_TOP_N = 5
LLM_MIN_CONFIDENCE = 0.7

# Method types allowed in treatment_methods (matches CHECK constraint
# from mig 095). LLM must propose exactly one of these for any new entry.
_ALLOWED_METHOD_TYPES = {"device", "substance", "technique", "protocol"}

# Categories we've seen so far — LLM may suggest new categories if a
# truly novel one (we accept those, no enum constraint at DB layer).
_KNOWN_CATEGORIES_HINT = (
    "laser_depilacja, laser_skin, rf_hifu, mezoterapia, peeling, "
    "substancja, brwi_rzesy, makijaz_permanentny, manicure, masaz, "
    "fryzjer_zabieg, fryzjer_koloryzacja, chirurgia, transplantacja_wlosow, "
    "trychologia, podologia, depilacja_klasyczna, oczyszczanie, inny"
)


@dataclass
class MethodRow:
    """In-memory representation of a treatment_methods row."""
    id: int
    canonical_name: str
    display_name: str
    category: str
    method_type: str
    brand_family: str | None
    aliases: list[str] = field(default_factory=list)
    source: str = "curated"


@dataclass
class MethodMatch:
    """One classified method for a service. `classifier` records which
    cascade step produced this, for provenance + debug. A service may
    have multiple MethodMatch results (e.g. 'PRO XN + Dermapen' →
    [pro_xn, dermapen])."""
    method_id: int
    canonical_name: str
    display_name: str
    category: str
    method_type: str
    brand_family: str | None
    confidence: float
    classifier: str  # 'cached' | 'alias_exact' | 'embedding_ann' | 'llm' | 'llm_new'
    matched_alias: str | None = None


def _ascii_fold(s: str) -> str:
    """Polish + diacritics → ASCII lowercase. Idempotent."""
    s = unicodedata.normalize("NFKD", s or "")
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower()


def _normalize_for_match(s: str) -> str:
    """Squash whitespace + folded ASCII for substring search."""
    folded = _ascii_fold(s)
    folded = re.sub(r"[^a-z0-9]+", " ", folded)
    folded = re.sub(r"\s+", " ", folded).strip()
    return folded


def _slug(name: str) -> str:
    """Snake_case ASCII identifier — used when LLM proposes new
    treatment_methods rows. Stable + deterministic."""
    s = _ascii_fold(name)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "unknown"


class MethodClassifier:
    """Cached cascading classifier. Instantiate once per pipeline run;
    warmup() loads all treatment_methods + alias index into memory.

    NEW in dictionary-first refactor:
      - classify_service returns list[MethodMatch] (multi-match)
      - LLM proposes_new entries are INSERT'ed into treatment_methods
        before the classifier returns the resulting MethodMatch
    """

    def __init__(
        self,
        supabase: SupabaseService,
        llm_client: Any | None = None,
        openai_client_for_embeddings: Any | None = None,
    ):
        self.supabase = supabase
        self.llm_client = llm_client
        # Separate handle for embedding generation when inserting new
        # dictionary entries. Default to the llm_client (OpenAI) which
        # has the embeddings endpoint. Can be overridden if the calling
        # pipeline uses a different LLM provider for chat.
        self.openai_client = openai_client_for_embeddings or llm_client

        self._methods: dict[int, MethodRow] = {}
        self._canonical_to_id: dict[str, int] = {}
        # Sorted longest-first to bias toward more specific matches
        self._alias_index: list[tuple[str, int]] = []

        self._pending_writes: list[dict[str, Any]] = []
        # In-process cache: service_id → list[MethodMatch]
        self._inprocess_cache: dict[int, list[MethodMatch]] = {}

    # ── Loading / warmup ─────────────────────────────────────────────

    async def warmup(self) -> None:
        """Load all treatment_methods + build alias substring index."""
        try:
            res = (
                self.supabase.client.table("treatment_methods")
                .select(
                    "id, canonical_name, display_name, category, method_type, "
                    "brand_family, aliases, source"
                )
                .execute()
            )
        except Exception as e:
            logger.error("warmup: failed to load treatment_methods: %s", e)
            raise

        rows = res.data or []
        self._methods = {}
        self._canonical_to_id = {}
        self._alias_index = []
        for r in rows:
            self._register_method_row(r)

        self._alias_index.sort(key=lambda t: -len(t[0]))
        logger.info(
            "MethodClassifier warmup: %d methods, %d alias entries",
            len(self._methods),
            len(self._alias_index),
        )

    def _register_method_row(self, r: dict) -> int:
        """Add a row to the in-memory index. Used both during warmup and
        after a fresh LLM-proposed INSERT lands in treatment_methods."""
        mid = int(r["id"])
        row = MethodRow(
            id=mid,
            canonical_name=r["canonical_name"],
            display_name=r["display_name"],
            category=r["category"],
            method_type=r["method_type"],
            brand_family=r.get("brand_family"),
            aliases=r.get("aliases") or [],
            source=r.get("source") or "curated",
        )
        self._methods[mid] = row
        self._canonical_to_id[row.canonical_name] = mid
        for alias in row.aliases:
            normalized = _normalize_for_match(alias)
            if len(normalized) >= 2:
                self._alias_index.append((normalized, mid))
        return mid

    # ── Cache (DB-backed + in-process) ───────────────────────────────

    async def _load_cache(self, service_id: int) -> list[MethodMatch] | None:
        """Return cached classifications for a service (list, possibly
        empty meaning 'classified but found no matches'). Returns None
        when there's no cache entry yet.

        FAIL-LOUD: DB errors are NOT swallowed — they bubble up to the
        caller. Cache miss (no rows) is a legitimate None return."""
        cached = self._inprocess_cache.get(service_id)
        if cached is not None:
            return cached

        res = (
            self.supabase.client.table("service_method_classification")
            .select("method_id, confidence, classifier, matched_alias")
            .eq("service_id", service_id)
            .order("confidence", desc=True)
            .execute()
        )

        rows = res.data or []
        if not rows:
            return None
        out: list[MethodMatch] = []
        for row in rows:
            method_id = int(row["method_id"])
            method = self._methods.get(method_id)
            if method is None:
                continue  # stale cache pointing at deleted method
            out.append(MethodMatch(
                method_id=method_id,
                canonical_name=method.canonical_name,
                display_name=method.display_name,
                category=method.category,
                method_type=method.method_type,
                brand_family=method.brand_family,
                confidence=float(row["confidence"]),
                classifier="cached",
                matched_alias=row.get("matched_alias"),
            ))
        self._inprocess_cache[service_id] = out
        return out

    def _queue_cache_write(
        self,
        service_id: int,
        matches: list[MethodMatch],
        llm_response: dict | None = None,
    ) -> None:
        """Buffer rows for batch UPSERT.

        Dedup by method_id (multi-match may return same method via two
        paths e.g. alias_exact + llm_new — keep highest confidence). The
        cache primary key is (service_id, method_id) so duplicates would
        crash bulk upsert with 'ON CONFLICT DO UPDATE cannot affect row
        a second time'."""
        # Per-service method dedup, keep highest-confidence MethodMatch
        by_method: dict[int, MethodMatch] = {}
        for m in matches:
            existing = by_method.get(m.method_id)
            if existing is None or m.confidence > existing.confidence:
                by_method[m.method_id] = m
        deduped = list(by_method.values())
        self._inprocess_cache[service_id] = deduped
        for m in deduped:
            self._pending_writes.append({
                "service_id": service_id,
                "method_id": m.method_id,
                "confidence": float(m.confidence),
                "classifier": m.classifier,
                "matched_alias": m.matched_alias,
                "llm_response": llm_response if m.classifier in ("llm", "llm_new") else None,
            })

    async def flush_cache_writes(self) -> int:
        """Flush buffered classifications. Final dedup pass over the
        buffer (in case two batches independently classified the same
        service before the previous flush). Chunks 500 rows per UPSERT.

        FAIL-LOUD: any chunk failure re-raises with full context."""
        if not self._pending_writes:
            return 0
        # Final dedup — collapse duplicate (service_id, method_id)
        # keys keeping highest-confidence row.
        by_key: dict[tuple[int, int], dict] = {}
        for row in self._pending_writes:
            key = (int(row["service_id"]), int(row["method_id"]))
            existing = by_key.get(key)
            if existing is None or float(row["confidence"]) > float(existing["confidence"]):
                by_key[key] = row
        deduped = list(by_key.values())
        if len(deduped) != len(self._pending_writes):
            logger.info(
                "flush_cache_writes: deduped %d → %d rows",
                len(self._pending_writes), len(deduped),
            )

        chunk_size = 500
        total = 0
        for i in range(0, len(deduped), chunk_size):
            chunk = deduped[i : i + chunk_size]
            try:
                self.supabase.client.table("service_method_classification") \
                    .upsert(chunk, on_conflict="service_id,method_id") \
                    .execute()
                total += len(chunk)
            except Exception as e:
                # Log first 3 conflict candidates for debugging
                key_counts: dict[tuple, int] = {}
                for r in chunk:
                    k = (int(r["service_id"]), int(r["method_id"]))
                    key_counts[k] = key_counts.get(k, 0) + 1
                conflict_keys = [k for k, c in key_counts.items() if c > 1][:3]
                logger.error(
                    "cache flush batch %d-%d failed (chunk size=%d, "
                    "duplicate keys in chunk: %s): %s",
                    i, i + len(chunk), len(chunk), conflict_keys, e,
                )
                raise
        logger.info("MethodClassifier flushed %d cache rows", total)
        self._pending_writes.clear()
        return total

    # ── Alias-exact MULTI-match ──────────────────────────────────────

    def _classify_by_alias_multi(
        self, service_name: str
    ) -> list[MethodMatch]:
        """Find ALL alias matches in the service name. Each match is a
        separate MethodMatch. Handles combo services like
        'PRO XN + Dermapen' → [pro_xn, dermapen]."""
        normalized_target = _normalize_for_match(service_name)
        if not normalized_target:
            return []

        out: list[MethodMatch] = []
        # Track which method ids we've already added — same method may
        # match via multiple aliases (e.g. 'rf' and 'radiofrekwencja'),
        # but per-service we only want one MethodMatch per method.
        seen_method_ids: set[int] = set()
        for alias, method_id in self._alias_index:
            if method_id in seen_method_ids:
                continue
            if self._is_token_substring(alias, normalized_target):
                method = self._methods[method_id]
                out.append(MethodMatch(
                    method_id=method_id,
                    canonical_name=method.canonical_name,
                    display_name=method.display_name,
                    category=method.category,
                    method_type=method.method_type,
                    brand_family=method.brand_family,
                    confidence=ALIAS_EXACT_CONFIDENCE,
                    classifier="alias_exact",
                    matched_alias=alias,
                ))
                seen_method_ids.add(method_id)
        return out

    @staticmethod
    def _is_token_substring(needle: str, haystack: str) -> bool:
        if not needle:
            return False
        if " " in needle:
            return f" {needle} " in f" {haystack} "
        return f" {needle} " in f" {haystack} "

    # ── Embedding ANN classifier ─────────────────────────────────────

    async def _classify_by_embedding(
        self,
        name_embedding: Any,
        category_hint: str | None = None,
    ) -> tuple[MethodMatch, list[dict]] | None:
        """ANN match via RPC. Returns (top_match, all_candidates) tuple,
        or None when service has no embedding / RPC returns empty.

        FAIL-LOUD: RPC errors are re-raised. Missing embedding is the
        only None-return path."""
        if name_embedding is None:
            return None
        res = self.supabase.client.rpc(
            "fn_classify_service_by_embedding",
            {
                "p_service_embedding": name_embedding,
                "p_category_hint": category_hint,
                "p_top_n": ANN_TOP_N,
                "p_min_similarity": 0.55,
            },
        ).execute()

        candidates = res.data or []
        if not candidates:
            return None

        top = candidates[0]
        top_sim = float(top["similarity"])
        method_id = int(top["method_id"])
        method = self._methods.get(method_id)
        if method is None:
            return None

        match = MethodMatch(
            method_id=method_id,
            canonical_name=method.canonical_name,
            display_name=method.display_name,
            category=method.category,
            method_type=method.method_type,
            brand_family=method.brand_family,
            confidence=top_sim,
            classifier="embedding_ann",
        )
        return (match, candidates)

    # ── LLM fallback with DICTIONARY-EXTEND ──────────────────────────

    async def _classify_by_llm(
        self,
        service_name: str,
        category_hint: str | None,
        candidates: list[dict],
    ) -> list[MethodMatch]:
        """Ask gpt-4o-mini structured-output to identify ALL canonical
        methods present in the service name. LLM either picks existing
        canonical_names from the candidates list OR proposes NEW entries
        which we INSERT into treatment_methods before returning matches.

        Returns list of MethodMatch (may be empty if LLM rejected all
        candidates and proposed nothing — service is genuinely unmatched)."""
        if not self.llm_client:
            return []

        candidate_lines = [
            f"- canonical_name='{c['canonical_name']}' "
            f"display='{c['display_name']}' "
            f"category={c['category']} "
            f"brand_family={c.get('brand_family') or 'n/a'} "
            f"cos_sim={float(c['similarity']):.2f}"
            for c in candidates
        ]
        candidates_block = "\n".join(candidate_lines) if candidates else "(none — embedding match returned nothing)"

        system = (
            "Jesteś ekspertem od taksonomii zabiegów beauty / medycyny "
            "estetycznej / fryzjerstwa / kosmetologii w Polsce. Twoim "
            "zadaniem jest zidentyfikować WSZYSTKIE rozpoznawalne metody / "
            "urządzenia / substancje / techniki / protokoły obecne w nazwie "
            "konkretnej usługi salonu. Jedna usługa może odnosić się do "
            "wielu metod jednocześnie (np. 'PRO XN + Dermapen' to peeling "
            "PRO XN ORAZ urządzenie Dermapen).\n\n"
            "BARDZO WAŻNE — KONSERWATYZM:\n"
            "1. ZAWSZE preferuj match do `existing` candidates jeśli concept jest "
            "tym samym co istniejący wpis. Np. 'Toksyna botulinowa' = `botox` "
            "(istnieje); NIE proponuj 'toksyna_botulinowa' jako nowy entry. "
            "'RF' = pasujący RF device z candidates (Virtue RF, Infini etc.) "
            "ALBO generic technique już istniejący; NIE twórz duplikatu.\n"
            "2. NIE proponuj wpisów które są: lokalizacjami ciała (twarz/szyja/"
            "plecy), objawami (przebarwienia, trądzik, naczynka, zmarszczki), "
            "rezultatami (nawilżenie, rozjaśnienie, wygładzenie), generic "
            "wordami (zabieg, pakiet, promocja, konsultacja), modyfikatorami "
            "ceny ('jedna okolica', 'I stopień', 'small/medium').\n"
            "3. NOWY wpis tylko gdy: KONKRETNA marka/produkt (np. 'Geneo' "
            "urządzenie, 'Hyalual' wypełniacz) LUB konkretna technika "
            "(np. 'mikropigmentacja', 'kawitacja') która faktycznie ma "
            "specyficzne właściwości metodyczne, NIE jest pokryta przez "
            "candidates, i NIE jest abstract'em.\n"
            "4. Jeśli usługa to zwykły akt obsługi (konsultacja, "
            "voucher, instruktaż, demonstracja) — zwróć puste listy.\n\n"
            "Kategorie używane w słowniku: " + _KNOWN_CATEGORIES_HINT + ".\n"
            "method_type ∈ {device, substance, technique, protocol}.\n\n"
            "Dla proposed_new generuj WYCZERPUJĄCĄ listę aliases — wszystkie "
            "warianty pisowni + synonimy PL+EN + skróty. Aliasy powinny "
            "być UNIKALNE dla tej metody (nie współdzielone z innymi "
            "wpisami w słowniku).\n\n"
            "Zwracaj striczny JSON."
        )
        user = (
            f"Nazwa usługi: {service_name!r}\n"
            f"Wskazówka kategorii: {category_hint or 'unknown'}\n\n"
            f"Top kandydaci ze słownika (po embedding similarity):\n{candidates_block}\n\n"
            "Zwróć JSON:\n"
            "{\n"
            "  \"matches\": [\n"
            "    {\"canonical_name\": \"<istniejące canonical>\", \"confidence\": 0.0-1.0, \"reasoning\": \"<krótkie\"}\n"
            "  ],\n"
            "  \"proposed_new\": [\n"
            "    {\n"
            "      \"display_name\": \"<UI label, np. 'Lonotin'>\",\n"
            "      \"category\": \"<jedna z istniejących lub nowa>\",\n"
            "      \"method_type\": \"device|substance|technique|protocol\",\n"
            "      \"brand_family\": \"<lowercase slug producenta lub null>\",\n"
            "      \"description\": \"<jedno-zdaniowy opis PL>\",\n"
            "      \"aliases\": [\"alias1\", \"alias2\", ...],\n"
            "      \"confidence\": 0.0-1.0\n"
            "    }\n"
            "  ]\n"
            "}\n"
            "Jeśli usługa to combo (np. 'PRO XN + Dermapen + laktoferyna') i niektóre części są w słowniku "
            "a inne nie — zwróć JEDEN match per część w słowniku, JEDEN proposed_new per część nowa.\n"
            "Jeśli usługa NIE ma sensownej metody (np. 'konsultacja kosmetologa', 'voucher prezentowy') "
            "zwróć puste obie listy."
        )

        # Transient OpenAI errors (connection drop, timeout, 429, 5xx)
        # retried z exponential backoff w `_openai_call_with_retry`.
        # Inne błędy (auth, invalid request, JSON parse) propagują —
        # caller (backfill, pipeline) decyduje skip vs abort.
        resp = await _openai_call_with_retry(
            self.llm_client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0,
            _retry_label="classify_by_llm",
        )
        content = resp.choices[0].message.content or "{}"
        payload = json.loads(content)

        out: list[MethodMatch] = []

        # Existing matches
        for m in (payload.get("matches") or []):
            canonical = (m.get("canonical_name") or "").strip()
            if not canonical:
                continue
            method_id = self._canonical_to_id.get(canonical)
            if method_id is None:
                logger.warning(
                    "LLM returned canonical=%r not in dictionary — skipping",
                    canonical,
                )
                continue
            confidence = float(m.get("confidence", 0.0))
            if confidence < LLM_MIN_CONFIDENCE:
                continue
            row = self._methods[method_id]
            out.append(MethodMatch(
                method_id=method_id,
                canonical_name=row.canonical_name,
                display_name=row.display_name,
                category=row.category,
                method_type=row.method_type,
                brand_family=row.brand_family,
                confidence=confidence,
                classifier="llm",
            ))

        # Proposed new entries — INSERT into treatment_methods FIRST,
        # then add MethodMatch pointing at the fresh row.
        for p in (payload.get("proposed_new") or []):
            try:
                new_id = await self._insert_proposed_method(p)
                if new_id is None:
                    continue
                row = self._methods[new_id]
                confidence = float(p.get("confidence", 0.7))
                out.append(MethodMatch(
                    method_id=new_id,
                    canonical_name=row.canonical_name,
                    display_name=row.display_name,
                    category=row.category,
                    method_type=row.method_type,
                    brand_family=row.brand_family,
                    confidence=confidence,
                    classifier="llm_new",
                ))
            except Exception as ie:
                # Per-proposal failure: log full context + RE-RAISE.
                # Caller (backfill batch) decides whether to abort the
                # batch or continue with remaining services after fix.
                logger.error(
                    "Failed to INSERT proposed_new method display=%r "
                    "category=%r method_type=%r aliases=%s: %s",
                    p.get("display_name"),
                    p.get("category"),
                    p.get("method_type"),
                    p.get("aliases"),
                    ie,
                )
                raise

        return out

    async def _insert_proposed_method(self, p: dict) -> int | None:
        """Validate LLM-proposed method + INSERT into treatment_methods
        with source='llm_inferred'. Generates embedding via OpenAI before
        insert.

        DEDUP GUARD (2026-05-19): If ANY proposed alias overlaps with
        aliases of an existing dictionary entry, we MERGE — extend the
        existing entry's aliases with new (non-overlapping) variants
        and return existing id. Prevents LLM from creating duplicate
        canonicals for the same concept (e.g. botox + botoks +
        toksyna_botulinowa as 3 separate entries instead of 1 with
        merged aliases). This guarantees one-method-one-canonical
        invariant for downstream pricing aggregation."""
        display = (p.get("display_name") or "").strip()
        if not display:
            return None

        method_type = (p.get("method_type") or "").strip().lower()
        if method_type not in _ALLOWED_METHOD_TYPES:
            logger.warning(
                "proposed_new rejected — invalid method_type=%r", method_type,
            )
            return None

        canonical = _slug(display)
        if canonical in self._canonical_to_id:
            # Already in dictionary by canonical name
            return self._canonical_to_id[canonical]

        category = (p.get("category") or "inny").strip().lower()
        brand_family = (p.get("brand_family") or "").strip().lower() or None
        description = (p.get("description") or "").strip() or None
        raw_aliases = p.get("aliases") or []
        if not isinstance(raw_aliases, list):
            raw_aliases = []
        # Normalize aliases — lowercase, dedupe, ensure at minimum the
        # display name itself + folded variant are present.
        alias_set: set[str] = set()
        for a in raw_aliases:
            if isinstance(a, str):
                a_norm = a.strip().lower()
                if a_norm and len(a_norm) >= 2:
                    alias_set.add(a_norm)
        alias_set.add(display.lower())
        alias_set.add(_ascii_fold(display))
        aliases = sorted(alias_set)

        # ── DEDUP GUARD — check alias overlap with existing entries ──
        # Build set of normalized aliases for substring match.
        proposed_normalized = {_normalize_for_match(a) for a in aliases}
        proposed_normalized.discard("")
        # Find any existing methods whose aliases overlap with proposed.
        for existing_id, existing in self._methods.items():
            existing_norm = {_normalize_for_match(a) for a in existing.aliases}
            existing_norm.discard("")
            overlap = proposed_normalized & existing_norm
            if overlap:
                # Merge: extend existing aliases with new (non-overlapping) ones
                new_aliases = sorted(set(existing.aliases) | set(aliases))
                if set(new_aliases) != set(existing.aliases):
                    try:
                        self.supabase.client.table("treatment_methods") \
                            .update({"aliases": new_aliases}) \
                            .eq("id", existing_id) \
                            .execute()
                        # Update in-memory representation
                        existing.aliases = new_aliases
                        # Rebuild relevant alias_index entries for this method
                        self._alias_index = [
                            t for t in self._alias_index if t[1] != existing_id
                        ]
                        for a in new_aliases:
                            n = _normalize_for_match(a)
                            if len(n) >= 2:
                                self._alias_index.append((n, existing_id))
                        self._alias_index.sort(key=lambda t: -len(t[0]))
                        logger.info(
                            "Dictionary MERGED: extended %r (id=%d) with %d new "
                            "aliases from proposed %r (overlap on %r)",
                            existing.canonical_name, existing_id,
                            len(new_aliases) - len(set(existing.aliases) - set(aliases)),
                            display, list(overlap)[:3],
                        )
                    except Exception as me:
                        logger.error(
                            "alias merge failed for existing id=%d "
                            "(was extending %r with proposed %r): %s",
                            existing_id, existing.canonical_name, display, me,
                        )
                        raise
                else:
                    logger.info(
                        "Dictionary dedup: proposed %r already covered by "
                        "existing %r (id=%d) via overlap on %r",
                        display, existing.canonical_name, existing_id,
                        list(overlap)[:3],
                    )
                return existing_id

        # Build embedding input
        emb_input = "\n".join([
            canonical,
            display,
            "Aliases: " + ", ".join(aliases) if aliases else "",
            description or "",
        ]).strip()

        # Generate embedding. FAIL-LOUD: OpenAI errors propagate to caller.
        # Embedding is REQUIRED for new dictionary entries — without it
        # ANN classifier can't find this method in future runs, defeating
        # the dictionary-first principle. Better to abort the proposal
        # than insert a broken row.
        emb_str: str | None = None
        if self.openai_client is not None:
            emb_resp = await _openai_call_with_retry(
                self.openai_client.embeddings.create,
                model="text-embedding-3-small",
                input=emb_input,
                _retry_label="insert_proposed_method.embed",
            )
            vec = emb_resp.data[0].embedding
            emb_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"

        row_payload = {
            "canonical_name": canonical,
            "display_name": display,
            "category": category,
            "method_type": method_type,
            "brand_family": brand_family,
            "aliases": aliases,
            "body_areas": None,
            "description": description,
            "competitor_methods": [],
            "source": "llm_inferred",
        }
        if emb_str:
            row_payload["embedding"] = emb_str

        try:
            res = self.supabase.client.table("treatment_methods") \
                .upsert(row_payload, on_conflict="canonical_name") \
                .execute()
        except Exception as e:
            logger.error(
                "INSERT proposed_new failed for canonical=%r aliases=%s: %s",
                canonical, aliases, e,
            )
            raise

        if not res.data:
            # No rows returned (rare — Supabase issue). Treat as failure.
            raise RuntimeError(
                f"UPSERT returned no rows for canonical={canonical!r}"
            )
        new_id = int(res.data[0]["id"])

        # Refresh in-memory index. Use the row we just inserted (DB has
        # the canonical form including generated id).
        self._register_method_row({
            "id": new_id,
            "canonical_name": canonical,
            "display_name": display,
            "category": category,
            "method_type": method_type,
            "brand_family": brand_family,
            "aliases": aliases,
            "source": "llm_inferred",
        })
        # Re-sort alias index now that we added entries
        self._alias_index.sort(key=lambda t: -len(t[0]))

        logger.info(
            "Dictionary extended: canonical=%r display=%r category=%s "
            "method_type=%s aliases_count=%d",
            canonical, display, category, method_type, len(aliases),
        )
        return new_id

    # ── Public entry point (MULTI-MATCH) ─────────────────────────────

    async def classify_service(
        self,
        service_id: int,
        service_name: str,
        *,
        name_embedding: Any = None,
        category_hint: str | None = None,
        use_llm: bool = True,
        skip_db_cache: bool = False,
    ) -> list[MethodMatch]:
        """Run the full cascade. Returns list of MethodMatch — may be:
        - empty (no method recognized, e.g. "konsultacja" / "voucher")
        - single element (most branded services like "Botoks 1 okolica")
        - multiple elements (combo services like "PRO XN + Dermapen")

        `skip_db_cache=True` skip'uje per-service GET na
        service_method_classification — caller (backfill z --skip-classified)
        gwarantuje że service NIE jest jeszcze classified, więc cache
        lookup byłby redundantnym roundtripem. Daje 3-5x speedup w
        Fazie G bo eliminuje ~60% HTTP traffic do Supabase."""
        if not skip_db_cache:
            cached = await self._load_cache(service_id)
            if cached is not None:
                return cached

        # 1. Alias substring MULTI-match
        alias_matches = self._classify_by_alias_multi(service_name)
        if alias_matches:
            self._queue_cache_write(service_id, alias_matches)
            return alias_matches

        # 2. Embedding ANN (single best match path — when alias missed,
        # service likely refers to ONE method via paraphrase)
        candidates: list[dict] = []
        ann_result = await self._classify_by_embedding(name_embedding, category_hint)
        if ann_result is not None:
            ann_match, candidates = ann_result
            if ann_match.confidence >= ANN_ACCEPT_THRESHOLD:
                self._queue_cache_write(service_id, [ann_match])
                return [ann_match]

        # 3. LLM — picks from candidates AND/OR proposes new entries
        if use_llm and self.llm_client is not None:
            llm_matches = await self._classify_by_llm(
                service_name, category_hint, candidates
            )
            if llm_matches:
                self._queue_cache_write(
                    service_id, llm_matches,
                    llm_response={"candidates": candidates},
                )
                return llm_matches

        # Genuinely unclassified — cache empty list so we don't re-try
        # this service every audit. Caller can decide if empty means
        # "service has no method" (consultation, voucher) or "needs more
        # dictionary entries" (long-tail unknown brand).
        self._queue_cache_write(service_id, [])
        return []

    # ── Bulk helper (multi-match) ────────────────────────────────────

    async def classify_services(
        self,
        services: list[dict],
        *,
        use_llm: bool = True,
        max_concurrent: int = 5,
    ) -> dict[int, list[MethodMatch]]:
        """Classify a batch of services. Returns {service_id: list[MethodMatch]}."""
        sem = asyncio.Semaphore(max_concurrent)

        async def _one(svc: dict) -> tuple[int, list[MethodMatch]]:
            async with sem:
                ms = await self.classify_service(
                    service_id=int(svc["id"]),
                    service_name=svc.get("name") or "",
                    name_embedding=svc.get("name_embedding"),
                    category_hint=svc.get("category_name"),
                    use_llm=use_llm,
                )
                return int(svc["id"]), ms

        results = await asyncio.gather(*[_one(s) for s in services])
        return dict(results)
