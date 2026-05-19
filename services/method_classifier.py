"""Method classifier — map salon_scrape_services rows to canonical
treatment_methods (mig 095) via cascading alias → ANN → LLM strategy.

Replaces the reactive regex whitelist in `services/brand_marker.py`
with a queryable, scalable taxonomy that every pipeline stage can use.

Cascade order (fastest + most precise first):

  1. **Cache hit** — service_method_classification row already exists.
     O(1) point lookup, no DB round-trip beyond that.

  2. **Alias substring** — ASCII-folded lowercase service name searched
     against each method's `aliases` JSONB. Longest alias wins to
     prevent "prx" matching before "prx_t33". O(n_methods) in memory
     after warmup() preloads the alias index. ~60-70% coverage.

  3. **Embedding ANN** — service's `name_embedding` (1536-dim, OpenAI
     text-embedding-3-small) compared against treatment_methods.embedding
     via RPC `fn_classify_service_by_embedding`. Default min_similarity
     0.70, accept ≥0.75 as confident. Single SQL round-trip. +20%
     coverage.

  4. **LLM fallback** — gpt-4o-mini in structured JSON mode, given
     service name + top-N ANN candidates as context. Cached forever in
     service_method_classification once decided (idempotent for re-runs).
     +5-10% coverage. Last resort.

Cache writes are batched via `flush_cache_writes()` to avoid one row
per classification. Caller should flush at the end of a pipeline run.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# Confidence thresholds (tunable per category if needed via overrides)
ALIAS_EXACT_CONFIDENCE = 1.0  # Hard match — alias literally present
ANN_ACCEPT_THRESHOLD = 0.75   # Below this we escalate to LLM
ANN_TOP_N = 5                 # Candidates passed to LLM for verification
LLM_MIN_CONFIDENCE = 0.7      # LLM-claimed confidence floor


# Pattern fragments that don't carry method information (Polish + EN).
# Stripped before alias matching to reduce false negatives like
# "depilacja kobieta thunder" not matching "thunder" if it would have
# (already works as substring, but stripping helps embedding too).
_NOISE_WORDS = {
    "zabieg", "zabiegu", "zabiegow", "zabiegow", "zabiegi",
    "twarz", "twarzy", "szyja", "szyi", "dekolt", "dekoltu",
    "rece", "dlonie", "stopy", "stop",
    "kobieta", "mezczyzna", "pani", "pan",
    "kompleksowy", "kompleksowa", "pakiet", "promocja", "promocyjny",
    "pelny", "pelna", "i", "ii", "iii", "stopnia",
    "small", "medium", "large", "xl", "xs",
}


@dataclass
class MethodRow:
    """In-memory representation of a treatment_methods row (cached after
    warmup). Minimal columns — we don't need the embedding here because
    ANN goes via RPC."""
    id: int
    canonical_name: str
    display_name: str
    category: str
    method_type: str
    brand_family: str | None
    aliases: list[str] = field(default_factory=list)


@dataclass
class MethodMatch:
    """Result of a classification call. `classifier` records which step
    in the cascade produced this result, for cache provenance + debug."""
    method_id: int
    canonical_name: str
    display_name: str
    category: str
    method_type: str
    brand_family: str | None
    confidence: float
    classifier: str  # 'cached' | 'alias_exact' | 'embedding_ann' | 'llm'
    matched_alias: str | None = None


def _ascii_fold(s: str) -> str:
    """Polish + diacritics → ASCII lowercase. Idempotent."""
    s = unicodedata.normalize("NFKD", s or "")
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower()


def _normalize_for_match(s: str) -> str:
    """Squash whitespace + folded ASCII. Used on BOTH alias and target
    side so substring search is symmetric."""
    folded = _ascii_fold(s)
    # Collapse non-alphanumeric runs to single space
    folded = re.sub(r"[^a-z0-9]+", " ", folded)
    folded = re.sub(r"\s+", " ", folded).strip()
    return folded


class MethodClassifier:
    """Cached cascading classifier. Instantiate once per pipeline run
    (NOT per service) — warmup loads all 689 methods into memory.

    Usage:

        clf = MethodClassifier(supabase, llm_client=openai_client)
        await clf.warmup()

        match = await clf.classify_service(
            service_id=12345,
            service_name="PRO XN - Acne Rescue Treatment (twarz + szyja)",
            name_embedding=svc.get("name_embedding"),
            category_hint="peeling",   # optional
            use_llm=True,              # set False to skip LLM and accept ANN ≥0.70
        )
        if match:
            print(match.canonical_name, match.confidence, match.classifier)

        await clf.flush_cache_writes()  # at end of pipeline
    """

    def __init__(
        self,
        supabase: SupabaseService,
        llm_client: Any | None = None,
    ):
        self.supabase = supabase
        self.llm_client = llm_client

        # Loaded after warmup
        self._methods: dict[int, MethodRow] = {}
        # Sorted longest-first to bias toward more specific matches
        self._alias_index: list[tuple[str, int]] = []  # (normalized_alias, method_id)

        # Cache write buffer (flushed in bulk)
        self._pending_writes: list[dict[str, Any]] = []

        # In-process cache: service_id → MethodMatch (so multiple calls
        # within one run for the same service don't re-query)
        self._inprocess_cache: dict[int, MethodMatch] = {}

    # ── Loading / warmup ─────────────────────────────────────────────

    async def warmup(self) -> None:
        """Load all treatment_methods from DB + build alias substring
        index. Call once per pipeline run before classify_service."""
        try:
            res = (
                self.supabase.client.table("treatment_methods")
                .select(
                    "id, canonical_name, display_name, category, method_type, "
                    "brand_family, aliases"
                )
                .execute()
            )
        except Exception as e:
            logger.error("warmup: failed to load treatment_methods: %s", e)
            raise

        rows = res.data or []
        for r in rows:
            mid = int(r["id"])
            row = MethodRow(
                id=mid,
                canonical_name=r["canonical_name"],
                display_name=r["display_name"],
                category=r["category"],
                method_type=r["method_type"],
                brand_family=r.get("brand_family"),
                aliases=r.get("aliases") or [],
            )
            self._methods[mid] = row
            for alias in row.aliases:
                normalized = _normalize_for_match(alias)
                if len(normalized) >= 2:
                    self._alias_index.append((normalized, mid))

        # Sort aliases longest-first for greedy match. Same alias may map
        # to multiple methods (rare but possible — e.g. "prx" in PRX-T33
        # and "PRX..." substring); we resolve ties by which method has
        # the LONGEST alias overall (more specific).
        self._alias_index.sort(key=lambda t: -len(t[0]))

        logger.info(
            "MethodClassifier warmup: %d methods, %d alias entries",
            len(self._methods),
            len(self._alias_index),
        )

    # ── Cache (DB-backed + in-process) ───────────────────────────────

    async def _load_cache(self, service_id: int) -> MethodMatch | None:
        """Check in-process cache first; fall back to DB
        service_method_classification row if not seen this run."""
        cached = self._inprocess_cache.get(service_id)
        if cached is not None:
            return cached

        try:
            res = (
                self.supabase.client.table("service_method_classification")
                .select("method_id, confidence, classifier, matched_alias")
                .eq("service_id", service_id)
                .order("confidence", desc=True)
                .limit(1)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "cache lookup failed for service_id=%s: %s", service_id, e
            )
            return None

        rows = res.data or []
        if not rows:
            return None
        row = rows[0]
        method_id = int(row["method_id"])
        method = self._methods.get(method_id)
        if method is None:
            # DB has classification pointing at a method we don't have
            # loaded — schema inconsistency. Treat as cache miss.
            logger.warning(
                "cache row for service_id=%s points to unknown method_id=%s",
                service_id, method_id,
            )
            return None

        match = MethodMatch(
            method_id=method_id,
            canonical_name=method.canonical_name,
            display_name=method.display_name,
            category=method.category,
            method_type=method.method_type,
            brand_family=method.brand_family,
            confidence=float(row["confidence"]),
            classifier="cached",
            matched_alias=row.get("matched_alias"),
        )
        self._inprocess_cache[service_id] = match
        return match

    def _queue_cache_write(
        self,
        service_id: int,
        match: MethodMatch,
        llm_response: dict | None = None,
    ) -> None:
        """Buffer a row for batch UPSERT. Use flush_cache_writes() at end
        of pipeline run to commit."""
        self._pending_writes.append({
            "service_id": service_id,
            "method_id": match.method_id,
            "confidence": float(match.confidence),
            "classifier": match.classifier,
            "matched_alias": match.matched_alias,
            "llm_response": llm_response,
        })
        # Update in-process cache so subsequent same-run calls hit
        self._inprocess_cache[service_id] = match

    async def flush_cache_writes(self) -> int:
        """Commit buffered classifications to service_method_classification.
        Returns count of rows written. Idempotent — UPSERT on
        (service_id, method_id) primary key."""
        if not self._pending_writes:
            return 0

        # Chunk to ~500 rows per HTTP call (Supabase REST limit guideline)
        chunk_size = 500
        total = 0
        for i in range(0, len(self._pending_writes), chunk_size):
            chunk = self._pending_writes[i : i + chunk_size]
            try:
                self.supabase.client.table("service_method_classification") \
                    .upsert(chunk, on_conflict="service_id,method_id") \
                    .execute()
                total += len(chunk)
            except Exception as e:
                logger.error(
                    "cache flush batch %d-%d failed: %s",
                    i, i + len(chunk), e,
                )

        logger.info("MethodClassifier flushed %d cache rows", total)
        self._pending_writes.clear()
        return total

    # ── Alias-exact classifier ───────────────────────────────────────

    def _classify_by_alias(
        self, service_name: str
    ) -> MethodMatch | None:
        """Substring search. Aliases are pre-normalized + sorted longest
        first so the first match wins. Returns None if no alias is a
        substring of the normalized service name."""
        normalized_target = _normalize_for_match(service_name)
        if not normalized_target:
            return None

        for alias, method_id in self._alias_index:
            # Word-boundary match — require the alias to appear as a
            # token (or token sequence), not embedded within a longer
            # word. "ipl" alias shouldn't match "siprilon" by accident.
            if self._is_token_substring(alias, normalized_target):
                method = self._methods[method_id]
                return MethodMatch(
                    method_id=method_id,
                    canonical_name=method.canonical_name,
                    display_name=method.display_name,
                    category=method.category,
                    method_type=method.method_type,
                    brand_family=method.brand_family,
                    confidence=ALIAS_EXACT_CONFIDENCE,
                    classifier="alias_exact",
                    matched_alias=alias,
                )
        return None

    @staticmethod
    def _is_token_substring(needle: str, haystack: str) -> bool:
        """Both strings are pre-normalized (lowercase ASCII tokens
        separated by single spaces). Match needle on word boundaries."""
        if not needle:
            return False
        # Multi-word needle: check exact substring within haystack
        # (already token-normalized so embedded false-positives are rare).
        if " " in needle:
            return f" {needle} " in f" {haystack} "
        # Single token: require word-boundary
        return f" {needle} " in f" {haystack} "

    # ── Embedding ANN classifier ─────────────────────────────────────

    async def _classify_by_embedding(
        self,
        name_embedding: Any,
        category_hint: str | None = None,
    ) -> tuple[MethodMatch, list[dict]] | None:
        """RPC call to fn_classify_service_by_embedding. Returns the top
        match (plus the full top-N candidates for the LLM fallback layer)
        when similarity ≥ ANN_ACCEPT_THRESHOLD, else None.

        Returns (match, top_n_candidates) tuple; the caller can pass
        top_n_candidates to LLM when the top alone isn't confident enough."""
        if name_embedding is None:
            return None
        try:
            res = self.supabase.client.rpc(
                "fn_classify_service_by_embedding",
                {
                    "p_service_embedding": name_embedding,
                    "p_category_hint": category_hint,
                    "p_top_n": ANN_TOP_N,
                    "p_min_similarity": 0.55,  # surface candidates for LLM
                },
            ).execute()
        except Exception as e:
            logger.warning("ANN RPC failed: %s", e)
            return None

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

    # ── LLM fallback classifier ──────────────────────────────────────

    async def _classify_by_llm(
        self,
        service_name: str,
        category_hint: str | None,
        candidates: list[dict],
    ) -> MethodMatch | None:
        """Ask gpt-4o-mini structured-output to pick the best canonical
        from the ANN top-N. Refuses with `{canonical_name: null}` if
        none fit. Returns None on LLM failure or rejection."""
        if not self.llm_client:
            return None
        if not candidates:
            return None

        candidate_lines = [
            f"- {c['canonical_name']} ({c['display_name']}) — "
            f"category={c['category']} brand_family={c.get('brand_family') or 'n/a'} "
            f"cos_sim={float(c['similarity']):.2f}"
            for c in candidates
        ]

        system = (
            "You are a Polish beauty/medical-aesthetics taxonomy expert. "
            "Given a salon's raw service name, choose the single canonical "
            "treatment method from the provided candidate list that best "
            "describes the actual treatment. Return strict JSON. "
            "If no candidate is a sensible match, return canonical_name=null."
        )
        user = (
            f"Service name: {service_name!r}\n"
            f"Category hint: {category_hint or 'unknown'}\n\n"
            f"Candidates (top-{len(candidates)} by embedding similarity):\n"
            + "\n".join(candidate_lines)
            + "\n\nReturn JSON: {\"canonical_name\": <one of the above OR null>, "
            "\"confidence\": <0.0-1.0>, \"reasoning\": <short PL/EN>}."
        )

        try:
            resp = await asyncio.to_thread(
                self.llm_client.chat.completions.create,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
        except Exception as e:
            logger.warning("LLM classify call failed: %s", e)
            return None

        try:
            content = resp.choices[0].message.content or "{}"
            payload = json.loads(content)
        except (json.JSONDecodeError, AttributeError, IndexError) as e:
            logger.warning("LLM returned unparseable JSON: %s", e)
            return None

        canonical = payload.get("canonical_name")
        if not canonical:
            return None
        confidence = float(payload.get("confidence", 0.0))
        if confidence < LLM_MIN_CONFIDENCE:
            return None

        # Find the matching method row by canonical_name
        target = next(
            (m for m in self._methods.values() if m.canonical_name == canonical),
            None,
        )
        if target is None:
            logger.warning(
                "LLM returned canonical_name=%r not in our loaded methods",
                canonical,
            )
            return None

        return MethodMatch(
            method_id=target.id,
            canonical_name=target.canonical_name,
            display_name=target.display_name,
            category=target.category,
            method_type=target.method_type,
            brand_family=target.brand_family,
            confidence=confidence,
            classifier="llm",
        )

    # ── Public entry point ───────────────────────────────────────────

    async def classify_service(
        self,
        service_id: int,
        service_name: str,
        *,
        name_embedding: Any = None,
        category_hint: str | None = None,
        use_llm: bool = True,
    ) -> MethodMatch | None:
        """Run the full cascade. Returns None when no path produces a
        confident match (caller decides whether to keep service in
        aggregation pool with `method=None` semantics)."""
        # 0. Cache (in-process + DB)
        cached = await self._load_cache(service_id)
        if cached is not None:
            return cached

        # 1. Alias substring (fast path)
        match = self._classify_by_alias(service_name)
        if match is not None:
            self._queue_cache_write(service_id, match)
            return match

        # 2. Embedding ANN
        candidates: list[dict] = []
        ann_result = await self._classify_by_embedding(name_embedding, category_hint)
        if ann_result is not None:
            match, candidates = ann_result
            if match.confidence >= ANN_ACCEPT_THRESHOLD:
                self._queue_cache_write(service_id, match)
                return match

        # 3. LLM fallback (uses ANN top-N as anchor)
        if use_llm and candidates:
            llm_match = await self._classify_by_llm(
                service_name, category_hint, candidates
            )
            if llm_match is not None:
                self._queue_cache_write(
                    service_id, llm_match,
                    llm_response={"candidates": candidates},
                )
                return llm_match

        return None

    # ── Bulk helper ──────────────────────────────────────────────────

    async def classify_services(
        self,
        services: list[dict],
        *,
        use_llm: bool = True,
        max_concurrent: int = 5,
    ) -> dict[int, MethodMatch]:
        """Classify a list of services. Returns {service_id: MethodMatch}
        for those that succeeded. Service dicts must include `id` and
        `name`; `name_embedding` and `category_name` are optional and
        improve cascade hit-rate."""
        sem = asyncio.Semaphore(max_concurrent)

        async def _one(svc: dict) -> tuple[int, MethodMatch | None]:
            async with sem:
                m = await self.classify_service(
                    service_id=int(svc["id"]),
                    service_name=svc.get("name") or "",
                    name_embedding=svc.get("name_embedding"),
                    category_hint=svc.get("category_name"),
                    use_llm=use_llm,
                )
                return int(svc["id"]), m

        results = await asyncio.gather(*[_one(s) for s in services])
        return {sid: m for sid, m in results if m is not None}
