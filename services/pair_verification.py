"""LLM-based service pair verification (Faza 7, 2026-05-16).

Embedding cosine sometimes accepts services that share vocabulary but
not the actual procedure. Empirical case from Beauty4ever 2026-05-16:
under Booksy tid=630 (peelings) the variant filter accepted

    subject:    PRO XN - II stopień (twarz + szyja)        1100 zł
    candidates: Wybielanie okolic intymnych - INFINI         300 zł
                Kwas azelainowy - plecy                       350 zł
                Bloomea LIGHTENING (peeling kwasowy)          650 zł

…even though they are completely different procedures (different brand,
different body parts, different chemistry). User feedback was blunt:
"embedding bez faktycznego myślenia AI na chuj sie nadaje".

This module is the semantic gate sitting on top of the embedding-based
method-similarity filter. For each pricing row we ask an LLM:
"Are these candidate competitor services actually comparable to the
subject for pricing purposes?". Verdicts are cached in the
`service_pair_verifications` Postgres table (mig 078 + mig 079) so the
same (subject_name, competitor_name, tid) pair seen in future audits
never re-pays for the LLM.

Scope: ONLY tier=treatment rows in `_compute_treatment_tier_rows`. For
tier=variant and tier=sub_variant, the cluster_id grouping (HDBSCAN /
native Booksy) already enforces method match. The treatment tier is
the loosest grouping and the only one where vocabulary-overlap false
positives bite.

Hard errors. No graceful fallbacks. LLM failure → raise. DB failure →
raise. Bugsink captures via the standard logger.
"""
from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

from services.supabase import SupabaseService

if TYPE_CHECKING:  # avoid a runtime circular import (pipeline_trace ← services)
    from services.pipeline_trace import TraceWriter

logger = logging.getLogger(__name__)


# Canonical model name. Cache rows are versioned by `model_used`, so
# bumping this string (e.g. to "gpt-4o") invalidates the cache cleanly
# without touching DB rows. All call sites — LLM client, lookup RPC,
# insert payload — MUST use this constant.
_PAIR_VERIFY_MODEL = "gpt-4o-mini"

# Token budget per batch LLM call. Each candidate adds ~25 tokens for
# the prompt line + ~50 for the verdict object. 20 candidates ≈ 1500
# user-prompt tokens + ~2000 output ≈ fits inside 4k easily.
_PAIR_VERIFY_BATCH_SIZE = 20
_PAIR_VERIFY_MAX_TOKENS = 2048


# Allowed structured rejection reasons. Mirrors the documentation in
# migration 079. Frontend renders these as pills next to rejected rows;
# unknown values fall back to "other".
_ALLOWED_REJECTION_REASONS = frozenset({
    "package",
    "bundle_areas",
    "bundle_methods",
    "intensity_mismatch",
    "different_method",
    "different_area",
    "different_brand",
    "low_confidence",
    "other",
})


# Decorative leading symbols often appended to salon service names that
# carry no semantic meaning for comparison purposes. We strip them in
# `_normalize_pair_name` so "✦ Botoks" and "Botoks" collapse to the
# same cache key. Kept in sync with what we see in production cenniks.
_LEADING_DECORATION_RE = re.compile(
    r"^[\s\-–—•·*✦💉🔲◾◽▪▫►▶❖✪✿❣♥]+",
    flags=re.UNICODE,
)

# Collapse runs of whitespace. Combines with .lower() to match the SQL
# fn_synthetic_normalize convention used by the cache table's unique
# index — without that the same logical pair gets distinct cache rows
# when one of the names has a stray double-space.
_WHITESPACE_RUN_RE = re.compile(r"\s+")


def _normalize_pair_name(name: str | None) -> str:
    """Canonical form for cache lookup keys.

    Matches the spirit of `fn_synthetic_normalize` in mig 073 — lower
    case, collapse whitespace, strip leading decoration. We do NOT
    fold diacritics: "łydka" and "lydka" are intentionally distinct
    because losing the ł changes meaning ("łydka" = calf, "lydka" is
    not a Polish word). Treat that as user error not noise.
    """
    if not name:
        return ""
    s = str(name).strip()
    s = _LEADING_DECORATION_RE.sub("", s)
    s = _WHITESPACE_RUN_RE.sub(" ", s).strip()
    return s.lower()


_PAIR_VERIFY_SYSTEM_PROMPT = """Jesteś ekspertem oceniającym czy dwie usługi w salonie beauty to NAPRAWDĘ porównywalne procedury cenowe. Twoja median musi reprezentować POJEDYNCZĄ usługę danego typu — jeśli dasz zielone światło pakietowi albo bundle'owi, raport pokaże fałszywą medianę i właściciel salonu podejmie decyzję na podstawie błędnych danych.

Dwie usługi są PORÓWNYWALNE tylko wtedy, gdy SPEŁNIAJĄ WSZYSTKO:
- Ta sama metoda/technologia (PRX-T33 == PRO XN, ale INFINI != PRX, peeling kwasowy != mezoterapia igłowa)
- Ta sama część ciała (twarz != okolice intymne != plecy != łydki)
- Ta sama intensywność (1 zabieg == 1 zabieg; pakiet 3x == pakiet 3x; 1 zabieg != pakiet 5x)
- Podobny scope (cała twarz == cała twarz, NIE okolice oczu; jedna okolica != całe ciało)

KRYTYCZNE — wymuszone reguły odrzucania (zwróć is_comparable=false z odpowiednim rejection_reason):

REJECT z rejection_reason='package':
- "Pakiet X zabiegów", "Pakietowo", "Pakiet [zabieg]" gdzie cena pokrywa wiele wizyt
- "Nx [zabieg]" lub "[zabieg] x N" (np. 3x, 5x, 10x mezoterapia)
- "abonament", "karnet", "voucher N wizyt", "miesięczny abonament"
- "seria po N", "seria 5", "N sesji"

REJECT z rejection_reason='bundle_areas':
- "szyja + dekolt", "twarz + szyja + dekolt", "twarz + szyja"
- "bikini + pachy + nogi", "łydki + uda", "kolana + łydki"
- Każda usługa łącząca więcej niż jeden obszar ciała w jednej cenie

REJECT z rejection_reason='bundle_methods':
- "Botoks + kwas hialuronowy", "Mezoterapia + LED therapy"
- "Peeling + maska + masaż twarzy"
- Każda usługa łącząca więcej niż jedną procedurę/metodę w jednej cenie

REJECT z rejection_reason='intensity_mismatch':
- Subject = pojedyncza usługa, candidate = pakiet (lub odwrotnie) — to są różne JEDNOSTKI sprzedaży
- Subject = "1 okolica", candidate = "całe ciało"
- Subject = "5 zabiegów", candidate = "10 zabiegów"

REJECT z rejection_reason='different_method':
- Różna technologia mimo tej samej kategorii Booksy
- Thunder IPL vs pasta cukrowa, PRX-T33 vs mezoterapia igłowa

REJECT z rejection_reason='different_area':
- Inna część ciała: "Peeling PRX-T33 (twarz)" vs "Wybielanie okolic intymnych PRX-T Lady"
- "Kwas azelainowy - plecy" vs "Peeling kwasowy twarz"

REJECT z rejection_reason='different_brand':
- Inna linia produktowa: INFINI vs PRX, Bloomea vs PRO XN
- Tylko gdy różnica linii oznacza inną aktywną substancję / efekt

REJECT z rejection_reason='low_confidence':
- Gdy nie masz pewności i nie ma silnego sygnału za is_comparable=true

REJECT z rejection_reason='other':
- Wszystkie inne powody — reasoning ma szczegóły

Tylko is_comparable=true (rejection_reason=null) gdy WSZYSTKIE warunki spełnione:
- Ta sama metoda/technologia
- Ta sama część ciała (lub jeden konkretny obszar pasujący do subject'a)
- Ta sama intensywność (1 sesja vs 1 sesja, pakiet 3x vs pakiet 3x)
- Podobny scope

ODPOWIADAJ WYŁĄCZNIE jednym JSON-em, bez markdown, bez komentarzy, bez tekstu przed/po. Format:
{"verdicts":[{"index":<int>,"competitor_name":"<string>","is_comparable":<bool>,"confidence":<float 0-1>,"reasoning":"<1 krótkie zdanie po polsku>","rejection_reason":"<string|null>"}, ...]}"""


def _build_pair_verify_user_prompt(
    subject_service_name: str,
    candidate_names: list[str],
    booksy_treatment_id: int | None,
    synthetic_treatment_id: int | None,
) -> str:
    """Build the user prompt for one batch of candidates."""
    tid_line = ""
    if booksy_treatment_id is not None:
        tid_line = f"\n  Kategoria Booksy: tid={booksy_treatment_id}"
    elif synthetic_treatment_id is not None:
        tid_line = f"\n  Kategoria syntetyczna: id={synthetic_treatment_id}"

    cand_lines = "\n".join(
        f"  [{i + 1}] {name}" for i, name in enumerate(candidate_names)
    )

    return f"""Subject service (TY):
  Nazwa: {subject_service_name}{tid_line}

Kandydaci do porównania:
{cand_lines}

Dla KAŻDEGO kandydata zwróć obiekt {{index, competitor_name, is_comparable, confidence, reasoning, rejection_reason}}. Zachowaj kolejność po index. rejection_reason MUSI być null gdy is_comparable=true, w przeciwnym razie jeden z: package, bundle_areas, bundle_methods, intensity_mismatch, different_method, different_area, different_brand, low_confidence, other."""


def _coerce_rejection_reason(raw: Any, is_comparable: bool) -> str | None:
    """Validate / normalize rejection_reason from LLM output."""
    if is_comparable:
        return None
    if raw is None:
        return "other"
    value = str(raw).strip().lower()
    if not value or value == "null":
        return "other"
    if value in _ALLOWED_REJECTION_REASONS:
        return value
    # LLM occasionally synonymizes — collapse common variants. Anything
    # else falls into 'other' so the cache row still has a valid value.
    mapping = {
        "packages": "package",
        "pakiet": "package",
        "bundle": "bundle_areas",
        "bundle_area": "bundle_areas",
        "multi_area": "bundle_areas",
        "bundle_method": "bundle_methods",
        "multi_method": "bundle_methods",
        "method_mismatch": "different_method",
        "area_mismatch": "different_area",
        "brand_mismatch": "different_brand",
        "intensity": "intensity_mismatch",
        "low_conf": "low_confidence",
    }
    return mapping.get(value, "other")


async def _call_llm_for_batch(
    *,
    subject_service_name: str,
    candidate_names: list[str],
    booksy_treatment_id: int | None,
    synthetic_treatment_id: int | None,
    llm_client: Any,
) -> dict[str, Any]:
    """Single LLM call for one batch of up to _PAIR_VERIFY_BATCH_SIZE
    candidates. Returns the parsed JSON dict (caller is responsible for
    validating shape)."""
    user_prompt = _build_pair_verify_user_prompt(
        subject_service_name=subject_service_name,
        candidate_names=candidate_names,
        booksy_treatment_id=booksy_treatment_id,
        synthetic_treatment_id=synthetic_treatment_id,
    )
    logger.info(
        "pair_verification: LLM batch call subject=%r candidates=%d "
        "booksy_tid=%s synthetic_tid=%s",
        subject_service_name[:80],
        len(candidate_names),
        booksy_treatment_id,
        synthetic_treatment_id,
    )
    response = await llm_client.generate_json(
        prompt=user_prompt,
        system=_PAIR_VERIFY_SYSTEM_PROMPT,
        max_tokens=_PAIR_VERIFY_MAX_TOKENS,
    )
    if not isinstance(response, dict):
        raise ValueError(
            f"pair_verification: LLM returned non-dict payload: "
            f"{type(response).__name__}"
        )
    return response


async def verify_service_pairs(
    *,
    subject_service_name: str,
    candidate_competitor_names: list[str],
    booksy_treatment_id: int | None,
    synthetic_treatment_id: int | None,
    supabase: SupabaseService,
    llm_client: Any,
    audit_id: str | None = None,
    model: str = _PAIR_VERIFY_MODEL,
    tracer: "TraceWriter | None" = None,
) -> dict[str, dict[str, Any]]:
    """Verify each candidate vs subject — is this actually a comparable
    service for pricing comparison purposes?

    Args:
        subject_service_name: representative name of the subject service
            for this pricing row (use the canonical / most common one).
        candidate_competitor_names: raw competitor service names. Will
            be normalized internally for cache lookup.
        booksy_treatment_id: Booksy tid of the row. Exactly one of
            booksy_treatment_id / synthetic_treatment_id must be set.
        synthetic_treatment_id: synthetic taxonomy id of the row.
        supabase: SupabaseService instance for cache lookup + insert.
        llm_client: instance with `async generate_json(prompt, system,
            max_tokens) -> dict`. Typically `_get_hidden_inference_llm()`.
        audit_id: convex_audit_id stored on newly created cache rows
            (`created_by_audit_id` — first audit that triggered the
            verdict).
        model: cache versioning key. Defaults to the canonical
            `_PAIR_VERIFY_MODEL` constant.

    Returns:
        Dict keyed by normalized competitor name. Each value:
            {
                is_comparable: bool,
                confidence: float,
                reasoning: str,
                rejection_reason: str | None,
                from_cache: bool,
                cached_id: int | None,
            }

    Raises:
        ValueError: invalid inputs (missing llm_client, missing tid).
        Exception: LLM or DB failures propagate. No graceful fallback.
    """
    if llm_client is None:
        raise RuntimeError(
            "pair_verification: llm_client is required (no OPENAI_API_KEY?)"
        )
    if booksy_treatment_id is None and synthetic_treatment_id is None:
        raise ValueError(
            "pair_verification: at least one of booksy_treatment_id / "
            "synthetic_treatment_id must be set"
        )
    if not subject_service_name:
        raise ValueError(
            "pair_verification: subject_service_name is required"
        )

    subject_norm = _normalize_pair_name(subject_service_name)
    if not subject_norm:
        raise ValueError(
            "pair_verification: subject_service_name normalized to empty"
        )

    # Map normalized → raw, deduped. Keep one raw name per normalized
    # form so the LLM prompt doesn't get bloated with near-duplicates.
    norm_to_raw: dict[str, str] = {}
    for raw in candidate_competitor_names:
        norm = _normalize_pair_name(raw)
        if not norm:
            continue
        norm_to_raw.setdefault(norm, raw)
    if not norm_to_raw:
        return {}

    candidate_norms = list(norm_to_raw.keys())

    # Step 1: bulk cache lookup. Cached verdicts come back keyed by
    # normalized competitor name; we trust them as-is.
    cached = await supabase.lookup_pair_verifications(
        subject_name_normalized=subject_norm,
        competitor_names_normalized=candidate_norms,
        booksy_treatment_id=booksy_treatment_id,
        synthetic_treatment_id=synthetic_treatment_id,
        model=model,
    )

    results: dict[str, dict[str, Any]] = {}
    cached_ids: list[int] = []
    for norm, entry in cached.items():
        results[norm] = {
            "is_comparable": bool(entry.get("is_comparable")),
            "confidence": float(entry.get("confidence") or 0.0),
            "reasoning": entry.get("reasoning") or "",
            "rejection_reason": entry.get("rejection_reason"),
            "from_cache": True,
            "cached_id": entry.get("cached_id"),
        }
        cid = entry.get("cached_id")
        if isinstance(cid, int):
            cached_ids.append(cid)

    uncached_norms = [n for n in candidate_norms if n not in results]
    logger.info(
        "pair_verification: subject=%r tid=(booksy=%s,synth=%s) "
        "cache hit=%d miss=%d",
        subject_service_name[:80],
        booksy_treatment_id,
        synthetic_treatment_id,
        len(results),
        len(uncached_norms),
    )

    # Step 2: LLM batches for uncached pairs.
    if uncached_norms:
        new_rows: list[dict[str, Any]] = []
        for _batch_index, batch_start in enumerate(
            range(0, len(uncached_norms), _PAIR_VERIFY_BATCH_SIZE)
        ):
            batch_norms = uncached_norms[
                batch_start : batch_start + _PAIR_VERIFY_BATCH_SIZE
            ]
            # Use the original raw names in the prompt so the LLM gets
            # the salon's actual labelling (with formatting cues like
            # parens that hint at intensity / area).
            batch_raw = [norm_to_raw[n] for n in batch_norms]

            # P3 LLM latency (quick 260613-m23): time each LLM batch call so
            # the operator can see per-batch verification latency in the trace.
            _llm_t0 = time.monotonic()
            response = await _call_llm_for_batch(
                subject_service_name=subject_service_name,
                candidate_names=batch_raw,
                booksy_treatment_id=booksy_treatment_id,
                synthetic_treatment_id=synthetic_treatment_id,
                llm_client=llm_client,
            )
            _llm_dur_ms = int((time.monotonic() - _llm_t0) * 1000)
            if tracer is not None:
                tracer.add(
                    "pair_verify.llm_batch",
                    {
                        "candidates": len(batch_raw),
                        "duration_ms": _llm_dur_ms,
                        "batch_index": _batch_index,
                        "subject": subject_service_name[:120],
                        "booksy_tid": booksy_treatment_id,
                        "synthetic_tid": synthetic_treatment_id,
                    },
                )

            verdicts = response.get("verdicts")
            if not isinstance(verdicts, list):
                raise ValueError(
                    "pair_verification: LLM response missing verdicts list: "
                    f"keys={list(response.keys())[:5]}"
                )

            # Build a lookup by 1-based index into batch_raw. We trust
            # the index over `competitor_name` because some models echo
            # back paraphrased names.
            for verdict in verdicts:
                if not isinstance(verdict, dict):
                    continue
                idx = verdict.get("index")
                if not isinstance(idx, int) or idx < 1 or idx > len(batch_raw):
                    continue
                raw_name = batch_raw[idx - 1]
                norm_name = _normalize_pair_name(raw_name)
                if not norm_name:
                    continue
                is_comparable = bool(verdict.get("is_comparable"))
                confidence = verdict.get("confidence")
                try:
                    confidence_f = float(confidence) if confidence is not None else 0.0
                except (TypeError, ValueError):
                    confidence_f = 0.0
                reasoning = str(verdict.get("reasoning") or "")
                rejection_reason = _coerce_rejection_reason(
                    verdict.get("rejection_reason"),
                    is_comparable,
                )

                results[norm_name] = {
                    "is_comparable": is_comparable,
                    "confidence": confidence_f,
                    "reasoning": reasoning,
                    "rejection_reason": rejection_reason,
                    "from_cache": False,
                    "cached_id": None,
                }

                new_rows.append({
                    "subject_name_normalized": subject_norm,
                    "competitor_name_normalized": norm_name,
                    "booksy_treatment_id": booksy_treatment_id,
                    "synthetic_treatment_id": synthetic_treatment_id,
                    "is_comparable": is_comparable,
                    "confidence": confidence_f,
                    "reasoning": reasoning,
                    "rejection_reason": rejection_reason,
                    "model_used": model,
                    "created_by_audit_id": audit_id,
                    "raw_llm_response": verdict,
                })

        # Step 3: persist new verdicts. Hard error on DB failure.
        if new_rows:
            await supabase.insert_pair_verifications(new_rows)

    # Step 4: bump hit_count for cache hits AFTER successful insert of
    # new rows so we don't double-touch on a failed insert path.
    if cached_ids:
        await supabase.touch_pair_verifications(cached_ids)

    # Sanity: any candidate that the LLM silently skipped (no matching
    # verdict by index) gets a permissive default. Better keep + flag
    # than silently drop. This should be exceedingly rare given temp=0.1.
    _missing_count = 0
    for norm in candidate_norms:
        if norm not in results:
            _missing_count += 1
            logger.warning(
                "pair_verification: LLM did not return verdict for "
                "candidate=%r subject=%r — defaulting to comparable=True",
                norm_to_raw.get(norm, norm),
                subject_service_name[:80],
            )
            results[norm] = {
                "is_comparable": True,
                "confidence": 0.0,
                "reasoning": "missing LLM verdict — kept by default",
                "rejection_reason": None,
                "from_cache": False,
                "cached_id": None,
            }

    # P0 silent-fail surfacing (quick 260613-m23): a candidate kept by the
    # permissive default above is a SILENT drop of LLM signal — the row stays
    # comparable=True without the model ever judging it. Aggregate-trace it so
    # the operator sees "subject X had N candidates without a verdict" without
    # grepping per-candidate warnings. Guarded — tracer=None is a no-op.
    if tracer is not None and _missing_count:
        tracer.add(
            "pair_verify.no_verdict",
            {
                "count": _missing_count,
                "subject": subject_service_name[:120],
                "booksy_tid": booksy_treatment_id,
                "synthetic_tid": synthetic_treatment_id,
            },
        )

    return results
