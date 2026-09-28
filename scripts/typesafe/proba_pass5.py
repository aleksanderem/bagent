"""Próba TypeSafe na Pass 5 — jedna kategoria Booksy dla grupy podobnych usług.

Po co: Pass 5 prosi dziś gpt-4o o JEDNĄ listę decyzji dla nawet 30 grup naraz
i parsuje odpowiedź. Model może pominąć grupy (pada cały etap raportu) albo
wskazać kategorię spoza listy kandydatów (przechodzi po cichu, bd sdx4).
TypeSafe zwraca wybór wyłącznie spośród podanych opcji, z rozkładem
prawdopodobieństwa — jedna decyzja na grupę, osobnym wywołaniem.

Tylko odczyt wobec prod Supabase. Wejście Pass 5 odtwarzane jest tą samą
ścieżką co w apply_intra_salon_consistency (build_clusters →
find_mixed_clusters → _hydrate_reference_embeddings →
match_taxonomy_candidates → filter_candidates_by_area). Skrypt NIE woła
gpt-4o ani MiniMaksa i NIE zapisuje kotwic ani syntetyków. Ładowanie usług
raportu przeniesione ze scripts/ab_pass5.py (worktree ab-bagent,
BEAUTY_AUDIT-qqsv.2).

Wzorzec do porównania: kotwice z last_audit_id = audyt raportu. Tylko decyzje
zapisane po POPRAWKA_TFYB zapadły Z listą kandydatów — starsze pochodzą
z pamięci modelu (bd 2y75) i są oznaczane jako nieważny wzorzec.

Do TypeSafe trafiają wyłącznie publiczne dane cenników Booksy: nazwy usług
i kategorii z cennika, znaczniki marki/metody/okolic oraz nazwy kategorii
Booksy. Bez nazw salonów, identyfikatorów i danych klientów.

Użycie:
  /Users/alex/Desktop/MOJE_PROJEKTY/bagent/.venv/bin/python \\
    /Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts/typesafe/proba_pass5.py \\
    --report-id 250 --max-calls 40 --out /tmp/proba_pass5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

BAGENT_ROOT = Path(__file__).resolve().parents[2]
# config.Settings szuka .env względem bieżącego katalogu.
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pass5_v2  # noqa: E402
from services.body_area_taxonomy import filter_candidates_by_area  # noqa: E402
from services.hidden_service_inference import match_taxonomy_candidates  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.taxonomy_consistency import (  # noqa: E402
    _hydrate_reference_embeddings,
    build_clusters,
    find_mixed_clusters,
)

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("proba_pass5")

TYPESAFE_URL = "https://api.typesafe.ai/v1/systemone"
TYPESAFE_MODEL = "jev-latest"
# docs.typesafe.ai/models (2026-09-21): 0,042 USD / 1M tokenów wejścia, wyjście darmowe.
USD_PER_M_INPUT = 0.042
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"

# Pass 5 pokazuje modelowi 15 pierwszych kandydatów (_format_cluster_for_prompt).
MAX_CANDIDATES = 15
MAX_MEMBERS = 40
OWN_CATEGORY = "wlasna_kategoria"
POPRAWKA_TFYB = datetime(2026, 9, 15, 17, 56, 41, tzinfo=timezone.utc)

INSTRUCTIONS = (
    "Wybierz jedną kategorię Booksy dla całej grupy usług `grupa`. Usługi w grupie "
    "pochodzą z cenników salonu i jego konkurencji; łączy je ta sama marka urządzenia, "
    "metoda i okolice ciała. Kategoria musi pasować JEDNOCZEŚNIE metodą i okolicą "
    "ciała — laser, wosk, pasta cukrowa, RF, HIFU i mezoterapia to różne metody. "
    "Kategoria szczegółowa jest lepsza od ogólnej, o ile pasuje metodą i okolicą. "
    "Jeśli żadna kategoria nie pasuje jednocześnie metodą i okolicą ciała, wybierz "
    "własną kategorię salonu."
)
OWN_CATEGORY_DESCRIPTION = (
    "Żadna z kategorii Booksy nie pasuje jednocześnie metodą i okolicą ciała — "
    "grupa potrzebuje własnej kategorii salonu"
)

ClusterKey = tuple[str | None, str, tuple[str, ...]]
Payload = tuple[int, ClusterKey, list[dict[str, Any]], list[dict[str, Any]]]


def read_api_key() -> str:
    key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    if not key and KEY_FILE.exists():
        key = KEY_FILE.read_text(encoding="utf-8").strip()
    if not key:
        raise SystemExit(f"Brak klucza TypeSafe: ustaw TYPESAFE_API_KEY albo zapisz go w {KEY_FILE}")
    return key


# ---------------------------------------------------------------------------
# Wejście Pass 5 (tylko odczyt)
# ---------------------------------------------------------------------------

async def load_report_services(supabase: SupabaseService, report_id: int) -> tuple[str, list[dict[str, Any]]]:
    """Usługi salonu + konkurentów raportu, jak w pipelines/competitor_analysis.py."""
    rep = (
        supabase.client.table("competitor_reports")
        .select("id,convex_audit_id")
        .eq("id", report_id)
        .single()
        .execute()
        .data
    )
    audit_id = rep["convex_audit_id"]
    subject = await supabase.get_subject_full_data(audit_id)
    services: list[dict[str, Any]] = list(subject.get("services") or [])
    matches = (
        supabase.client.table("competitor_matches")
        .select("competitor_salon_id")
        .eq("report_id", report_id)
        .execute()
        .data
        or []
    )
    salon_ids = [m["competitor_salon_id"] for m in matches]
    salons = supabase.client.table("salons").select("id,booksy_id").in_("id", salon_ids).execute().data or []
    booksy_ids = [s["booksy_id"] for s in salons if s.get("booksy_id")]
    competitors = await supabase.get_competitor_full_data(booksy_ids)
    for bid in booksy_ids:
        services = services + list((competitors.get(bid) or {}).get("services") or [])
    return audit_id, services


async def build_payloads(supabase: SupabaseService, services: list[dict[str, Any]]) -> list[Payload]:
    """Ta sama ścieżka co apply_intra_salon_consistency, bez wywołania modelu i bez zapisu."""
    mixed = find_mixed_clusters(build_clusters(services))
    await _hydrate_reference_embeddings(mixed, supabase=supabase, label="proba_typesafe")
    payloads: list[Payload] = []
    for cid, (key, members) in enumerate(mixed, start=1):
        ref = members[0]
        candidates: list[dict[str, Any]] = []
        if ref.get("name_embedding"):
            raw = await match_taxonomy_candidates(supabase, ref["name_embedding"], top_k=30)
            candidates, _, _ = filter_candidates_by_area(ref.get("name") or "", raw)
        payloads.append((cid, key, members, candidates[:MAX_CANDIDATES]))
    return payloads


def load_reference(supabase: SupabaseService, audit_id: str) -> dict[tuple[str | None, str, str], dict[str, Any]]:
    rows = (
        supabase.client.table("taxonomy_consistency_anchors")
        .select("brand_marker,method_marker,body_area_set,tid_kind,booksy_tid,synthetic_canonical_name,updated_at")
        .eq("last_audit_id", audit_id)
        .execute()
        .data
        or []
    )
    return {(r["brand_marker"] or None, r["method_marker"], r["body_area_set"] or ""): r for r in rows}


def anchor_key(key: ClusterKey) -> tuple[str | None, str, str]:
    brand, method, areas = key
    return (brand or None, method, ",".join(areas))


# ---------------------------------------------------------------------------
# TypeSafe
# ---------------------------------------------------------------------------

def describe_candidate(c: dict[str, Any]) -> str:
    parent = c.get("parent_canonical_name")
    return f"{c['canonical_name']} (dział: {parent})" if parent else str(c["canonical_name"])


def build_request(key: ClusterKey, members: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    brand, method, areas = key
    state = {
        "grupa": {
            "marka_urzadzenia": brand or "brak",
            "metoda": method,
            "okolice_ciala": list(areas) or ["brak"],
            "uslugi": [
                {"nazwa": m.get("name") or "", "kategoria_w_cenniku": m.get("category_name") or ""}
                for m in members[:MAX_MEMBERS]
            ],
        }
    }
    criteria = {f"tid_{c['tid']}": describe_candidate(c) for c in candidates}
    criteria[OWN_CATEGORY] = OWN_CATEGORY_DESCRIPTION
    return {
        "model": TYPESAFE_MODEL,
        "state": state,
        "questions": {"kategoria": {"type": "choice", "instructions": INSTRUCTIONS, "criteria": criteria}},
    }


async def ask_typesafe(client: httpx.AsyncClient, request: dict[str, Any]) -> dict[str, Any]:
    """Jedno wywołanie; ponowienie tylko przy 429/529 (limit / przeciążenie)."""
    for attempt in range(3):
        resp = await client.post(TYPESAFE_URL, json=request)
        if resp.status_code in (429, 529) and attempt < 2:
            await asyncio.sleep(2 ** (attempt + 1))
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("TypeSafe: wyczerpane ponowienia")


# ---------------------------------------------------------------------------
# Porównanie
# ---------------------------------------------------------------------------

def compare(choice: str, ref: dict[str, Any] | None, candidate_tids: set[int]) -> dict[str, Any]:
    if ref is None:
        return {"wzorzec": "brak"}
    updated = datetime.fromisoformat(str(ref["updated_at"]).replace("Z", "+00:00"))
    valid = updated >= POPRAWKA_TFYB
    if ref["tid_kind"] == "booksy":
        ref_choice = f"tid_{ref['booksy_tid']}"
        outside = ref["booksy_tid"] not in candidate_tids
    else:
        ref_choice = OWN_CATEGORY
        outside = False
    if choice == ref_choice:
        verdict = "zgodne"
    elif choice == OWN_CATEGORY:
        verdict = "TypeSafe: własna / wzorzec: Booksy"
    elif ref_choice == OWN_CATEGORY:
        verdict = "TypeSafe: Booksy / wzorzec: własna"
    else:
        verdict = "obie Booksy, inna kategoria"
    return {
        "wzorzec": ref_choice,
        "wzorzec_nazwa": ref.get("synthetic_canonical_name"),
        "wzorzec_wazny": valid,
        "wzorzec_spoza_listy": outside,
        "werdykt": verdict,
    }


async def run(args: argparse.Namespace) -> int:
    supabase = SupabaseService()
    audit_id, services = await load_report_services(supabase, args.report_id)
    payloads = await build_payloads(supabase, services)
    reference = load_reference(supabase, audit_id)

    with_candidates = [p for p in payloads if p[3]]
    # Najpierw grupy, dla których jest wzorzec — tylko tam da się porównać.
    ordered = sorted(with_candidates, key=lambda p: anchor_key(p[1]) not in reference)
    planned = ordered[: args.max_calls]
    print(
        f"raport {args.report_id}: usług {len(services)}, grup do rozstrzygnięcia {len(payloads)}, "
        f"z kandydatami {len(with_candidates)}, bez kandydatów {len(payloads) - len(with_candidates)}, "
        f"kotwic-wzorców {len(reference)} → wywołań TypeSafe: {len(planned)}"
    )

    sem = asyncio.Semaphore(args.concurrency)
    headers = {"Authorization": f"Bearer {read_api_key()}"}

    async with httpx.AsyncClient(headers=headers, timeout=60.0) as client:

        async def one(p: Payload) -> dict[str, Any]:
            cid, key, members, candidates = p
            row: dict[str, Any] = {
                "grupa": cid,
                "marka": key[0],
                "metoda": key[1],
                "okolice": list(key[2]),
                "liczba_uslug": len(members),
                "przyklady": [m.get("name") for m in members[:5]],
                "kandydaci": {f"tid_{c['tid']}": c["canonical_name"] for c in candidates},
            }
            if args.wersja == "v2":
                state, has_method, has_area = pass5_v2.build_state(key, members, args.jezyk, MAX_MEMBERS)
                request = {
                    "model": TYPESAFE_MODEL,
                    "state": state,
                    "questions": pass5_v2.build_questions(candidates, args.jezyk, has_method, has_area),
                }
            else:
                request = build_request(key, members, candidates)
            t0 = time.monotonic()
            async with sem:
                try:
                    resp = await ask_typesafe(client, request)
                except Exception as e:  # noqa: BLE001 — każdy błąd ma trafić do raportu
                    return {**row, "blad": f"{type(e).__name__}: {str(e)[:300]}"}
            if args.wersja == "v2":
                decision = pass5_v2.decide(resp["answers"], candidates)
                extra = {k: v for k, v in decision.items() if k not in ("wybor", "pewnosc")}
                choice, certainty = decision["wybor"], decision["pewnosc"]
            else:
                answer = resp["answers"]["kategoria"]
                extra = {"top3": sorted(answer["probabilities"].items(), key=lambda kv: -kv[1])[:3]}
                choice, certainty = answer["choice"], answer["confidence"]
            return {
                **row,
                "sekundy": round(time.monotonic() - t0, 2),
                "pytan": len(request["questions"]),
                "wybor": choice,
                "wybor_nazwa": row["kandydaci"].get(choice, "WŁASNA KATEGORIA"),
                "pewnosc": certainty,
                **extra,
                "tokeny": resp.get("usage", {}).get("input_tokens", 0),
                **compare(choice, reference.get(anchor_key(key)), {c["tid"] for c in candidates}),
            }

        rows = await asyncio.gather(*[one(p) for p in planned])

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tag = f"{args.report_id}" if args.wersja == "v1" else f"{args.report_id}_{args.wersja}_{args.jezyk}"
    (out / f"wiersze_{tag}.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8"
    )
    summary = summarize(rows)
    (out / f"podsumowanie_{tag}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"szczegóły: {out}/wiersze_{tag}.jsonl")
    return 0


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [r for r in rows if "blad" not in r]
    compared = [r for r in ok if r.get("wzorzec") not in (None, "brak") and r.get("wzorzec_wazny")]
    agree = [r for r in compared if r["werdykt"] == "zgodne"]
    disagree = [r for r in compared if r["werdykt"] != "zgodne"]
    verdicts: dict[str, int] = {}
    for r in compared:
        verdicts[r["werdykt"]] = verdicts.get(r["werdykt"], 0) + 1
    tokens = sum(r.get("tokeny", 0) for r in ok)

    def mean(xs: list[float]) -> float | None:
        return round(sum(xs) / len(xs), 3) if xs else None

    return {
        "wywolan": len(rows),
        "bledow": len(rows) - len(ok),
        "typesafe_wybral_wlasna": sum(1 for r in ok if r["wybor"] == OWN_CATEGORY),
        "typesafe_wybral_booksy": sum(1 for r in ok if r["wybor"] != OWN_CATEGORY),
        "porownanych_z_waznym_wzorcem": len(compared),
        "zgodnych": len(agree),
        "zgodnosc_proc": round(100 * len(agree) / len(compared), 1) if compared else None,
        "rozbicie": verdicts,
        "pewnosc_srednia_zgodne": mean([r["pewnosc"] for r in agree]),
        "pewnosc_srednia_niezgodne": mean([r["pewnosc"] for r in disagree]),
        "wzorzec_spoza_listy_kandydatow": sum(1 for r in compared if r.get("wzorzec_spoza_listy")),
        "tokeny_wejscia": tokens,
        "koszt_usd": round(tokens * USD_PER_M_INPUT / 1_000_000, 5),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Próba TypeSafe na Pass 5 (tylko odczyt)")
    p.add_argument("--report-id", type=int, required=True)
    p.add_argument("--max-calls", type=int, default=40, help="twardy limit wywołań TypeSafe")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--out", default="/tmp/proba_pass5")
    p.add_argument("--wersja", choices=("v1", "v2"), default="v1", help="v1 = jedno pytanie; v2 = pass5_v2.py")
    p.add_argument("--jezyk", choices=("pl", "en"), default="pl", help="język instrukcji v2 (dane zawsze po polsku)")
    sys.exit(asyncio.run(run(p.parse_args())))


if __name__ == "__main__":
    main()
