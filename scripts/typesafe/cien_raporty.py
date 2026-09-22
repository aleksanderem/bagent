"""Test w cieniu: prawdziwy silnik cen z wetem GLM vs z wetem TypeSafe.

Uruchamia services.similarity_pricing.report_pricing.compute_pricing_comparisons_v2
(ten sam kod co raport) dwa razy na tych samych bliźniakach:
  A) GLM — osie z service_taxonomy, BEZ mostka destylacji (mostek woła GLM
     i ZAPISUJE do bazy; tu wyłączony — test nic nie zapisuje),
  B) TypeSafe — profile z badania (badanie_uslugi + mapa_pytan), brakujące nazwy
     destylowane na żądanie przez SDK, z twardym limitem budżetu; weto liczone
     regułami weto_profil (oś „rozmiar” silnika = objętość/zakres/pakiet).
Porównuje to, co widzi klientka: w ilu wierszach jest cena rynkowa i jak bardzo
przesuwa się mediana.

Podmiotem jest prawdziwy raport (--raporty) albo dowolny salon (--salony
booksy_id:branża) — wtedy wycena „jak do raportu” z konkurentami z promienia.
Tylko odczyt. Budżet: gdy kolejny przebieg się nie mieści, jest pomijany.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/cien_raporty.py --out <katalog> --budzet 3 \\
      --raporty 143,142 --salony 123:Fryzjer
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import statistics as st
import sys
import time
import types
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
for _line in (BAGENT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    _k, _, _v = _line.partition("=")
    if _k.strip() in ("QDRANT_URL", "QDRANT_API_KEY") and _v.strip():
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

import badanie_uslugi  # noqa: E402
import weto_profil  # noqa: E402
from services.similarity_pricing import layer_identity, report_pricing  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

MODEL = "jev-1.13.0"
USD_PER_TOKEN = 0.042 / 1_000_000  # zweryfikowane w konsoli TypeSafe 21.09
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
MAPA = {b: list(k) for b, k in badanie_uslugi.MAPA_PYTAN.items()}

ORIG_LOAD = report_pricing._load_taxonomy_axes
ORIG_VOTE = layer_identity.vote_taxonomy_axis
ORIG_SEARCH = report_pricing.search_twins
ORIG_VECS = report_pricing.fetch_twin_vectors


class BudgetExceeded(Exception):
    pass


class Stan:
    mode = "glm"
    spent_tokens = 0
    budget_tokens = 0
    tokens_per_name = 3100.0  # startowo z pomiaru z mapą; aktualizowane w locie
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    names_meta: dict[str, tuple[str, str | None]] = {}
    glm_found = 0
    glm_needed = 0
    client: AsyncTypeSafeClient | None = None
    memo: dict[Any, Any] = {}


S = Stan()


def nk(name: str | None) -> str:
    return " ".join((name or "").lower().split())


# ── pamięć wyszukiwania: oba przebiegi na tych samych bliźniakach ──────────
def memo_search(subject_ids, pool, **kw):
    key = ("s", tuple(subject_ids), tuple(sorted(pool)), kw.get("min_similarity"), kw.get("limit"))
    if key not in S.memo:
        S.memo[key] = ORIG_SEARCH(subject_ids, pool, **kw)
    return copy.deepcopy(S.memo[key])


def memo_vecs(ids, *a, **kw):
    key = ("v", tuple(ids))
    if key not in S.memo:
        S.memo[key] = ORIG_VECS(ids, *a, **kw)
    return S.memo[key]


# ── źródło osi i weto zależne od trybu ───────────────────────────────────────
def patched_load(service, subject_booksy_id, subject_services, clusters):
    osie, branza = ORIG_LOAD(service, subject_booksy_id, subject_services, clusters)
    names = {nk(s.get("name")) for s in subject_services} | {nk(x.get("service_name")) for lst in clusters.values() for x in lst}
    names.discard("")
    if S.mode == "glm":
        S.glm_needed += len(names)
        S.glm_found += sum(1 for n in names if n in osie)
        return osie, branza
    return {n: S.cache[(branza, n)] for n in names if (branza, n) in S.cache}, branza


async def patched_bridge(service, branza, osie, subject_services, clusters):
    if S.mode == "glm" or not branza:
        return {}  # przebieg GLM: bez mostka (nie woła GLM, nie zapisuje)
    for s in subject_services:
        S.names_meta.setdefault(nk(s.get("name")), (s.get("name") or "", s.get("category_name")))
    for lst in clusters.values():
        for x in lst:
            S.names_meta.setdefault(nk(x.get("service_name")), (x.get("service_name") or "", x.get("category_name")))
    missing = sorted(n for n in S.names_meta if n and (branza, n) not in S.cache and n in _needed(subject_services, clusters))
    if missing and S.spent_tokens + len(missing) * S.tokens_per_name > S.budget_tokens:
        raise BudgetExceeded(f"{len(missing)} nazw nie zmieści się w budżecie")
    await distill(branza, missing)
    return {n: S.cache[(branza, n)] for n in missing if (branza, n) in S.cache}


def _needed(subject_services, clusters) -> set[str]:
    return {nk(s.get("name")) for s in subject_services} | {nk(x.get("service_name")) for lst in clusters.values() for x in lst}


async def distill(branza: str, names: list[str]) -> None:
    sem = asyncio.Semaphore(6)
    allowed = set(MAPA[branza]) if branza in MAPA else None

    async def one(n: str) -> None:
        name, cat = S.names_meta.get(n, (n, None))
        async with sem:
            res = await S.client.system_one(
                badanie_uslugi.build_state(name, cat, branza), badanie_uslugi.build_questions(branza, allowed)
            )
        S.spent_tokens += res.usage.input_tokens or 0
        S.cache[(branza, n)] = badanie_uslugi.profile(res)

    before = S.spent_tokens
    await asyncio.gather(*[one(n) for n in names])
    if names:
        S.tokens_per_name = max(S.tokens_per_name, (S.spent_tokens - before) / len(names))


def patched_vote(subject, sample, axis):
    if S.mode == "glm":
        return ORIG_VOTE(subject, sample, axis)
    a, b = subject.get("_tax"), sample.get("_tax")
    if not a or not b:
        return "abstain"
    if axis in ("obszar", "odbiorca", "etap", "dlugosc", "metoda"):
        return "against" if weto_profil.REGULY[axis](a, b) else "abstain"
    if axis == "rozmiar":
        return "against" if any(weto_profil.REGULY[r](a, b) for r in ("objetosc", "zakres", "pakiet")) else "abstain"
    return "abstain"


report_pricing._load_taxonomy_axes = patched_load
report_pricing._bridge_distill_missing = patched_bridge
report_pricing.search_twins = memo_search
report_pricing.fetch_twin_vectors = memo_vecs
layer_identity.vote_taxonomy_axis = patched_vote


# ── wejście: prawdziwy raport albo dowolny salon ─────────────────────────────
async def inputs_report(sb: SupabaseService, report_id: int):
    rep = sb.client.table("competitor_reports").select("id,convex_audit_id,subject_salon_id").eq("id", report_id).single().execute().data
    try:
        subject = await sb.get_subject_full_data(rep["convex_audit_id"])
    except ValueError:
        # Raporty testowe z 05.2026 (convex_audit_id „test-…”) nie mają już skanu
        # audytu — bierzemy aktualny cennik TEGO SAMEGO salonu, konkurenci z raportu.
        sid = rep["subject_salon_id"]
        bid = (sb.client.table("salons").select("booksy_id").eq("id", sid).single().execute().data or {}).get("booksy_id")
        subject = {**((await sb.get_competitor_full_data([bid])).get(bid) or {}), "booksy_id": bid}
    matches = sb.client.table("competitor_matches").select("competitor_salon_id,counts_in_aggregates").eq("report_id", report_id).execute().data or []
    salons = {s["id"]: s["booksy_id"] for s in (sb.client.table("salons").select("id,booksy_id").in_("id", [m["competitor_salon_id"] for m in matches]).execute().data or [])}
    data = await sb.get_competitor_full_data([b for b in salons.values() if b])
    aligned = []
    for m in matches:
        bid = salons.get(m["competitor_salon_id"])
        if bid and bid in data:
            aligned.append((types.SimpleNamespace(booksy_id=bid, counts_in_aggregates=m.get("counts_in_aggregates", True)), data[bid]))
    return subject, aligned


async def inputs_salon(sb: SupabaseService, booksy_id: int):
    data = (await sb.get_competitor_full_data([booksy_id])).get(booksy_id) or {}
    return {**data, "booksy_id": booksy_id}, []


def compare(rows_a: list[dict[str, Any]], rows_b: list[dict[str, Any]]) -> dict[str, Any]:
    pa = [r.get("market_median_grosze") for r in rows_a]
    pb = [r.get("market_median_grosze") for r in rows_b]
    both = [(a, b) for a, b in zip(pa, pb) if a and b]
    shifts = [abs(b - a) / a * 100 for a, b in both]
    return {
        "wierszy": len(rows_a),
        "z_cena_glm": sum(1 for x in pa if x),
        "z_cena_ts": sum(1 for x in pb if x),
        "cena_tylko_glm": sum(1 for a, b in zip(pa, pb) if a and not b),
        "cena_tylko_ts": sum(1 for a, b in zip(pa, pb) if b and not a),
        "mediana_zmiany_proc": round(st.median(shifts), 1) if shifts else None,
        "zmiana_ponad_10proc": sum(1 for s in shifts if s > 10),
        "zmiana_ponad_25proc": sum(1 for s in shifts if s > 25),
    }


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    S.budget_tokens = int(args.budzet / USD_PER_TOKEN)
    api_key = os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")
    sb = SupabaseService()
    jobs = [("raport", int(x), None) for x in args.raporty.split(",") if x] + [
        ("salon", int(x.split(":")[0]), x.split(":", 1)[1]) for x in args.salony.split(",") if x
    ]
    results = []
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        S.client = client
        for kind, ident, label in jobs:
            t0 = time.monotonic()
            try:
                subject, aligned = await (inputs_report(sb, ident) if kind == "raport" else inputs_salon(sb, ident))
            except Exception as e:  # noqa: BLE001 — brak danych jednego podmiotu nie zatrzymuje reszty
                print(f"BŁĄD WEJŚCIA {kind} {ident}: {type(e).__name__}: {str(e)[:160]}")
                continue
            S.glm_found = S.glm_needed = 0
            try:
                S.mode = "glm"
                rows_a = await report_pricing.compute_pricing_comparisons_v2(sb, ident if kind == "raport" else 0, subject, aligned)
                cov = S.glm_found / max(S.glm_needed, 1)
                S.mode = "ts"
                spent_before = S.spent_tokens
                rows_b = await report_pricing.compute_pricing_comparisons_v2(sb, ident if kind == "raport" else 0, subject, aligned)
            except BudgetExceeded as e:
                print(f"POMINIĘTO {kind} {ident}: {e} — koniec budżetu")
                break
            except Exception as e:  # noqa: BLE001 — błąd jednego przebiegu nie zatrzymuje reszty
                print(f"BŁĄD {kind} {ident}: {type(e).__name__}: {str(e)[:200]}")
                continue
            res = {
                "rodzaj": kind, "id": ident, "etykieta": label,
                "pokrycie_glm_nazw": round(cov, 3),
                "koszt_usd": round((S.spent_tokens - spent_before) * USD_PER_TOKEN, 4),
                "sekundy": round(time.monotonic() - t0, 1),
                **compare(rows_a, rows_b),
            }
            results.append(res)
            (out / f"cien_{kind}_{ident}.json").write_text(
                json.dumps({"wynik": res, "glm": rows_a, "ts": rows_b}, ensure_ascii=False, default=str), encoding="utf-8"
            )
            print(json.dumps(res, ensure_ascii=False))
    total = S.spent_tokens * USD_PER_TOKEN
    (out / "cien_podsumowanie.json").write_text(json.dumps({"przebiegi": results, "wydano_usd": round(total, 4)}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nwydano łącznie: {total:.4f} USD z limitu {args.budzet} USD")


def main() -> None:
    p = argparse.ArgumentParser(description="Test w cieniu: silnik cen GLM vs TypeSafe (tylko odczyt)")
    p.add_argument("--out", required=True)
    p.add_argument("--budzet", type=float, default=3.0)
    p.add_argument("--raporty", default="")
    p.add_argument("--salony", default="")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
