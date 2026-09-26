"""Porównanie wariantów pytań sędziego par na holdoucie (mig 188) — bez zapisu.

Każdy wariant = pytania + reguła decyzji w kodzie. Ocena na podziale:
  * strojenie: pary holdoutu o PARZYSTYM id z etykietą zgodnych sędziów-modeli,
  * sprawdzian: pary o NIEPARZYSTYM id (tych nie oglądamy przy projektowaniu),
  * miara nadrzędna: oceny człowieka (49 par).
Bezpiecznik: liczba par „sędzia: ta sama, a naprawdę różne” musi zostać 0.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/warianty_sedziego.py --out <katalog> [--warianty v1,v2]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from kalibracja_sedziego import KLASY, zgodnosc  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_ocena import pytania as v1  # noqa: E402
from services.typesafe_ocena.pytania import MODEL, stan, strona, werdykt  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Noul, RetryPolicy, Score  # noqa: E402

KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
A, B = "`usluga_a`", "`usluga_b`"

# ── v2: poziom środkowy wymaga NAZWANEJ różnicy; „ta sama” obejmuje różnice
#    pisowni i dopisków. Mechanizm błędu v1: dosłowne czytanie — gdy nazwa nie
#    potwierdza wariantu, model nie dawał „tej samej” nawet identycznym nazwom.
POZIOMY_V2 = [
    "Dwie różne usługi: inny zabieg, inna część ciała albo inny cel — klientka szukająca "
    "jednej nie zamówiłaby w jej miejsce drugiej.",
    "Ten sam rodzaj zabiegu, ale nazwy lub kategorie podają konkretną różnicę, która zmienia to, "
    "co klientka dostaje: inny obszar albo zakres, inną długość, rozmiar lub objętość, inny etap "
    "(np. założenie a uzupełnienie), inną liczbę zabiegów, pakiet albo inną technikę. Także gdy "
    "jedna z nazw jest tak ogólna, że może oznaczać kilka różnych zabiegów.",
    "Ta sama usługa: obie nazwy opisują ten sam zabieg i różnią się tylko pisownią, kolejnością "
    "słów, dopiskami marketingowymi albo szczegółami, które nie zmieniają tego, co klientka dostaje.",
]
REL_V2 = (
    f"Jak mają się do siebie usługa {A} i usługa {B} z cenników dwóch salonów, "
    "biorąc pod uwagę nazwę, kategorię w cenniku i typ salonu?"
)

# Pytania o RÓŻNICĘ zamiast o zgodność: brak informacji = „nie”, a nie „nie wiadomo”.
ROZNICE = {
    "r_obszar": "obszarem ciała albo zakresem (np. twarz a twarz z szyją, jedna partia a całe ciało)",
    "r_rozmiar": "długością, rozmiarem albo objętością (np. włosy krótkie a długie, rzęsy 1:1 a 3D)",
    "r_etap": "etapem (np. pierwsze założenie, uzupełnienie, zdjęcie, korekta)",
    "r_liczba": "liczbą zabiegów w cenie albo tym, że jedna z nich to pakiet",
    "r_technika": "techniką lub metodą wykonania (np. hybryda a żel, laser a wosk)",
}


def pytania_v2(z_roznicami: bool) -> dict[str, Any]:
    q: dict[str, Any] = {"relacja": Score(instructions=REL_V2, criteria=POZIOMY_V2)}
    if z_roznicami:
        for k, opis in ROZNICE.items():
            q[k] = Noul(instructions=f"Czy nazwy lub kategorie wprost wskazują, że {A} i {B} różnią się {opis}?")
    return q


# ── v4: v2 z instrukcjami po angielsku (główny język treningowy Jev), dane po polsku.
LEVELS_V4 = [
    "Two different services: a different treatment, body part or purpose — a client looking for "
    "one would not book the other instead.",
    "The same kind of treatment, but the names or categories state a concrete difference that "
    "changes what the client gets: a different area or scope, length, size or volume, stage (e.g. "
    "first application vs refill), number of sessions, a package, or a different technique. Also "
    "when one of the names is so generic that it could mean several different treatments.",
    "The same service: both names describe the same treatment and differ only in spelling, word "
    "order, marketing additions or details that do not change what the client gets.",
]
REL_V4 = (
    f"How do service {A} and service {B}, taken from the price lists of two beauty salons "
    "(names in Polish), relate to each other, considering the name, the price-list category "
    "and the salon type?"
)


# ── v5: v4 + granice poziomów zgodne z definicją właściciela produktu (ocena
#    Alexa): ten sam rodzaj zabiegu na INNEJ części ciała to usługa powiązana,
#    nie różna; „różne” = inny rodzaj zabiegu. Drobna zmiana materiału, która nie
#    zmienia tego, co dostaje klientka, nie rozdziela usług. Przykłady spoza holdoutu.
LEVELS_V5 = [
    "Two different services: a different kind of treatment or a different purpose, e.g. a haircut "
    "and a hair colouring, a facial peel and a facial massage, laser hair removal and waxing.",
    "Related services that are not the same offer: the same kind of treatment but a different body "
    "area or scope (e.g. waxing legs vs waxing armpits), a different length, size or volume, a "
    "different stage (first application vs refill vs removal), a different number of sessions or a "
    "package, or a clearly different technique. Also when one name is so generic that it could mean "
    "several different treatments.",
    "The same service: both names describe the same treatment for the same area and scope, differing "
    "only in spelling, word order, marketing additions, or details that do not change what the "
    "client gets (e.g. a small variation of the product used for the same procedure).",
]

# ── v6: v5 + pytania o nazwaną różnicę (po angielsku) i reguła „powiązane wymaga
#    różnicy” — jak v3, ale z v5 jako skalą.
DIFFS_EN = {
    "r_obszar": "the body area or scope (e.g. face vs face and neck, one area vs full body)",
    "r_rozmiar": "length, size or volume (e.g. short vs long hair, 1:1 vs 3D lashes)",
    "r_etap": "stage (first application, refill, removal, correction)",
    "r_liczba": "the number of sessions included, or one of them being a package",
    "r_technika": "the technique or method (e.g. gel polish vs gel extension, laser vs wax)",
}


# ── v7/v8: klauzula „nazwa ogólna” tylko gdy nazwy SIĘ RÓŻNIĄ. Mechanizm błędu:
#    dosłowne czytanie — identyczna ogólna nazwa po obu stronach („Kolorowe rzęsy”)
#    spełniała opis poziomu środkowego, choć nie ma między stronami żadnej różnicy.
GENERIC_OLD = ("Also when one of the names is so generic that it could mean several different treatments.",
               "Also when one name is so generic that it could mean several different treatments.")
GENERIC_NEW = ("Also when the two names differ and one of them is so generic that it could mean several "
               "different treatments. Identical names are not a difference.")


def _bez_ogolnej(levels: list[str]) -> list[str]:
    out = []
    for lv in levels:
        for old in GENERIC_OLD:
            lv = lv.replace(old, GENERIC_NEW)
        out.append(lv)
    return out


def pytania_v6() -> dict[str, Any]:
    q: dict[str, Any] = {"relacja": Score(instructions=REL_V4, criteria=LEVELS_V5)}
    for k, opis in DIFFS_EN.items():
        q[k] = Noul(instructions=f"Do the names or categories explicitly show that {A} and {B} differ in {opis}?")
    return q


def decyzja_score(res: Any) -> str:
    return werdykt(res.scores["relacja"].score)


def decyzja_roznice(res: Any) -> str:
    """„Powiązane” wymaga nazwanej różnicy: gdy skala wskazuje środek, ale żadne
    pytanie o różnicę nie przekracza 0,5 (neutralny punkt tak/nie z dokumentacji),
    a skala jest w górnej połowie środka — para jest tą samą usługą."""
    s = res.scores["relacja"].score
    w = werdykt(s)
    if w == "powiazane" and s >= 1.0 and max(res.nouls[k].noul for k in ROZNICE) < 0.5:
        return "tozsame"
    return w


WARIANTY: dict[str, tuple[dict[str, Any], Callable[[Any], str]]] = {
    "v1": (v1.PYTANIA, decyzja_score),
    "v2": (pytania_v2(False), decyzja_score),
    "v3": (pytania_v2(True), decyzja_roznice),
    "v4": ({"relacja": Score(instructions=REL_V4, criteria=LEVELS_V4)}, decyzja_score),
    "v5": ({"relacja": Score(instructions=REL_V4, criteria=LEVELS_V5)}, decyzja_score),
    "v6": (pytania_v6(), decyzja_roznice),
    "v7": ({"relacja": Score(instructions=REL_V4, criteria=_bez_ogolnej(LEVELS_V5))}, decyzja_score),
    "v8": ({"relacja": Score(instructions=REL_V4, criteria=_bez_ogolnej(LEVELS_V4))}, decyzja_score),
}


async def ocen_wariant(client: Any, nazwa: str, strony: list[tuple[dict, dict]]) -> tuple[list[str | None], int]:
    pyt, decyzja = WARIANTY[nazwa]
    sem = asyncio.Semaphore(6)
    tokeny = [0]

    async def jedna(a: dict, b: dict) -> str | None:
        async with sem:
            try:
                res = await client.system_one(stan(a, b), pyt, model=MODEL)
            except Exception as e:  # noqa: BLE001
                print(f"{nazwa}: błąd {type(e).__name__}: {str(e)[:120]}")
                return None
        tokeny[0] += res.usage.input_tokens or 0
        return decyzja(res)

    return await asyncio.gather(*[jedna(a, b) for a, b in strony]), tokeny[0]


def binarnie(pary: list[tuple[str, str]]) -> dict[str, Any]:
    """Miara, która decyduje o cenie: czy para to TA SAMA usługa (tak/nie)."""
    tp = sum(1 for x, s in pary if x == "tozsame" and s == "tozsame")
    fp = sum(1 for x, s in pary if x != "tozsame" and s == "tozsame")
    fn = sum(1 for x, s in pary if x == "tozsame" and s != "tozsame")
    n = max(len(pary), 1)
    return {
        "trafnosc_tak_nie_proc": round((n - fp - fn) / n * 100, 1),
        "wychwycone_tozsame_proc": round(tp / max(tp + fn, 1) * 100, 1),
        "precyzja_tozsame_proc": round(tp / max(tp + fp, 1) * 100, 1),
        "falszywie_tozsame": fp,
    }


def ocena(rows: list[dict], wynik_: list[str | None]) -> dict[str, Any]:
    pary = list(zip(rows, wynik_))
    fin = lambda parz: [(r["label_final"], w) for r, w in pary if w and r.get("label_final") in KLASY and r["id"] % 2 == parz]  # noqa: E731
    czl = [(r["label_human"], w) for r, w in pary if w and r.get("label_human") in KLASY]
    return {
        "bin_sprawdzian": binarnie(fin(1)),
        "bin_czlowiek": binarnie(czl),
        "strojenie_vs_modele": zgodnosc(fin(0)),
        "sprawdzian_vs_modele": zgodnosc(fin(1)),
        "vs_czlowiek": zgodnosc([(r["label_human"], w) for r, w in pary if w and r.get("label_human") in KLASY]),
        "sprawdzian_per_branza": {
            b: zgodnosc([(r["label_final"], w) for r, w in pary if w and r["branza"] == b
                         and r.get("label_final") in KLASY and r["id"] % 2 == 1])["zgodnosc_proc"]
            for b in sorted({r["branza"] for r in rows})
        },
    }


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cli = SupabaseService().client
    rows = cli.table("matching_holdout").select("*").order("id").execute().data or []
    branze = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    bids = sorted({r["cand_booksy_id"] for r in rows if r.get("cand_booksy_id")})
    typ: dict[int, str] = {}
    for i in range(0, len(bids), 200):
        for s in cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", bids[i : i + 200]).execute().data or []:
            typ[s["booksy_id"]] = branze.get(s.get("primary_category_id"), "")
    strony = [(strona(r["subject_name"], r.get("subject_category"), r["branza"]),
               strona(r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), ""))) for r in rows]
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    raport: dict[str, Any] = {}
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        for nazwa in args.warianty.split(","):
            wynik_, tok = await ocen_wariant(client, nazwa, strony)
            raport[nazwa] = {**ocena(rows, wynik_), "koszt_usd": round(tok * 0.042 / 1e6, 4)}
            (out / f"wariant_{nazwa}.json").write_text(json.dumps(
                [{"id": r["id"], "branza": r["branza"], "a": a["nazwa"], "b": b["nazwa"], "human": r.get("label_human"),
                  "final": r.get("label_final"), "sedzia": w} for r, (a, b), w in zip(rows, strony, wynik_)],
                ensure_ascii=False, indent=1), encoding="utf-8")
            k = raport[nazwa]
            bs, bc = k["bin_sprawdzian"], k["bin_czlowiek"]
            print(f"{nazwa}: TAK/NIE sprawdzian {bs['trafnosc_tak_nie_proc']}% (wychwycone {bs['wychwycone_tozsame_proc']}%, "
                  f"precyzja {bs['precyzja_tozsame_proc']}%)  człowiek {bc['trafnosc_tak_nie_proc']}% "
                  f"(wychwycone {bc['wychwycone_tozsame_proc']}%, precyzja {bc['precyzja_tozsame_proc']}%)", flush=True)
            print(f"{nazwa}: strojenie {k['strojenie_vs_modele']['zgodnosc_proc']}%  sprawdzian {k['sprawdzian_vs_modele']['zgodnosc_proc']}%  "
                  f"człowiek {k['vs_czlowiek']['zgodnosc_proc']}%  groźne(sprawdzian/człowiek) "
                  f"{k['sprawdzian_vs_modele']['sedzia_tozsame_a_naprawde_rozne']}/{k['vs_czlowiek']['sedzia_tozsame_a_naprawde_rozne']}  "
                  f"koszt {k['koszt_usd']} USD", flush=True)
    (out / "warianty.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Warianty pytań sędziego par (bez zapisu)")
    p.add_argument("--out", required=True)
    p.add_argument("--warianty", default="v4,v5,v7,v8")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
