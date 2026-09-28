"""Tańsza destylacja bez utraty trafności? Cztery warianty wyboru rodzaju zabiegu (bd BEAUTY_AUDIT-asrk).

Koszt destylacji v8 to w 81% listy opcji (dziedzina, grupa zabiegów, odmiana) wysyłane
w całości przy każdej usłudze. Warianty różnią się TYLKO tym, jak podają te listy;
cechy i pytania o zestaw/dodatek są wszędzie takie same:
  A  pełne opisy (dziś): dziedzina z 15 przykładami, grupa z 3 przykładami i listą odmian,
  D  chude opisy: dziedzina z 3 przykładami, grupa i odmiana z 1 przykładem, bez listy odmian,
  E  same nazwy opcji,
  C  paczki po 25 usług: pełne opisy RAZ w danych wywołania, w pytaniach same nazwy.

Miara: te same pary (usługa salonu, kandydat z dzisiejszej sieci: 80 najbliższych, ≥ 0,68),
decyzja kodem (podzial.porownaj_v8) na destylacjach danego wariantu, sędzia par ocenia
WSZYSTKIE pary jako miernik. Per wariant: koszt na usługę, precyzja i czułość „ta sama”
względem sędziego, osobno per branża. Salony nowe — inne niż w pomiarach 25 i 26.09.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/warianty_destylacji.py --salonow 10 --uslug 7 --budzet 2.8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ocena_trafnosci as ot  # noqa: E402  (katalog, Qdrant, wyłączony pomost GLM)
import siec_kandydatow as sk  # noqa: E402

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import (  # noqa: E402
    BEAM, MIN_KRAWEDZ, OGOLNIE, WERSJA, cechy_v8, dodatki_v8, dzieci, jako_schemat, porownaj_v8, rodzaj_v8,
)
from services.typesafe_drzewo.schemat import MODEL, liczby, stan  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, RetryPolicy  # noqa: E402

SEED = 20260927
NET = (80, 0.68)
PACZKA = 25
CENA_TOK = 0.042 / 1e6
ROWNOLEGLE = 30
WARIANTY = ("A", "D", "E", "C")
OPIS = {"A": "pełne opisy (dziś)", "D": "chude opisy (1 przykład)", "E": "same nazwy", "C": "paczki po 25, opisy raz"}
OUT = sk.DANE / "2026-09-27" / "warianty"
CACHE_A = sk.DANE / "2026-09-26" / "siec" / "destylacje.json"  # ten sam algorytm co A, bez opisu


# ---------- listy opcji w trzech odchudzeniach ----------
def _przyklady(podzial: dict, r: str, n: int) -> list[str]:
    return podzial["przyklady"].get(r, [])[:n]


def q_dziedzina(podzial: dict, w: str, u: str = "usluga") -> Choice:
    instr = f"Do jakiej dziedziny usług należy zabieg `{u}`? Rozstrzyga nazwa i opis, nie typ salonu."
    branze = {b: sorted(rs, key=lambda r: -podzial["liczn"].get(r, 0)) for b, rs in podzial["branze"].items() if rs}
    if w == "E":
        return Choice(instructions=instr, criteria={b: None for b in branze})
    n = 3 if w == "D" else 15
    return Choice(instructions=instr, criteria={b: {"examples": rs[:n]} for b, rs in branze.items()})


def korzenie(podzial: dict, dziedzina: str) -> list[str]:
    return sorted(podzial["branze"][dziedzina], key=lambda r: -podzial["liczn"].get(r, 0))[:240]


def q_korzen(podzial: dict, dz: dict, dziedzina: str, w: str, u: str = "usluga") -> Choice:
    instr = (f"Jaki zabieg wykonuje się w usłudze `{u}`? Wybierz najbliższą grupę zabiegów"
             + ("; odmiany grupy są wymienione przy niej." if w == "A" else "."))
    inny = {"inny": {"what": "zabieg spoza tej listy"}}
    rs = korzenie(podzial, dziedzina)
    if w == "E":
        return Choice(instructions=instr, criteria={**{r: None for r in rs}, **inny})
    if w == "D":
        return Choice(instructions=instr, criteria={**{r: {"examples": _przyklady(podzial, r, 1)} for r in rs}, **inny})
    return Choice(instructions=instr, criteria={**{r: ({"examples": _przyklady(podzial, r, 3)}
                                                        | ({"odmiany": dz[r][:12]} if dz.get(r) else {})) for r in rs}, **inny})


def q_odmiana(podzial: dict, korzen: str, odm: list[str], w: str, u: str = "usluga") -> Choice:
    instr = (f"Którą odmianą zabiegu „{korzen}” jest usługa `{u}`? "
             "Jeśli nazwa i opis nie mówią, o którą odmianę chodzi, wybierz „ogólnie”.")
    og = {OGOLNIE: {"what": f"{korzen} bez wskazania odmiany", "not_for": "nazwa lub opis wprost wskazują jedną z odmian"}}
    if w == "E":
        return Choice(instructions=instr, criteria={**{o: None for o in odm[:240]}, **og})
    n = 1 if w == "D" else 3
    return Choice(instructions=instr, criteria={**{o: {"examples": _przyklady(podzial, o, n)} for o in odm[:240]}, **og})


def _sciezki(dzd: list[tuple[str, float]], wyb: dict[str, tuple[str, float]]) -> tuple[str, float, list]:
    sc = []
    for b, p in dzd:
        if b in wyb:
            ch, pr = wyb[b]
            sc.append([b, ch, round((p * pr) ** 0.5, 3)])
    sc.sort(key=lambda x: (x[1] == "inny", -x[2]))
    return (sc[0][1], sc[0][2], sc[:3]) if sc else ("inny", 0.0, [])


# ---------- warianty A/D/E: jedna usługa na wywołanie ----------
async def rodzaj_chudy(client: Any, st: dict, podzial: dict, dz: dict, w: str, tok: list) -> dict:
    r1 = await client.system_one(st, {"d": q_dziedzina(podzial, w)}, model=MODEL)
    tok[0] += r1.usage.input_tokens or 0
    dzd = sorted(r1.choices["d"].probabilities.items(), key=lambda kv: -kv[1])[:BEAM]
    q = {f"k|{b}": q_korzen(podzial, dz, b, w) for b, p in dzd if p >= MIN_KRAWEDZ}
    r2 = await client.system_one(st, q, model=MODEL)
    tok[0] += r2.usage.input_tokens or 0
    wyb = {k.split("|", 1)[1]: (a.choice, a.probabilities.get(a.choice, 0.0)) for k, a in r2.choices.items()}
    korzen, pewnosc, sciezki = _sciezki(dzd, wyb)
    rodzaj, p_odm = korzen, None
    if korzen in dz:
        r3 = await client.system_one(st, {"o": q_odmiana(podzial, korzen, dz[korzen], w)}, model=MODEL)
        tok[0] += r3.usage.input_tokens or 0
        o = r3.choices["o"]
        p_odm = round(o.probabilities.get(o.choice, 0.0), 3)
        if o.choice != OGOLNIE and p_odm >= 0.5:
            rodzaj = o.choice
    return {"rodzaj": rodzaj, "korzen": korzen, "pewnosc": pewnosc, "p_odmiany": p_odm, "sciezki": sciezki}


# ---------- wariant C: paczki, opisy raz w danych wywołania ----------
async def rodzaje_paczkami(client: Any, uslugi: dict[str, dict], podzial: dict, dz: dict, tok: list) -> dict[str, dict]:
    klucze = list(uslugi)
    sem = asyncio.Semaphore(ROWNOLEGLE)
    branze = {b: sorted(rs, key=lambda r: -podzial["liczn"].get(r, 0)) for b, rs in podzial["branze"].items() if rs}

    async def wywolaj(state: dict, q: dict) -> Any:
        async with sem:
            r = await client.system_one(state, q, model=MODEL)
        tok[0] += r.usage.input_tokens or 0
        return r

    # 1. dziedzina
    dzd: dict[str, list] = {}

    async def p1(ks: list[str]) -> None:
        state = {"uslugi": [uslugi[k]["usluga"] for k in ks], "dziedziny": {b: {"examples": rs[:15]} for b, rs in branze.items()}}
        q = {f"d{i}": Choice(instructions=f"Do jakiej dziedziny usług należy zabieg `uslugi[{i}]`? Rozstrzyga nazwa i opis, "
                                          "nie typ salonu. Przykłady zabiegów każdej dziedziny są w `dziedziny`.",
                             criteria={b: None for b in branze}) for i in range(len(ks))}
        r = await wywolaj(state, q)
        for i, k in enumerate(ks):
            dzd[k] = sorted(r.choices[f"d{i}"].probabilities.items(), key=lambda kv: -kv[1])[:BEAM]

    await asyncio.gather(*[p1(klucze[i:i + PACZKA]) for i in range(0, len(klucze), PACZKA)])

    # 2. grupa zabiegów — paczki w obrębie jednej dziedziny, opisy tej dziedziny w danych
    wyb: dict[str, dict] = defaultdict(dict)
    po_dz: dict[str, list[str]] = defaultdict(list)
    for k, lst in dzd.items():
        for b, p in lst:
            if p >= MIN_KRAWEDZ:
                po_dz[b].append(k)

    async def p2(b: str, ks: list[str]) -> None:
        rs = korzenie(podzial, b)
        state = {"uslugi": [uslugi[k]["usluga"] for k in ks],
                 "grupy_zabiegow": {r: {"examples": _przyklady(podzial, r, 3)} | ({"odmiany": dz[r][:12]} if dz.get(r) else {}) for r in rs}}
        q = {f"k{i}": Choice(instructions=f"Jaki zabieg wykonuje się w usłudze `uslugi[{i}]`? Wybierz najbliższą grupę zabiegów. "
                                          "Opis, przykłady i odmiany każdej grupy są w `grupy_zabiegow`.",
                             criteria={**{r: None for r in rs}, "inny": {"what": "zabieg spoza tej listy"}}) for i in range(len(ks))}
        r = await wywolaj(state, q)
        for i, k in enumerate(ks):
            a = r.choices[f"k{i}"]
            wyb[k][b] = (a.choice, a.probabilities.get(a.choice, 0.0))

    await asyncio.gather(*[p2(b, ks[i:i + PACZKA]) for b, ks in po_dz.items() for i in range(0, len(ks), PACZKA)])
    wynik: dict[str, dict] = {}
    for k in klucze:
        korzen, pewnosc, sciezki = _sciezki(dzd.get(k, []), wyb.get(k, {}))
        wynik[k] = {"rodzaj": korzen, "korzen": korzen, "pewnosc": pewnosc, "p_odmiany": None, "sciezki": sciezki}

    # 3. odmiana — paczki w obrębie jednego korzenia
    po_k: dict[str, list[str]] = defaultdict(list)
    for k, w in wynik.items():
        if w["korzen"] in dz:
            po_k[w["korzen"]].append(k)

    async def p3(korzen: str, ks: list[str]) -> None:
        odm = dz[korzen][:240]
        state = {"uslugi": [uslugi[k]["usluga"] for k in ks], "odmiany": {o: {"examples": _przyklady(podzial, o, 3)} for o in odm}}
        q = {f"o{i}": Choice(instructions=f"Którą odmianą zabiegu „{korzen}” jest usługa `uslugi[{i}]`? Jeśli nazwa i opis nie mówią, "
                                          "o którą odmianę chodzi, wybierz „ogólnie”. Przykłady odmian są w `odmiany`.",
                             criteria={**{o: None for o in odm}, OGOLNIE: {"what": f"{korzen} bez wskazania odmiany",
                                                                           "not_for": "nazwa lub opis wprost wskazują jedną z odmian"}})
             for i in range(len(ks))}
        r = await wywolaj(state, q)
        for i, k in enumerate(ks):
            o = r.choices[f"o{i}"]
            p = round(o.probabilities.get(o.choice, 0.0), 3)
            wynik[k]["p_odmiany"] = p
            if o.choice != OGOLNIE and p >= 0.5:
                wynik[k]["rodzaj"] = o.choice

    await asyncio.gather(*[p3(kz, ks[i:i + PACZKA]) for kz, ks in po_k.items() for i in range(0, len(ks), PACZKA)])
    return wynik


# ---------- destylacja wariantu (cechy i zestaw/dodatek identyczne dla wszystkich) ----------
async def destyluj(client: Any, w: str, uslugi: dict[str, tuple], podzial: dict, dz: dict, pamiec: dict, tok_r: list, tok_c: list) -> float:
    brak = [k for k in uslugi if k not in pamiec]
    stany = {k: stan(*uslugi[k], None, None) for k in brak}
    t0 = time.monotonic()
    sem = asyncio.Semaphore(ROWNOLEGLE)
    rodzaje: dict[str, dict] = {}
    if w == "C":
        rodzaje = await rodzaje_paczkami(client, stany, podzial, dz, tok_r)
    else:
        async def jr(k: str) -> None:
            async with sem:
                try:
                    rodzaje[k] = (await rodzaj_v8(client, stany[k], podzial, dz, tok_r) if w == "A"
                                  else await rodzaj_chudy(client, stany[k], podzial, dz, w, tok_r))
                except Exception as e:  # noqa: BLE001
                    print(f"  {w} rodzaj {k[:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
        await asyncio.gather(*[jr(k) for k in brak])

    async def jc(k: str) -> None:
        r = rodzaje.get(k)
        if not r:
            return
        async with sem:
            try:
                cechy, dod = await asyncio.gather(cechy_v8(client, stany[k], r["rodzaj"], podzial, tok_c),
                                                  dodatki_v8(client, stany[k], tok_c))
            except Exception as e:  # noqa: BLE001
                print(f"  {w} cechy {k[:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
                return
        pamiec[k] = {"wersja": WERSJA, **r, "cechy": {r["rodzaj"]: cechy}, **dod, **liczby(uslugi[k][0], None)}

    await asyncio.gather(*[jc(k) for k in brak])
    return time.monotonic() - t0


def metryki(pary: list[dict], w: str) -> dict:
    z = [p for p in pary if p["sedzia"] in ("tozsame", "powiazane", "rozne") and p[w]]
    tp = sum(1 for p in z if p[w] == "tozsame" and p["sedzia"] == "tozsame")
    fp = sum(1 for p in z if p[w] == "tozsame" and p["sedzia"] != "tozsame")
    fn = sum(1 for p in z if p[w] != "tozsame" and p["sedzia"] == "tozsame")
    n = max(len(z), 1)
    return {"par": len(z), "precyzja_ta_sama_proc": round(tp / max(tp + fp, 1) * 100, 1),
            "czulosc_ta_sama_proc": round(tp / max(tp + fn, 1) * 100, 1),
            "trafnosc_proc": round((len(z) - fp - fn) / n * 100, 1), "mowi_ta_sama": tp + fp}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    branze_nazwy = ot._nazwy_branz(cli)
    pomin = sk.uzyte_salony()
    siec_sal = sk.DANE / "2026-09-26" / "siec" / "salony.json"
    if siec_sal.exists():
        pomin |= {b for _br, b, _m in json.loads(siec_sal.read_text(encoding="utf-8"))}
    salony = await sk.losuj(sb, a.salonow, random.Random(SEED), pomin)
    (OUT / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print(f"wylosowano {len(salony)} (pominięto {len(pomin)} użytych wcześniej): " + ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)

    # zbiór usług i par — wspólny dla wszystkich wariantów
    rng = random.Random(SEED)
    do_dest: dict[str, tuple] = {}
    pary: list[dict] = []
    for branza, bid, miasto, dane in salony:
        uslugi = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        uslugi, _ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, uslugi, [int(s["id"]) for s in uslugi], bid)
        uslugi = [s for s in uslugi if int(s["id"]) in emb]
        if not uslugi:
            continue
        uslugi = rng.sample(uslugi, min(a.uslug, len(uslugi)))
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, sk.PROMIEN_KM) if b != bid]
        kand = search_twins([int(s["id"]) for s in uslugi], pula, subject_embeddings=emb, limit=NET[0], min_similarity=NET[1], exact=True)
        typy = ot._typy_salonow(cli, {c["booksy_id"] for cs in kand.values() for c in cs} | {bid}, branze_nazwy)
        for s in uslugi:
            ka = sk.klucz(s.get("name"), s.get("category_name"), typy.get(bid))
            do_dest[ka] = (s.get("name") or "", s.get("category_name"), typy.get(bid))
            for c in kand.get(int(s["id"]), []):
                t = typy.get(c["booksy_id"])
                kb = sk.klucz(c["service_name"], c.get("category_name"), t)
                do_dest[kb] = (c["service_name"], c.get("category_name"), t)
                pary.append({"branza": branza, "salon": bid, "ka": ka, "kb": kb, "a": strona(s.get("name"), s.get("category_name"), typy.get(bid)),
                             "b": strona(c["service_name"], c.get("category_name"), t), "cand_salon": c["booksy_id"], "sim": c["similarity"],
                             "fa": {"nazwa": s.get("name"), "price_grosze": s.get("price_grosze"), "duration_minutes": s.get("duration_minutes"), "is_package": s.get("is_package")},
                             "fb": {"nazwa": c["service_name"], "price_grosze": c.get("price_grosze"), "duration_minutes": c.get("duration_minutes"), "is_package": c.get("is_package")}})
        print(f"  {branza:<20} {miasto:<18} pula {len(pula):>5} | usług {len(uslugi)} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    print(f"usług do destylacji: {len(do_dest)}, par: {len(pary)}", flush=True)

    podzial = json.loads((sk.DANE / "2026-09-25" / "podzial.json").read_text(encoding="utf-8"))
    dz = dzieci(podzial)
    sch8 = jako_schemat(podzial)
    api_key = (os.environ.get("TYPESAFE_API_KEY") or ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    cache_a = json.loads(CACHE_A.read_text(encoding="utf-8")) if CACHE_A.exists() else {}
    pamieci = {w: {} for w in WARIANTY}
    pamieci["A"] = {k: cache_a[k] for k in do_dest if k in cache_a}
    koszt: dict[str, dict] = {}
    wydane = 0.0
    szac = {"A": 7400, "D": 5600, "E": 3700, "C": 2500}  # tok./usługę, z pomiarów 26–27.09
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        for w in WARIANTY:
            brak = sum(1 for k in do_dest if k not in pamieci[w])
            s = brak * szac[w] * CENA_TOK
            if wydane + s > a.budzet:
                print(f"STOP budżetu przed {w}: szac. {s:.2f} USD, wydane {wydane:.2f} z {a.budzet:.2f}", flush=True)
                break
            tok_r, tok_c = [0], [0]
            sek = await destyluj(client, w, do_dest, podzial, dz, pamieci[w], tok_r, tok_c)
            usd = (tok_r[0] + tok_c[0]) * CENA_TOK
            wydane += usd
            koszt[w] = {"nowych_uslug": brak, "tok_rodzaj_na_usluge": round(tok_r[0] / max(brak, 1)),
                        "tok_cechy_na_usluge": round(tok_c[0] / max(brak, 1)), "usd": round(usd, 3),
                        "usd_na_usluge": round(usd / max(brak, 1), 6), "sekund": round(sek)}
            (OUT / f"destylacje_{w}.json").write_text(json.dumps(pamieci[w], ensure_ascii=False), encoding="utf-8")
            print(f"{w} {OPIS[w]:<26} {brak} usług | rodzaj {koszt[w]['tok_rodzaj_na_usluge']} + cechy {koszt[w]['tok_cechy_na_usluge']} tok./usł. "
                  f"| {usd:.3f} USD | {sek:.0f} s", flush=True)
        # decyzje kodu per wariant + miernik
        for p in pary:
            for w in koszt:
                p[w] = porownaj_v8(pamieci[w].get(p["ka"]), pamieci[w].get(p["kb"]), podzial, p["fa"], p["fb"], _sch=sch8)[0]
        sedzia = OcenaPar(cli, client, budzet_usd=max(a.budzet - wydane, 0.3))
        oc = await sedzia.ocen([(p["a"], p["b"]) for p in pary])
    for p in pary:
        p["sedzia"] = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("werdykt")

    wynik = {
        "koszt": koszt, "sedzia_usd": round(sedzia.koszt_usd, 4), "wydane_usd": round(wydane + sedzia.koszt_usd, 3),
        "lacznie": {w: metryki(pary, w) for w in koszt},
        "per_branza": {b: {w: metryki([p for p in pary if p["branza"] == b], w) for w in koszt} for b in sorted({p["branza"] for p in pary})},
        "sedzia_rozklad": dict(Counter(p["sedzia"] for p in pary)),
        "zgodnosc_rodzaju_z_A_proc": {w: round(sum(1 for k in do_dest if k in pamieci["A"] and k in pamieci[w]
                                                   and pamieci[w][k]["rodzaj"] == pamieci["A"][k]["rodzaj"])
                                                   / max(sum(1 for k in do_dest if k in pamieci["A"] and k in pamieci[w]), 1) * 100, 1)
                                      for w in koszt},
    }
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "pary.json").write_text(json.dumps([{k: v for k, v in p.items() if k not in ("fa", "fb")} for p in pary], ensure_ascii=False), encoding="utf-8")
    print(json.dumps(wynik, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Cztery warianty destylacji: koszt vs trafność względem sędziego")
    p.add_argument("--salonow", type=int, default=10)
    p.add_argument("--uslug", type=int, default=7)
    p.add_argument("--budzet", type=float, default=2.8, help="USD łącznie (destylacje + sędzia)")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
