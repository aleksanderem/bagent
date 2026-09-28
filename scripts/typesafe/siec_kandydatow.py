"""Pomiar szerszej sieci kandydatów — taksonomia decyduje, wektory tylko łowią (bd BEAUTY_AUDIT-asrk).

Decyzja Alexa 26.09: nazwa, opis i dane z Booksy budują wektory, żeby było z czego
wybierać kandydatów; o tym, czy to ta sama usługa, decyduje taksonomia z destylacji.
Pytanie pomiaru: ile tych samych usług przybywa, gdy sieć jest szersza niż dziś
(80 najbliższych, podobieństwo ≥ 0,68), i czy nowe znaleziska są prawdziwe.

Dla wylosowanych salonów (po jednym z branży, różne miasta, inne niż w pomiarze 25.09)
i próbki ich usług:
  1. kandydaci z Qdranta w promieniu 15 km — jedno szerokie wyszukiwanie; węższe sieci
     to jego początek (wyniki są posortowane po podobieństwie),
  2. krok zerowy: destylacja v8 każdej usługi bez wpisu w pamięci (plik), zapis,
  3. decyzja o parze: kod (podzial.porownaj_v8) na destylacjach obu stron, bez modelu,
  4. per wiersz: liczba salonów z TĄ SAMĄ usługą w każdej sieci,
  5. miernik: sędzia par ocenia próbkę par „ta sama” znalezionych tylko przez szerszą
     sieć oraz próbkę z dzisiejszej — czy nowe znaleziska są prawdziwe,
  6. dla porównania: czy obecny silnik daje temu wierszowi cenę (--bez-silnika wyłącza).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/siec_kandydatow.py --salonow 1 --pilot
  bagent/.venv/bin/python bagent/scripts/typesafe/siec_kandydatow.py --salonow 12 --budzet 6
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics as st
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ocena_trafnosci as ot  # noqa: E402  (ustawia katalog, Qdranta i wyłącza pomost GLM)

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import WERSJA, cechy_v8, dodatki_v8, dzieci, jako_schemat, porownaj_v8, rodzaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, liczby, stan  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import para_klucz, strona  # noqa: E402
from services.typesafe_profile.destylacja import nk  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20260926
PROMIEN_KM = 15
SIECI = {"dzis": (80, 0.68), "srednia": (150, 0.60)}  # szeroka = (--limit, --prog)
TOK_NA_USLUGE = 7200  # zmierzone 26.09 (predkosc_v8): 6,6–7,2 tys.
CENA_TOK = 0.042 / 1e6
ROWNOLEGLE = 30
DANE = Path(__file__).resolve().parent / "dane"
POPRZEDNIE = DANE / "2026-09-25" / "ocena_trafnosci_wiersze.json"


def klucz(nazwa: str | None, kat: str | None, typ: str | None) -> str:
    return f"{nk(nazwa)}|{nk(kat)}|{nk(typ)}"


def uzyte_salony() -> set[int]:
    if not POPRZEDNIE.exists():
        return set()
    w = json.loads(POPRZEDNIE.read_text(encoding="utf-8"))
    return {r["salon"] for rs in w.values() for r in rs if r.get("salon")}


async def losuj(sb: SupabaseService, n: int, rng: random.Random, pomin: set[int]) -> list[tuple[str, int, str, dict]]:
    """(branża, booksy_id, miasto, dane) — po jednym z branży, różne miasta."""
    cli, wybrane, miasta = sb.client, [], set()
    branze = list(ot.BRANZE.items())
    rng.shuffle(branze)
    for runda in range(3):
        for branza, cat in branze:
            if len(wybrane) >= n or sum(1 for b, *_ in wybrane if b == branza) > runda:
                continue
            kand = (cli.table("salons").select("booksy_id,city").eq("primary_category_id", cat)
                    .not_.is_("city", "null").order("booksy_id").limit(3000).execute().data or [])
            rng.shuffle(kand)
            for s in kand:
                bid, city = s.get("booksy_id"), s.get("city")
                if not bid or bid in pomin or city in miasta:
                    continue
                dane = (await sb.get_competitor_full_data([bid])).get(bid) or {}
                akt = [x for x in dane.get("services") or [] if x.get("is_active", True) and x.get("price_grosze")]
                if len(akt) < ot.MIN_USLUG:
                    continue
                wybrane.append((branza, bid, city, {**dane, "booksy_id": bid}))
                miasta.add(city)
                break
    return wybrane


class Destylacja:
    """Krok zerowy: destylacja v8 z pamięcią w pliku (zapis co 300 usług)."""

    def __init__(self, plik: Path, podzial: dict, client: Any, budzet: float):
        self.plik, self.podzial, self.client, self.budzet = plik, podzial, client, budzet
        self.pamiec: dict[str, dict] = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
        self.dz = dzieci(podzial)
        self.tok = [0]
        self.bledy = 0

    @property
    def koszt(self) -> float:
        return self.tok[0] * CENA_TOK

    async def _jedna(self, k: str, nazwa: str, kat: str | None, typ: str | None, sem: asyncio.Semaphore) -> None:
        s = stan(nazwa, kat, typ, None, None)
        async with sem:
            try:
                r = await rodzaj_v8(self.client, s, self.podzial, self.dz, self.tok)
                cechy, dod = await asyncio.gather(
                    cechy_v8(self.client, s, r["rodzaj"], self.podzial, self.tok), dodatki_v8(self.client, s, self.tok))
            except Exception as e:  # noqa: BLE001 — jedna usługa nie zatrzymuje pomiaru
                self.bledy += 1
                print(f"  destylacja {nazwa[:40]!r}: {type(e).__name__}: {str(e)[:100]}", flush=True)
                return
        self.pamiec[k] = {"wersja": WERSJA, **r, "cechy": {r["rodzaj"]: cechy}, **dod, **liczby(nazwa, None)}

    async def uzupelnij(self, uslugi: dict[str, tuple[str, str | None, str | None]]) -> tuple[int, float] | None:
        brak = [k for k in uslugi if k not in self.pamiec]
        szac = len(brak) * TOK_NA_USLUGE * CENA_TOK
        if self.koszt + szac > self.budzet:
            print(f"STOP budżetu: do destylacji {len(brak)} usług, szac. {szac:.2f} USD, "
                  f"wydane {self.koszt:.2f} z {self.budzet:.2f} USD — kończę na zebranych salonach", flush=True)
            return None
        sem, t0 = asyncio.Semaphore(ROWNOLEGLE), time.monotonic()
        for i in range(0, len(brak), 300):
            await asyncio.gather(*[self._jedna(k, *uslugi[k], sem) for k in brak[i:i + 300]])
            self.plik.write_text(json.dumps(self.pamiec, ensure_ascii=False), encoding="utf-8")
        return len(brak), time.monotonic() - t0


def w_sieci(kand: list[dict], limit: int, prog: float) -> list[dict]:
    return [c for c in kand if c["similarity"] >= prog][:limit]


def podsumuj(wiersze: list[dict], sieci: list[str]) -> dict:
    out: dict[str, Any] = {"wierszy": len(wiersze)}
    n = max(len(wiersze), 1)
    for s in sieci:
        t = [w["sieci"][s]["salony_tozsame"] for w in wiersze]
        tj = [w["sieci"][s].get("salony_tozsame_i_sedzia") for w in wiersze]
        out[s] = {
            **({"potwierdzone_sedzia_3plus_proc": round(sum(1 for x in tj if x >= 3) / n * 100, 1),
                "potwierdzone_sedzia_5plus_proc": round(sum(1 for x in tj if x >= 5) / n * 100, 1)} if all(x is not None for x in tj) and tj else {}),
            "wiersze_3plus_proc": round(sum(1 for x in t if x >= 3) / n * 100, 1),
            "wiersze_5plus_proc": round(sum(1 for x in t if x >= 5) / n * 100, 1),
            "mediana_salonow_tozsamych": st.median(t) if t else 0,
            "kandydatow_srednio": round(st.mean([w["sieci"][s]["kandydatow"] for w in wiersze]), 1) if wiersze else 0,
        }
    ceny = [w for w in wiersze if w.get("silnik_ma_cene") is not None]
    if ceny:
        out["silnik_wiersze_z_cena_proc"] = round(sum(1 for w in ceny if w["silnik_ma_cene"]) / len(ceny) * 100, 1)
    return out


async def main_async(a: argparse.Namespace) -> None:
    out = DANE / "2026-09-26" / "siec"
    out.mkdir(parents=True, exist_ok=True)
    siatki = {**SIECI, "szeroka": (a.limit, a.prog)}
    sb = SupabaseService()
    cli = sb.client
    branze = ot._nazwy_branz(cli)
    salony = await losuj(sb, a.salonow, random.Random(SEED), uzyte_salony())
    (out / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print(f"wylosowano {len(salony)}: " + ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)
    podzial = json.loads((DANE / "2026-09-25" / "podzial.json").read_text(encoding="utf-8"))
    sch8 = jako_schemat(podzial)
    api_key = (os.environ.get("TYPESAFE_API_KEY") or ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    rng = random.Random(SEED)
    wiersze: list[dict] = []
    nowe_pary: list[tuple[dict, dict]] = []
    stare_pary: list[tuple[dict, dict]] = []
    odrz_wys: list[tuple] = []  # taksonomia „nie ta sama”, podobieństwo ≥ 0,68
    odrz_nis: list[tuple] = []  # taksonomia „nie ta sama”, podobieństwo < 0,68
    szczegoly: dict[str, list[dict]] = {}
    do_sedziego: list[tuple[int, int, int, float, dict, dict]] = []  # (wiersz, pozycja, salon, podobieństwo, a, b)
    powody: dict[str, Counter] = {}
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        dest = Destylacja(out / "destylacje.json", podzial, client, a.budzet)
        for branza, bid, miasto, dane in salony:
            t0 = time.monotonic()
            uslugi = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
            uslugi, ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(
                sb, uslugi, [int(s["id"]) for s in uslugi], bid)
            uslugi = [s for s in uslugi if int(s["id"]) in emb]
            if not uslugi:
                print(f"{branza} {bid}: brak wektorów usług — pomijam", flush=True)
                continue
            uslugi = rng.sample(uslugi, min(a.uslug, len(uslugi)))
            pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, PROMIEN_KM) if b != bid]
            kand = search_twins([int(s["id"]) for s in uslugi], pula, subject_embeddings=emb,
                                limit=a.limit, min_similarity=a.prog, exact=True)
            typy = ot._typy_salonow(cli, {c["booksy_id"] for cs in kand.values() for c in cs} | {bid}, branze)
            do_dest: dict[str, tuple[str, str | None, str | None]] = {}
            for s in uslugi:
                do_dest[klucz(s.get("name"), s.get("category_name"), typy.get(bid))] = (s.get("name") or "", s.get("category_name"), typy.get(bid))
            for cs in kand.values():
                for c in cs:
                    t = typy.get(c["booksy_id"])
                    do_dest[klucz(c["service_name"], c.get("category_name"), t)] = (c["service_name"], c.get("category_name"), t)
            wynik_dest = await dest.uzupelnij(do_dest)
            if wynik_dest is None:
                break
            nowych, sek = wynik_dest
            silnik: dict[str, bool] = {}
            if not a.bez_silnika:
                try:
                    rows = await report_pricing.compute_pricing_comparisons_v2(sb, 0, dane, [])
                    silnik = {nk(r.get("treatment_name")): r.get("market_median_grosze") is not None for r in rows}
                except Exception as e:  # noqa: BLE001
                    print(f"  silnik {bid}: {type(e).__name__}: {str(e)[:120]}", flush=True)
            for s in uslugi:
                ka = klucz(s.get("name"), s.get("category_name"), typy.get(bid))
                fa = {"nazwa": s.get("name"), "price_grosze": s.get("price_grosze"),
                      "duration_minutes": s.get("duration_minutes"), "is_package": s.get("is_package")}
                oceny = []
                for c in kand.get(int(s["id"]), []):
                    kb = klucz(c["service_name"], c.get("category_name"), typy.get(c["booksy_id"]))
                    fb = {"nazwa": c["service_name"], "price_grosze": c.get("price_grosze"),
                          "duration_minutes": c.get("duration_minutes"), "is_package": c.get("is_package")}
                    w, pw = porownaj_v8(dest.pamiec.get(ka), dest.pamiec.get(kb), podzial, fa, fb, _sch=sch8)
                    oceny.append({**c, "werdykt": w, "powod": pw, "typ": typy.get(c["booksy_id"])})
                siec_w: dict[str, dict] = {}
                for nazwa_s, (lim, prog) in siatki.items():
                    z = w_sieci(oceny, lim, prog)
                    siec_w[nazwa_s] = {
                        "kandydatow": len(z),
                        "salony_tozsame": len({c["booksy_id"] for c in z if c["werdykt"] == "tozsame"}),
                        "salony_powiazane": len({c["booksy_id"] for c in z if c["werdykt"] in ("powiazane", "niepelne")}),
                    }
                dzis_ids = {c["service_id"] for c in w_sieci(oceny, *siatki["dzis"])}
                sa = strona(s.get("name"), s.get("category_name"), typy.get(bid))
                for nazwa_s, (lim, prog) in siatki.items():
                    powody.setdefault(nazwa_s, Counter()).update(
                        (c["powod"] or "").split(",")[0] for c in w_sieci(oceny, lim, prog) if c["werdykt"] != "tozsame")
                for poz, c in enumerate(oceny):
                    if c["werdykt"] == "tozsame" and c["similarity"] >= siatki["szeroka"][1] and poz < siatki["szeroka"][0]:
                        do_sedziego.append((len(wiersze), poz, c["booksy_id"], c["similarity"], sa,
                                            strona(c["service_name"], c.get("category_name"), c["typ"])))
                for c in w_sieci(oceny, *siatki["szeroka"]):
                    sb_ = strona(c["service_name"], c.get("category_name"), c["typ"])
                    kb = klucz(c["service_name"], c.get("category_name"), c["typ"])
                    rek = (sa, sb_, (c["powod"] or "").split(",")[0], ka, kb, c["similarity"], s.get("duration_minutes"), c.get("duration_minutes"))
                    if c["werdykt"] == "tozsame":
                        (stare_pary if c["service_id"] in dzis_ids else nowe_pary).append(rek)
                    else:
                        (odrz_wys if c["similarity"] >= 0.68 else odrz_nis).append(rek)
                wiersze.append({
                    "branza": branza, "salon": bid, "miasto": miasto, "usluga": s.get("name"),
                    "rodzaj": (dest.pamiec.get(ka) or {}).get("rodzaj"),
                    "silnik_ma_cene": silnik.get(nk(s.get("name"))) if silnik else None,
                    "sieci": siec_w,
                    "tozsame_tylko_szeroka": [
                        {"nazwa": c["service_name"], "podobienstwo": c["similarity"], "salon": c["booksy_id"]}
                        for c in w_sieci(oceny, *siatki["szeroka"]) if c["werdykt"] == "tozsame" and c["service_id"] not in dzis_ids
                    ][:8],
                })
            print(f"{branza:<18} {bid:>7} {miasto:<16} pula {len(pula):>5} | usług {len(uslugi)} | do destylacji "
                  f"{len(do_dest)} (nowych {nowych}, {sek:.0f} s) | łącznie {dest.koszt:.3f} USD, błędów {dest.bledy} "
                  f"| {time.monotonic() - t0:.0f} s", flush=True)
            if a.pilot:
                print(json.dumps(podsumuj([w for w in wiersze if w["salon"] == bid], list(siatki)), ensure_ascii=False), flush=True)
        # miernik: sędzia na próbce par „ta sama” — nowe (tylko szersza sieć) vs dzisiejsze
        for lst in (nowe_pary, stare_pary, odrz_wys, odrz_nis):
            rng.shuffle(lst)
        sedzia = OcenaPar(cli, client, budzet_usd=max(a.budzet - dest.koszt, 0.2))
        miernik = {}
        grupy = (("nowe_tylko_szersza", nowe_pary), ("z_dzisiejszej", stare_pary),
                 ("odrzucone_podobne_0_68plus", odrz_wys), ("odrzucone_mniej_podobne", odrz_nis))
        for nazwa_g, rek in grupy:
            probka = rek[: a.miernik]
            oc = await sedzia.ocen([(r[0], r[1]) for r in probka])
            wyn = [oc.get(para_klucz(r[0], r[1])) or {} for r in probka]
            werd = [w.get("werdykt") for w in wyn]
            n = max(sum(1 for w in werd if w), 1)
            miernik[nazwa_g] = {"par": len(probka), "sedzia_tozsame_proc": round(werd.count("tozsame") / n * 100, 1),
                                "sedzia_powiazane_proc": round(werd.count("powiazane") / n * 100, 1),
                                "sedzia_rozne_proc": round(werd.count("rozne") / n * 100, 1),
                                "powod_taksonomii_gdy_sedzia_tozsame": dict(Counter(r[2] for r, w in zip(probka, werd) if w == "tozsame").most_common(10))}
            szczegoly[nazwa_g] = [
                {"a": r[0], "b": r[1], "podobienstwo": r[5], "czas_a": r[6], "czas_b": r[7], "powod_taksonomii": r[2],
                 "sedzia": w.get("werdykt"), "sedzia_cechy": w.get("cechy"),
                 "tax_a": {k: (dest.pamiec.get(r[3]) or {}).get(k) for k in ("rodzaj", "korzen", "cechy", "zestaw", "rozszerzenie")},
                 "tax_b": {k: (dest.pamiec.get(r[4]) or {}).get(k) for k in ("rodzaj", "korzen", "cechy", "zestaw", "rozszerzenie")}}
                for r, w in zip(probka, wyn)
            ]
    if a.sedzia_wszystkie and do_sedziego:
        async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client2:
            sedzia2 = OcenaPar(cli, client2, budzet_usd=1.0)
            oc = await sedzia2.ocen([(x[4], x[5]) for x in do_sedziego])
        for nazwa_s, (lim, prog) in siatki.items():
            for i, w in enumerate(wiersze):
                w["sieci"][nazwa_s]["salony_tozsame_i_sedzia"] = len({
                    x[2] for x in do_sedziego
                    if x[0] == i and x[3] >= prog and x[1] < lim and (oc.get(para_klucz(x[4], x[5])) or {}).get("werdykt") == "tozsame"})
        miernik["wszystkie_pary_ta_sama"] = {"par": len(do_sedziego), "koszt_usd": round(sedzia2.koszt_usd, 4)}
    raport = {
        "sieci": {k: {"limit": v[0], "prog": v[1]} for k, v in siatki.items()},
        "lacznie": podsumuj(wiersze, list(siatki)),
        "per_branza": {b: podsumuj([w for w in wiersze if w["branza"] == b], list(siatki)) for b in sorted({w["branza"] for w in wiersze})},
        "miernik_sedzia": miernik,
        "par_tozsamych_nowych": len(nowe_pary), "par_tozsamych_z_dzisiejszej": len(stare_pary),
        "par_odrzuconych": {"podobne_0_68plus": len(odrz_wys), "mniej_podobne": len(odrz_nis)},
        "powody_odrzucenia": {k: dict(v.most_common(12)) for k, v in powody.items()},
        "destylacja": {"uslug_w_pamieci": len(dest.pamiec), "koszt_usd": round(dest.koszt, 3), "bledow": dest.bledy},
        "sedzia_koszt_usd": round(sedzia.koszt_usd, 4),
    }
    (out / "wynik.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "wiersze.json").write_text(json.dumps(wiersze, ensure_ascii=False), encoding="utf-8")
    (out / "miernik_pary.json").write_text(json.dumps(szczegoly, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Pomiar szerszej sieci kandydatów (taksonomia decyduje)")
    p.add_argument("--salonow", type=int, default=12)
    p.add_argument("--uslug", type=int, default=12, help="usług na salon (losowo)")
    p.add_argument("--limit", type=int, default=300, help="szeroka sieć: ilu kandydatów na usługę")
    p.add_argument("--prog", type=float, default=0.5, help="szeroka sieć: próg podobieństwa")
    p.add_argument("--budzet", type=float, default=6.0, help="USD łącznie (destylacja + sędzia)")
    p.add_argument("--miernik", type=int, default=150, help="ile par „ta sama” ocenia sędzia w każdej grupie")
    p.add_argument("--bez-silnika", action="store_true", help="nie licz obecnego silnika dla porównania")
    p.add_argument("--pilot", action="store_true")
    p.add_argument("--sedzia-wszystkie", action="store_true", help="sędzia ocenia WSZYSTKIE pary „ta sama” (cel: pokrycie przy dokładności sędziego)")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
