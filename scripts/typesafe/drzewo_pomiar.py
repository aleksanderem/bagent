"""Drzewo usług v11 na zbiorach roboczych — te same pary co v9 i v10, bez bazy (bd BEAUTY_AUDIT-asrk, 28.09).

Zbiory robocze (18 salonów: warianty + diagnoza 27.09) to salony, na których diagnozowano — to
kierunek, nie dowód. Dowód: nowe losowe salony (po zgodzie na odczyt bazy produkcyjnej).

Każda usługa z par przechodzi po drzewie (drzewo_uslug.wiazka, 3 ścieżki na każdym poziomie).
Pierwsze pytanie (grupa) jest już w pliku z budowy drzewa — to samo pytanie, więc bez wywołania.
Cechy zabiegu: z destylacji v9, gdy liść drzewa to ten sam rodzaj; inaczej pytanie o cechy
nowego rodzaju (pytania v9). Zestaw i dodatek — z destylacji v9 (te same pytania).

Wynik per branża względem sędziego par v3 (czas trwania nie jest różnicą): v9, v10, v11,
v11 z regułą czasu; bilans w obie strony; błędy per węzeł drzewa (gdzie poprawiać drzewo).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_pomiar.py --budzet 1.5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.drzewo_uslug import najlepsza, porownaj_v11, wiazka, zabieg_liscia  # noqa: E402
from services.typesafe_drzewo.podzial import cechy_v8, jako_schemat  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, stan  # noqa: E402
from services.typesafe_drzewo.schemat_v10 import liczby_v10, schemat_v10  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DANE = Path(__file__).resolve().parent / "dane"
OUT = DANE / "2026-09-28" / "drzewo"
PARY = DANE / "2026-09-28" / "v10_robocze" / "pary.json"
V9 = [DANE / "2026-09-27" / "warianty" / "destylacje_v9.json", DANE / "2026-09-27" / "diagnoza" / "destylacje_v9.json"]
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_USLUGA = 5000  # poziomy 2–3 drzewa + cechy dla części usług (szacunek przed pomiarem)
ROWNOLEGLE = 30
WERSJE = ("v9", "v10", "v11", "v11_czas")
MIERNIK = "sedzia_v3"


async def destyluj(client, uslugi: dict[str, dict], drzewo: dict, p10: dict, v9: dict, grupy: dict, pamiec: dict, tok: list) -> None:
    sem = asyncio.Semaphore(ROWNOLEGLE)

    async def jedna(k: str) -> None:
        if k in pamiec:
            return
        s = uslugi[k]
        st = stan(s["nazwa"], s.get("kategoria_w_cenniku") or None, s.get("typ_salonu") or None, None, None)
        r9 = v9.get(k) or {}
        async with sem:
            try:
                beam = await wiazka(client, st, drzewo, tok, grupy=grupy.get(k))
                r = zabieg_liscia(tuple(beam[0]["sciezka"])) if beam else None
                cechy = (r9.get("cechy", {}).get(r, {}) if r and r9.get("rodzaj") == r
                         else (await cechy_v8(client, st, r, p10, tok) if r and r in p10["cechy"] else {}))
            except Exception as e:  # noqa: BLE001 — jedna usługa bez destylacji, reszta dalej
                print(f"  drzewo {s['nazwa'][:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
                return
        pamiec[k] = {"wersja": 11, "sciezki": [[list(b["sciezka"]), round(pow(2.718281828459045, b["log_p"]), 5), round(b["wynik"], 5)] for b in beam],
                     "rodzaj": r or "inny", "cechy": {r or "inny": cechy},
                     "zestaw": r9.get("zestaw"), "rozszerzenie": r9.get("rozszerzenie"), **liczby_v10(s["nazwa"], None)}

    await asyncio.gather(*[jedna(k) for k in uslugi])


def metryki(pary: list[dict], w: str) -> dict:
    z = [p for p in pary if p.get(MIERNIK) in ("tozsame", "powiazane", "rozne") and p.get(w)]
    tp = sum(1 for p in z if p[w] == "tozsame" and p[MIERNIK] == "tozsame")
    fp = sum(1 for p in z if p[w] == "tozsame" and p[MIERNIK] != "tozsame")
    fn = sum(1 for p in z if p[w] != "tozsame" and p[MIERNIK] == "tozsame")
    return {"par": len(z), "precyzja": round(tp / max(tp + fp, 1) * 100, 1), "czulosc": round(tp / max(tp + fn, 1) * 100, 1),
            "trafnosc": round((len(z) - fp - fn) / max(len(z), 1) * 100, 1), "mowi_ta_sama": tp + fp}


def bilans(pary: list[dict], stara: str, nowa: str) -> dict:
    ok = lambda p, w: (p[w] == "tozsame") == (p[MIERNIK] == "tozsame")  # noqa: E731
    z = [p for p in pary if p.get(MIERNIK) in ("tozsame", "powiazane", "rozne") and p.get(stara) and p.get(nowa)]
    return {"lepiej": sum(ok(p, nowa) and not ok(p, stara) for p in z), "gorzej": sum(ok(p, stara) and not ok(p, nowa) for p in z)}


def bledy_wezlow(pary: list[dict], rek: dict, w: str = "v11") -> list[tuple[str, int, int]]:
    """Węzeł (najlepszy liść usługi salonu) → liczba par i błędów — gdzie poprawiać drzewo."""
    wszystkie, zle = Counter(), Counter()
    for p in pary:
        if p.get(MIERNIK) not in ("tozsame", "powiazane", "rozne") or p["ka"] not in rek:
            continue
        wezel = " › ".join(najlepsza(rek[p["ka"]])[:2])
        wszystkie[wezel] += 1
        zle[wezel] += (p[w] == "tozsame") != (p[MIERNIK] == "tozsame")
    return [(wz, wszystkie[wz], zle[wz]) for wz, _ in zle.most_common(20)]


async def main_async(a: argparse.Namespace) -> None:
    drzewo = json.loads((OUT / "drzewo_v11.json").read_text(encoding="utf-8"))
    grupy = json.loads((OUT / "grupy_uslug.json").read_text(encoding="utf-8"))
    p10 = schemat_v10(json.loads((DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8")))
    v9: dict = {}
    for f in V9:
        v9.update(json.loads(f.read_text(encoding="utf-8")))
    pary = json.loads(PARY.read_text(encoding="utf-8"))
    uslugi: dict[str, dict] = {}
    for p in pary:
        uslugi.setdefault(p["ka"], p["a"])
        uslugi.setdefault(p["kb"], p["b"])
    plik = OUT / "destylacje_v11.json"
    rek: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = sum(1 for k in uslugi if k not in rek)
    szac = brak * TOK_USLUGA * CENA_TOK
    print(f"usług {len(uslugi)}, do przejścia po drzewie {brak}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet")
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        await destyluj(client, uslugi, drzewo, p10, v9, grupy, rek, tok)
    plik.write_text(json.dumps(rek, ensure_ascii=False), encoding="utf-8")
    sch = jako_schemat(p10)
    for p in pary:
        fa = {"nazwa": p["a"]["nazwa"], "duration_minutes": p.get("czas_a")}
        fb = {"nazwa": p["b"]["nazwa"], "duration_minutes": p.get("czas_b")}
        p["v11"] = porownaj_v11(rek.get(p["ka"]), rek.get(p["kb"]), p10, fa, fb, _sch=sch)[0]
        p["v11_czas"] = porownaj_v11(rek.get(p["ka"]), rek.get(p["kb"]), p10, fa, fb, _sch=sch, regula_czasu=True)[0]
    grupy_par = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    wynik = {"koszt_usd": round(tok[0] * CENA_TOK, 3), "tok_na_usluge": round(tok[0] / max(brak, 1)),
             "per_branza": {g: {**{w: metryki(z, w) for w in WERSJE},
                                **{f"bilans_v9_{w}": bilans(z, "v9", w) for w in WERSJE[1:]},
                                "salonow": len({p["salon"] for p in z})} for g, z in grupy_par.items()},
             "bledy_wezlow": bledy_wezlow(pary, rek)}
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "pary.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    print(f"koszt {wynik['koszt_usd']} USD, {wynik['tok_na_usluge']} tok./usługę")
    for g, d in wynik["per_branza"].items():
        print(f"{g:<20} sal {d['salonow']:>2} par {d['v9']['par']:>4} | "
              + " | ".join(f"{w}: P{d[w]['precyzja']} C{d[w]['czulosc']}" for w in WERSJE)
              + " | " + " ".join(f"{w}: +{d[f'bilans_v9_{w}']['lepiej']}/-{d[f'bilans_v9_{w}']['gorzej']}" for w in WERSJE[1:]))
    print("\nwęzły z największą liczbą błędów (węzeł, par, błędów):")
    for wz, n, z in wynik["bledy_wezlow"]:
        print(f"  {wz[:60]:<60} {n:>5} {z:>4}")


def main() -> None:
    p = argparse.ArgumentParser(description="Drzewo usług v11 na zbiorach roboczych")
    p.add_argument("--budzet", type=float, default=1.5, help="USD na przejścia po drzewie i cechy")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
