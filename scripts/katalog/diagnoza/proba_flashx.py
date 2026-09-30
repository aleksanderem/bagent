"""Próba GLM-5.3-FlashX obok GLM-5.3-Flash na tych samych paczkach ofert (Alex 30.09: FlashX jako kierunek dla prędkości).

Flash idzie planem kodowania Z.ai (kredyty planu Pro), FlashX NIE jest w planie — płatny z salda konta (cennik
docs.z.ai 30.09: 0,37 / 0,075 / 1,25 USD za 1 mln tokenów wejścia / wejścia z pamięci / wyjścia). Te same parametry co
rozbiór ofert (`taxonomy_backfill.KlientGLM`: thinking + reasoning_effort=low, temperature 0, JSON). Paczki raportu 279,
które Flash już rozebrał (p12.json) — zgodność liczona z tymi rekordami, a osobny przebieg Flash teraz daje poziom
odniesienia (ile rekordów zmienia się między dwoma przebiegami tego samego modelu).

  python scripts/katalog/diagnoza/proba_flashx.py --model flashx --rownolegle 2   # z salda, ~0,003 USD / paczka
  python scripts/katalog/diagnoza/proba_flashx.py --model flash --rownolegle 2    # kredyty planu, ~2 / paczka
  python scripts/katalog/diagnoza/proba_flashx.py --porownaj
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import statistics
import sys
import time
from pathlib import Path

B = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug.ekstrakcja import prompt, waliduj  # noqa: E402
from services.katalog_uslug.podpis import podpis  # noqa: E402

MODELE = {"flash": ("glm-5.3-flash", "https://api.z.ai/api/coding/paas/v4"),
          "flashx": ("glm-5.3-flashx", "https://api.z.ai/api/paas/v4")}
CENA_FLASHX = (0.37, 0.075, 1.25)  # USD / 1 mln tokenów: wejście, wejście z pamięci, wyjście
KREDYTY_FLASH = (2.3, 0.56, 8)      # mnożniki planu kodowania, / 10 000


def _s7():
    spec = importlib.util.spec_from_file_location("s7", B / "scripts" / "katalog" / "sprawdzian7.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.OUT = m.OUT.parent / "raport_279"
    return m


def _paczki() -> tuple[dict[int, list], dict]:
    s7 = _s7()
    wszystkie = s7.tp.paczki(s7.do_wyciagniecia(), "p12")
    flash = json.loads((s7.OUT / "p12.json").read_text(encoding="utf-8"))
    return {int(k): wszystkie[int(k)] for k in flash if flash[k].get("odp") is not None}, flash


async def przebieg(model: str, rownolegle: int) -> None:
    from openai import AsyncOpenAI
    nazwa, url = MODELE[model]
    klucz = (Path.home() / ".config" / "zai" / "api_key").read_text(encoding="utf-8").strip()
    cli = AsyncOpenAI(api_key=klucz, base_url=url)
    paczki, _flash = _paczki()
    sem = asyncio.Semaphore(rownolegle)
    wynik: dict[str, dict] = {}

    async def jedna(i: int, p: list) -> None:
        async with sem:
            t0 = time.monotonic()
            try:
                r = await cli.chat.completions.create(
                    model=nazwa, messages=[{"role": "system", "content": "Odpowiadasz WYLACZNIE poprawnym JSON."},
                                           {"role": "user", "content": prompt(p)}],
                    response_format={"type": "json_object"},
                    extra_body={"thinking": {"type": "enabled"}, "reasoning_effort": "low"},
                    temperature=0.0, max_tokens=16000, timeout=180)
                u = r.usage.model_dump()
                wynik[str(i)] = {"sekundy": round(time.monotonic() - t0, 1), "usage": u,
                                 "odp": json.loads(r.choices[0].message.content), "blad": None}
            except Exception as e:  # noqa: BLE001 — błąd zapisany, próba idzie dalej
                wynik[str(i)] = {"sekundy": round(time.monotonic() - t0, 1), "blad": f"{type(e).__name__}: {str(e)[:200]}"}

    await asyncio.gather(*(jedna(i, p) for i, p in sorted(paczki.items())))
    plik = Path(__file__).parent / f"proba_{model}.json"
    plik.write_text(json.dumps(wynik, ensure_ascii=False), encoding="utf-8")
    print(f"{model}: paczek {len(wynik)}, błędów {sum(1 for v in wynik.values() if v['blad'])} → {plik.name}")


def _koszt(u: dict, model: str) -> float:
    wej = u.get("prompt_tokens") or 0
    pam = (u.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
    wyj = u.get("completion_tokens") or 0
    a, b, c = CENA_FLASHX if model == "flashx" else KREDYTY_FLASH
    suma = (wej - pam) * a + pam * b + wyj * c
    return suma / 1e6 if model == "flashx" else suma / 10000


def porownaj() -> None:
    paczki, flash_dawny = _paczki()
    przebiegi = {m: json.loads((Path(__file__).parent / f"proba_{m}.json").read_text(encoding="utf-8"))
                 for m in MODELE if (Path(__file__).parent / f"proba_{m}.json").exists()}
    rekordy = {"flash_dawny": {}}
    for i, p in paczki.items():
        rek, _b = waliduj(p, flash_dawny[str(i)]["odp"])
        rekordy["flash_dawny"].update(rek)
    for m, w in przebiegi.items():
        rekordy[m] = {}
        sek = [v["sekundy"] for v in w.values() if not v["blad"]]
        wyj = [v["usage"]["completion_tokens"] for v in w.values() if not v["blad"]]
        bledy_walidacji = 0
        for i, v in w.items():
            if v["blad"]:
                continue
            rek, b = waliduj(paczki[int(i)], v["odp"])
            rekordy[m].update(rek)
            bledy_walidacji += len(b)
        koszt = sum(_koszt(v["usage"], m) for v in w.values() if not v["blad"])
        jedn = "USD" if m == "flashx" else "kredytów"
        print(f"{m:7} paczek {len(sek)}/{len(w)} | mediana {statistics.median(sek):.1f} s na paczkę | "
              f"wyjście {statistics.median(wyj):.0f} tok. | {sum(wyj) / sum(sek):.0f} tok./s na zapytanie | "
              f"błędy walidacji {bledy_walidacji} | koszt {koszt:.3f} {jedn}")
    baza = rekordy["flash_dawny"]
    for m in przebiegi:
        wspolne = [oid for oid in baza if oid in rekordy[m]]
        zgodne = sum(podpis(baza[oid]).zbior == podpis(rekordy[m][oid]).zbior for oid in wspolne)
        print(f"zgodność podpisu z wcześniejszym przebiegiem Flash — {m}: {zgodne}/{len(wspolne)} "
              f"({zgodne / max(len(wspolne), 1):.0%})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=list(MODELE))
    ap.add_argument("--rownolegle", type=int, default=2)
    ap.add_argument("--porownaj", action="store_true")
    a = ap.parse_args()
    if a.porownaj:
        porownaj()
    else:
        asyncio.run(przebieg(a.model, a.rownolegle))


if __name__ == "__main__":
    main()
