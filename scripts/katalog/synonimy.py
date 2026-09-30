"""Katalog usług, etap 1 — słownik synonimów rdzeni słów z danych (TypeSafe Choice 4-stanowy, grosze).

Kandydaci: (1) rdzenie o podobnej pisowni (slownik.kandydaci), (2) pary ofert z RÓŻNYCH salonów, które różnią się
dokładnie jednym słowem po każdej stronie przy wspólnej reszcie („strzyżenie głowy i brody” / „strzyżenie włosów
i brody” → głowa ↔ włosy). Odpada bez pytania: ten sam salon sprzedaje obie wersje osobno (slownik.negatywy).
Wynik: pamięć relacji + słownik rdzeń → rdzeń kanoniczny (scalane tylko „to samo”). Użycie w ocenie:
ocena_podpisu.py --klasy --slownik.

  bagent/.venv/bin/python bagent/scripts/katalog/synonimy.py --budzet 0.1   (bramka płatnych uruchomień)
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts")]
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU, oferty_z_uslugi  # noqa: E402
from services.katalog_uslug.podpis import podpis  # noqa: E402
from services.katalog_uslug.slownik import TO_SAMO, kandydaci, negatywy, pytanie_relacji, zbuduj  # noqa: E402

_spec = importlib.util.spec_from_file_location("test_paczek", B / "scripts" / "katalog" / "test_paczek.py")
tp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tp)

KLUCZ = Path.home() / ".config" / "typesafe" / "api_key"
MODEL = "jev-1.13.0"
CENA_TOK = 0.042 / 1e6
KAT = B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}"
PRZYKLADOW = 3
PRZED: set[str] = set()


def dane() -> tuple[list, dict, dict]:
    tp.OUT = KAT / "wszystkie"
    oferty = tp.oferty_probki(0)
    rek, _ = tp.rekordy("p12", oferty)
    salon = {}
    for z in tp.ZBIORY:
        us = json.loads((tp.DZ / z / "uslugi.json").read_text(encoding="utf-8"))["uslugi"]
        salon |= {str(k): v.get("booksy_id") or v.get("salon") for k, v in us.items()}
    return oferty, rek, salon


def dane_rynku() -> tuple[list, dict, dict]:
    """Zbiór 1042 par + wszystkie inne wyciągnięte cenniki (raporty testowe, sprawdziany) — przybliżenie rynku.
    Słownik z samego zbioru par nie znał branż spoza niego (29.09: piercing „Piercing” / „Przekłucie” w raporcie VEAN)."""
    oferty, rek, salon = dane()
    znane = {o.id for o in oferty}
    baza = B / "scripts" / "katalog" / "dane" / "2026-09-29"
    for kat in [*sorted((baza / "raport").glob("*")), *sorted(baza.glob("sprawdzian*"))]:
        if not ((kat / "uslugi.json").exists() and (kat / "p12.json").exists()):
            continue
        us = json.loads((kat / "uslugi.json").read_text(encoding="utf-8"))["uslugi"]
        of = sorted((o for k, u in us.items() for o in oferty_z_uslugi({**u, "id": int(k)})), key=lambda o: o.id)
        tp.OUT = kat
        r, _ = tp.rekordy("p12", of)
        oferty += [o for o in of if o.id not in znane and o.id in r]
        znane |= {o.id for o in of}
        rek |= {k: v for k, v in r.items() if k not in rek}
        salon |= {str(k): v.get("booksy_id") for k, v in us.items() if str(k) not in salon}
    tp.OUT = KAT / "wszystkie"
    return oferty, rek, salon


def pary_kandydatow(oferty: list, rek: dict, salon: dict) -> tuple[set[tuple[str, str]], dict[str, list[str]], set]:
    pod = {o.id: podpis(rek[o.id]) for o in oferty if o.id in rek}
    cena = {o.id: o.cena_zl for o in oferty}
    zbiory = [(salon.get(oid.split("#")[0]), p.zbior, oid) for oid, p in pod.items()]
    neg = negatywy((str(s), z, cena.get(oid)) for s, z, oid in zbiory)
    czestosc = Counter(w for _s, z, _o in zbiory for w in z)
    przyklady: dict[str, list[str]] = defaultdict(list)
    nazwy = {o.id: (o.nazwa + (f" — {o.wariant}" if o.wariant else "")) for o in oferty}
    for _s, z, oid in zbiory:
        for w in z:
            if len(przyklady[w]) < PRZYKLADOW and nazwy[oid] not in przyklady[w]:
                przyklady[w].append(nazwy[oid])
    kand = {tuple(sorted(p)) for p in kandydaci(czestosc)}
    po_reszcie: dict[frozenset, list[tuple[str, str]]] = defaultdict(list)
    for s, z, _o in zbiory:
        if len(z) >= 2:
            for w in z:
                po_reszcie[z - {w}].append((str(s), w))
    for wpisy in po_reszcie.values():
        for i, (sa, wa) in enumerate(wpisy):
            for sb, wb in wpisy[i + 1:]:
                if sa != sb and wa != wb:
                    kand.add(tuple(sorted((wa, wb))))
    kand = {p for p in kand if frozenset(p) not in neg and not any(c.isdigit() for c in "".join(p))}
    return kand, przyklady, neg


async def zapytaj(kand: set[tuple[str, str]], przyklady: dict[str, list[str]], budzet: float, proba: int = 0) -> float:
    from typesafe_sdk import AsyncTypeSafeClient
    plik = KAT / "synonimy.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    nowe = [p for p in sorted(kand) if " | ".join(p) not in pamiec]
    if proba:  # próba do obejrzenia przed pełnym przebiegiem: losowe N nowych par
        import random
        nowe = random.Random(proba).sample(nowe, min(proba, len(nowe)))
    szac = len(nowe) * 350 * CENA_TOK
    print(f"kandydatów {len(kand)}, nowych {len(nowe)}, szac. {szac:.3f} USD (budżet {budzet})", flush=True)
    if szac > budzet:
        sys.exit("szacunek ponad budżet — przerwane")
    tok, sem = [0], asyncio.Semaphore(6)
    async with AsyncTypeSafeClient(api_key=KLUCZ.read_text(encoding="utf-8").strip(), model=MODEL, timeout=60.0) as c:
        async def jedna(a: str, b: str) -> None:
            async with sem:
                stan = {"oferty_a": przyklady.get(a, []), "oferty_b": przyklady.get(b, [])}
                try:
                    r = await c.system_one(stan, {"r": pytanie_relacji(a, b)}, model=MODEL)
                    tok[0] += r.usage.input_tokens or 0
                    ch = r.choices["r"]
                    pamiec[f"{a} | {b}"] = {"relacja": ch.choice, "rozklad": dict(ch.probabilities), **stan}
                except Exception as e:  # noqa: BLE001 — brak odpowiedzi = brak scalenia, zapisane jawnie
                    pamiec[f"{a} | {b}"] = {"relacja": None, "blad": f"{type(e).__name__}: {str(e)[:120]}", **stan}
        await asyncio.gather(*(jedna(a, b) for a, b in nowe))
    plik.write_text(json.dumps(pamiec, ensure_ascii=False, indent=1), encoding="utf-8")
    return tok[0] * CENA_TOK


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budzet", type=float, default=0.1)
    ap.add_argument("--prog", type=float, default=0.8, help="min. prawdopodobieństwo „to samo” do scalenia")
    ap.add_argument("--rynek", action="store_true", help="kandydaci ze wszystkich wyciągniętych cenników → slownik_rynek.json")
    ap.add_argument("--licz", action="store_true", help="tylko liczba kandydatów, bez pytań")
    ap.add_argument("--proba", type=int, default=0, help="zapytaj tylko o N losowych nowych par (do obejrzenia)")
    a = ap.parse_args()
    oferty, rek, salon = dane_rynku() if a.rynek else dane()
    global PRZED
    PRZED = set(json.loads((KAT / "synonimy.json").read_text(encoding="utf-8"))) if (KAT / "synonimy.json").exists() else set()
    kand, przyklady, neg = pary_kandydatow(oferty, rek, salon)
    print(f"ofert {len(oferty)}, z rekordem {len(rek)}; negatywów (ten sam salon sprzedaje obie wersje): {len(neg)}")
    if a.licz:
        pam = json.loads((KAT / "synonimy.json").read_text(encoding="utf-8")) if (KAT / "synonimy.json").exists() else {}
        sys.exit(f"kandydatów {len(kand)}, nowych {sum(' | '.join(p) not in pam for p in kand)}")
    koszt = asyncio.run(zapytaj(kand, przyklady, a.budzet, a.proba))
    if a.proba:
        pam = json.loads((KAT / "synonimy.json").read_text(encoding="utf-8"))
        for k in sorted(pam, key=lambda k: -pam[k].get("rozklad", {}).get(TO_SAMO, 0)):
            v = pam[k]
            if tuple(k.split(" | ")) in kand and v.get("oferty_a") and k not in PRZED:
                print(f"  {v.get('relacja')} {v.get('rozklad', {}).get(TO_SAMO, 0):.2f}  {k}  | {v['oferty_a'][:1]} / {v['oferty_b'][:1]}")
        sys.exit(f"próba: koszt {koszt:.4f} USD")
    pamiec = json.loads((KAT / "synonimy.json").read_text(encoding="utf-8"))
    # mój przegląd scaleń (plan: przegląd słownika — ja): pary odrzucone z powodem, np. „zestaw” ≠ „pakiet” z modelu
    odrzucone = json.loads((KAT / "slownik_odrzucone.json").read_text(encoding="utf-8")) if (KAT / "slownik_odrzucone.json").exists() else {}
    relacje = {tuple(k.split(" | ")): v["relacja"] for k, v in pamiec.items()
               if v.get("relacja") == TO_SAMO and v["rozklad"].get(TO_SAMO, 0) >= a.prog and k not in odrzucone}
    print(f"odrzuconych w przeglądzie: {len(odrzucone)}")
    czestosc = Counter(w for o in oferty if o.id in rek for w in podpis(rek[o.id]).zbior)
    slownik = zbuduj(czestosc, relacje)
    (KAT / ("slownik_rynek.json" if a.rynek else "slownik.json")).write_text(json.dumps(slownik, ensure_ascii=False, indent=1),
                                                                         encoding="utf-8")
    rozk = Counter(v.get("relacja") for v in pamiec.values())
    print(f"koszt {koszt:.4f} USD; relacje: {dict(rozk)}; scaleń w słowniku (≥ {a.prog}): {len(slownik)}")
    for k, v in sorted(pamiec.items(), key=lambda kv: -kv[1].get("rozklad", {}).get(TO_SAMO, 0))[:30]:
        print(f"  {v.get('relacja')} {v.get('rozklad', {}).get(TO_SAMO, 0):.2f}  {k}  | {v['oferty_a'][:1]} / {v['oferty_b'][:1]}")


if __name__ == "__main__":
    main()
