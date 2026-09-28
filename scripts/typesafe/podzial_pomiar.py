"""Pomiar klasyfikacji po drzewie-podziale (v8) vs v7 i sędzia par (bd BEAUTY_AUDIT-qbe4).

Zbiór: pary holdoutu (mig 188) + usługi, których identyczna nazwa występuje
w ≥ 2 salonach (do 6 wpisów na nazwę). Klucze usług jak w schemat_pomiar.py,
więc v7 czyta się z tej samej pamięci.

Miary:
  A. bez etykiet — spójność rodzaju dla identycznej nazwy w różnych salonach:
     ściśle (ten sam rodzaj) i zgodnie (ten sam albo jeden jest przodkiem drugiego),
  B. z etykietami — na TYCH SAMYCH parach co sędzia par v2: wszystkie, przyjęte
     przez silnik do ceny, rozstrzygnięte, nieparzyste id,
  C. udział par rozstrzygniętych (nie „niepełne”).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/podzial_pomiar.py [--dane <katalog>] [--budzet 1.5]
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
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"

from schemat_pomiar import dane as wczytaj, klucz  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import WERSJA, cechy_v8, dodatki_v8, dzieci, jako_schemat, porownaj_v8, rodzaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, liczby, porownaj, stan  # noqa: E402
from services.typesafe_profile.destylacja import nk  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

K = ("tozsame", "powiazane", "rozne")
NA_NAZWE = 6
TOK_NA_USLUGE = 6500


def bin_(pary):
    pary = [(x, s) for x, s in pary if x in K and s]
    n = len(pary)
    tp = sum(1 for x, s in pary if x == "tozsame" and s == "tozsame")
    fp = sum(1 for x, s in pary if x != "tozsame" and s == "tozsame")
    fn = sum(1 for x, s in pary if x == "tozsame" and s != "tozsame")
    return {"n": n, "trafnosc": round((n - fp - fn) / max(n, 1) * 100, 1),
            "wychwycone": round(tp / max(tp + fn, 1) * 100, 1), "precyzja": round(tp / max(tp + fp, 1) * 100, 1)}


def przodkowie(r: str, rodzic: dict) -> set:
    out = set()
    while r in rodzic and r not in out:
        r = rodzic[r]
        out.add(r)
    return out


def spojnosc(grupy: dict[str, list[str]], rodzic: dict) -> dict:
    scis = zg = n = 0
    for rs in grupy.values():
        for i in range(len(rs)):
            for j in range(i + 1, len(rs)):
                n += 1
                a, b = rs[i], rs[j]
                scis += a == b
                zg += a == b or a in przodkowie(b, rodzic) or b in przodkowie(a, rodzic)
    return {"par": n, "scisle_proc": round(scis / max(n, 1) * 100, 1), "zgodnie_proc": round(zg / max(n, 1) * 100, 1)}


async def main_async(args: argparse.Namespace) -> None:
    D = Path(args.dane)
    podzial = json.loads((D / "podzial.json").read_text(encoding="utf-8"))
    schemat7 = json.loads((D / "schemat.json").read_text(encoding="utf-8"))
    v7 = json.loads((D / "destylacje_v7.json").read_text(encoding="utf-8"))
    sedzia = {w["id"]: w["sedzia"] for w in json.loads((D / "kalibracja_pary_sedzia_v2.json").read_text(encoding="utf-8"))}
    wiersze, hold, sv, typ, cennik = await wczytaj(SupabaseService(), D / "ocena_trafnosci_wiersze.json")
    uslugi, fakty = {}, {}

    def dodaj(n, kat, typ_s, s, cena, czas, pakiet) -> str:
        t = (n, kat or s.get("category_name"), typ_s, s.get("description"), s.get("treatment_name"), typ_s)
        k = klucz(t)
        uslugi[k] = t
        fakty[k] = {"nazwa": n, "price_grosze": cena, "duration_minutes": czas or s.get("duration_minutes"), "is_package": pakiet}
        return k

    pary_h = []
    for r in hold:
        sa = cennik.get((r["subject_booksy_id"], nk(r["subject_name"])), {})
        sbv = cennik.get((r.get("cand_booksy_id"), nk(r["cand_name"])), {})
        ka = dodaj(r["subject_name"], r.get("subject_category"), r["branza"], sa, r.get("subject_price_grosze"), None, sa.get("is_package"))
        kb = dodaj(r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), ""), sbv, r.get("cand_price_grosze"), None, sbv.get("is_package"))
        pary_h.append((r, ka, kb))
    for _b, w in wiersze:
        s = cennik.get((w["salon"], nk(w["usluga"])), {})
        dodaj(w["usluga"], s.get("category_name"), typ.get(w["salon"], ""), s, w.get("cena_podmiotu"), w.get("czas_podmiotu"), s.get("is_package"))
        for p in w["probki"]:
            dodaj(p["nazwa"], p.get("kategoria"), typ.get(p.get("booksy_id"), ""), sv.get(p.get("service_id"), {}), p.get("cena"), p.get("czas"), p.get("pakiet"))
    po_nazwie: dict[str, list[str]] = defaultdict(list)
    for k in sorted(uslugi):
        if k in v7:
            po_nazwie[k.split("|")[0]].append(k)
    grupy_k = {n: ks[:NA_NAZWE] for n, ks in po_nazwie.items() if len(ks) >= 2}
    zbior = {k for _r, ka, kb in pary_h for k in (ka, kb)} | {k for ks in grupy_k.values() for k in ks}

    plik = D / "destylacje_v8.json"
    v8 = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = sorted(k for k in zbior if k not in v8)
    bez_dodatkow = sorted(k for k in zbior if k in v8 and "zestaw" not in v8[k])
    szac = (len(brak) * TOK_NA_USLUGE + len(bez_dodatkow) * 800) * 0.042 / 1e6
    print(f"zbiór {len(zbior)} usług (holdout {len(pary_h)} par, nazw wielokrotnych {len(grupy_k)}); "
          f"do destylacji {len(brak)}, do pytań o zestaw/dodatek {len(bez_dodatkow)} (szac. {szac:.2f} USD)", flush=True)
    if szac > args.budzet:
        raise SystemExit("ponad budżet")
    dz = dzieci(podzial)
    tok = [0]
    sem = asyncio.Semaphore(6)

    async def jedna(k: str) -> None:
        n, kat, typ_s, opis, zb, _br = uslugi[k]
        st = stan(n, kat, typ_s, opis, zb)
        async with sem:
            try:
                r = await rodzaj_v8(client, st, podzial, dz, tok)
                stary = v7.get(k, {})
                cechy = (stary.get("cechy", {}).get(r["rodzaj"]) if stary.get("rodzaj") == r["rodzaj"] else None)
                if cechy is None:
                    cechy = await cechy_v8(client, st, r["rodzaj"], podzial, tok)
                dod = await dodatki_v8(client, st, tok)
            except Exception as e:  # noqa: BLE001
                print(f"v8 {n[:40]!r}: {type(e).__name__}: {str(e)[:100]}")
                return
        v8[k] = {"wersja": WERSJA, **r, "cechy": {r["rodzaj"]: cechy}, **dod, **liczby(n, opis)}

    async def dodatki(k: str) -> None:
        n, kat, typ_s, opis, zb, _br = uslugi[k]
        async with sem:
            try:
                v8[k] = {**v8[k], **await dodatki_v8(client, stan(n, kat, typ_s, opis, zb), tok)}
            except Exception as e:  # noqa: BLE001
                print(f"dodatki {n[:40]!r}: {type(e).__name__}: {str(e)[:100]}")

    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        await asyncio.gather(*[jedna(k) for k in brak], *[dodatki(k) for k in bez_dodatkow])
    plik.write_text(json.dumps(v8, ensure_ascii=False), encoding="utf-8")
    print(f"zdestylowano {len(brak)}, zestaw/dodatek {len(bez_dodatkow)}, koszt {tok[0] * 0.042 / 1e6:.3f} USD", flush=True)

    # A. spójność identycznych nazw
    wyn = {
        "spojnosc_v7": spojnosc({n: [v7[k]["rodzaj"] for k in ks] for n, ks in grupy_k.items()}, schemat7.get("rodzice", {})),
        "spojnosc_v8": spojnosc({n: [v8[k]["rodzaj"] for k in ks if k in v8] for n, ks in grupy_k.items()}, podzial["rodzic"]),
    }
    # B/C. holdout na tych samych parach
    sch8 = jako_schemat(podzial)
    H = []
    for r, ka, kb in pary_h:
        w7, _ = porownaj(v7.get(ka), v7.get(kb), schemat7, fakty[ka], fakty[kb])
        w8, p8 = porownaj_v8(v8.get(ka), v8.get(kb), podzial, fakty[ka], fakty[kb], _sch=sch8)
        H.append({"id": r["id"], "dec": r.get("decyzja_silnika"), "branza": r["branza"], "final": r.get("label_final"),
                  "human": r.get("label_human"), "sedzia": sedzia.get(r["id"]), "v7": w7, "v8": w8, "pow8": p8,
                  "a": r["subject_name"], "b": r["cand_name"]})
    jak = lambda w: "powiazane" if w == "niepelne" else w  # noqa: E731
    for etyk in ("final", "human"):
        for nazwa, filtr in (("wszystkie", lambda h: True), ("przyjete", lambda h: h["dec"] == "przyjety"),
                             ("przyjete_nieparzyste", lambda h: h["dec"] == "przyjety" and h["id"] % 2)):
            z = [h for h in H if filtr(h)]
            for metoda in ("v7", "v8", "sedzia"):
                wyn[f"{etyk}|{nazwa}|{metoda}"] = bin_([(h[etyk], jak(h[metoda])) for h in z])
    wyn["rozstrzygniete_v7_proc"] = round(sum(h["v7"] != "niepelne" for h in H) / len(H) * 100, 1)
    wyn["rozstrzygniete_v8_proc"] = round(sum(h["v8"] != "niepelne" for h in H) / len(H) * 100, 1)
    wyn["powody_v8"] = dict(Counter(h["pow8"].split(",")[0] for h in H).most_common(12))
    (D / "podzial_pomiar.json").write_text(json.dumps(wyn, ensure_ascii=False, indent=1), encoding="utf-8")
    (D / "podzial_pomiar_pary.json").write_text(json.dumps(H, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(wyn, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Pomiar podziału rodzajów (v8)")
    p.add_argument("--dane", default=str(Path(__file__).resolve().parent / "dane" / "2026-09-25"))
    p.add_argument("--budzet", type=float, default=1.5)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
