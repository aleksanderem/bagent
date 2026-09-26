"""Rewizja wniosków z pomiaru trafności 25.09 (tylko odczyt, zero wywołań TypeSafe).

A. Holdout: werdykt taksonomii v7 i sędziego par v2 na TYCH SAMYCH parach
   (v7 rozstrzyga tylko część — wczoraj porównałem v7 na jej podzbiorze
   z sędzią na całości), plus podział parzyste/nieparzyste id.
B. 18 salonów: tabela krzyżowa v7 × sędzia v2 na tych samych próbkach ceny.

Dane wejściowe: scripts/typesafe/dane/2026-09-25/ (patrz README tam).
Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/rewizja_trafnosci.py [--dane <katalog>]
"""
import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
os.chdir(B)
sys.path.insert(0, str(B))
sys.path.insert(0, str(B / "scripts/typesafe"))
from schemat_pomiar import dane, klucz  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.schemat import porownaj  # noqa: E402
from services.typesafe_profile.destylacja import nk  # noqa: E402

S = Path(__file__).resolve().parent / "dane" / "2026-09-25"
K = ("tozsame", "powiazane", "rozne")


def bin_(pary):
    pary = [(x, s) for x, s in pary if x in K and s]
    n = len(pary)
    tp = sum(1 for x, s in pary if x == "tozsame" and s == "tozsame")
    fp = sum(1 for x, s in pary if x != "tozsame" and s == "tozsame")
    fn = sum(1 for x, s in pary if x == "tozsame" and s != "tozsame")
    return {"n": n, "trafnosc": round((n - fp - fn) / max(n, 1) * 100, 1),
            "wychwycone": round(tp / max(tp + fn, 1) * 100, 1), "precyzja": round(tp / max(tp + fp, 1) * 100, 1),
            "tp": tp, "fp": fp, "fn": fn}


async def main(S: Path):
    schemat = json.loads((S / "schemat.json").read_text())
    pamiec = json.loads((S / "destylacje_v7.json").read_text())
    sb = SupabaseService()
    wiersze, hold, sv, typ, cennik = await dane(sb, S / "ocena_trafnosci_wiersze.json")
    fakty = {}

    def k_(n, kat, typ_s, s, cena, czas, pakiet):
        t = (n, kat or s.get("category_name"), typ_s, s.get("description"), s.get("treatment_name"), typ_s)
        k = klucz(t)
        fakty[k] = {"nazwa": n, "price_grosze": cena, "duration_minutes": czas or s.get("duration_minutes"), "is_package": pakiet}
        return k

    sedzia = {w["id"]: w["sedzia"] for w in json.loads((S / "kalibracja_pary_sedzia_v2.json").read_text())}
    H = []
    for r in hold:
        sa = cennik.get((r["subject_booksy_id"], nk(r["subject_name"])), {})
        sbv = cennik.get((r.get("cand_booksy_id"), nk(r["cand_name"])), {})
        ka = k_(r["subject_name"], r.get("subject_category"), r["branza"], sa, r.get("subject_price_grosze"), None, sa.get("is_package"))
        kb = k_(r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), ""), sbv, r.get("cand_price_grosze"), None, sbv.get("is_package"))
        w, pw = porownaj(pamiec.get(ka), pamiec.get(kb), schemat, fakty[ka], fakty[kb])
        H.append({"id": r["id"], "branza": r["branza"], "bin": r.get("sim_bin"), "dec": r.get("decyzja_silnika"),
                  "a": r["subject_name"], "b": r["cand_name"], "v7": w, "pow": pw, "sedzia": sedzia.get(r["id"]),
                  "final": r.get("label_final"), "human": r.get("label_human")})
    rozstrz = [h for h in H if h["v7"] != "niepelne"]
    wyn = {}
    for etyk in ("final", "human"):
        for nazwa, zbior in (("wszystkie", H), ("v7_rozstrzygniete", rozstrz), ("v7_niepelne", [h for h in H if h["v7"] == "niepelne"]),
                             ("nieparzyste_rozstrz", [h for h in rozstrz if h["id"] % 2])):
            wyn[f"{etyk}|{nazwa}|v7"] = bin_([(h[etyk], "powiazane" if h["v7"] == "niepelne" else h["v7"]) for h in zbior])
            wyn[f"{etyk}|{nazwa}|sedzia"] = bin_([(h[etyk], h["sedzia"]) for h in zbior])
    wyn["holdout_rozkład_pasm"] = dict(Counter(h["bin"] for h in H))
    wyn["holdout_rozkład_decyzji_silnika"] = dict(Counter(h["dec"] for h in H))
    wyn["holdout_etykiety_modeli"] = dict(Counter(h["final"] for h in H))

    # B. salony: v7 × sędzia na tych samych próbkach
    X = defaultdict(Counter)
    przyk = defaultdict(list)
    for b, w in wiersze:
        s = cennik.get((w["salon"], nk(w["usluga"])), {})
        ka = k_(w["usluga"], s.get("category_name"), typ.get(w["salon"], ""), s, w.get("cena_podmiotu"), w.get("czas_podmiotu"), s.get("is_package"))
        for p in w["probki"]:
            kb = k_(p["nazwa"], p.get("kategoria"), typ.get(p.get("booksy_id"), ""), sv.get(p.get("service_id"), {}), p.get("cena"), p.get("czas"), p.get("pakiet"))
            v7, pw = porownaj(pamiec.get(ka), pamiec.get(kb), schemat, fakty[ka], fakty[kb])
            X[b][(v7, p.get("werdykt"))] += 1
            X["_razem"][(v7, p.get("werdykt"))] += 1
            if v7 == "rozne" and p.get("werdykt") == "tozsame" and len(przyk[b]) < 6:
                przyk[b].append((w["usluga"][:40], (pamiec.get(ka) or {}).get("rodzaj"), p["nazwa"][:40], (pamiec.get(kb) or {}).get("rodzaj")))
    wyn["salony_v7_x_sedzia"] = {b: {f"{a}|{s}": n for (a, s), n in c.most_common()} for b, c in X.items()}
    wyn["salony_przyklady_v7_rozne_sedzia_tozsame"] = przyk
    (S / "rewizja.json").write_text(json.dumps(wyn, ensure_ascii=False, indent=1, default=list))
    (S / "rewizja_holdout.json").write_text(json.dumps(H, ensure_ascii=False, indent=1))
    print(f"gotowe → {S / 'rewizja.json'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Rewizja pomiaru trafności (tylko odczyt)")
    ap.add_argument("--dane", default=str(S))
    asyncio.run(main(Path(ap.parse_args().dane)))
