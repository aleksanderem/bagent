"""Porównanie par na ZAPISANYCH profilach TypeSafe zamiast pytania o parę.

Każda nazwa dostaje profil raz (services/typesafe_profile — ten sam kod, który
destyluje przy raporcie; zapis w service_profile, mig 198). Werdykt pary liczy
kod na dwóch profilach (typesafe_profile/weto.py):
  * sprzeczna technika (obie strony pewne, różne pozycje listy branży) → różne,
  * sprzeczność na innej osi (obszar, etap, długość, zakres, objętość, pakiet,
    odbiorca) → powiązane,
  * brak sprzeczności → ta sama usługa.
Żadnego nowego progu: reguły i progi są te same co w wecie silnika.

Miara prawdy: oceny człowieka (49 par holdoutu) i zgodne etykiety sędziów-modeli.
Obok: sędzia par v2 (matching_pair_verdict) na tych samych parach.

Wejścia: holdout (mig 188) + pary z pomiaru na salonach (plik wierszy z
ocena_trafnosci.py). Branża = branża salonu podmiotu (kontekst czytania nazwy,
jak w wycenie raportu).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/porownanie_na_profilach.py \\
      --out <katalog> --wiersze <ocena_trafnosci_wiersze.json> --sedzia <kalibracja_pary.json>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
if not os.environ.get("TYPESAFE_API_KEY") and KEY_FILE.exists():
    os.environ["TYPESAFE_API_KEY"] = KEY_FILE.read_text(encoding="utf-8").strip()

from kalibracja_sedziego import KLASY, zgodnosc  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_profile import destylacja  # noqa: E402
from services.typesafe_profile.destylacja import ProfileSession, nk  # noqa: E402
from services.typesafe_profile.weto import REGULY, veto  # noqa: E402
from warianty_sedziego import binarnie  # noqa: E402

CECHY = tuple(REGULY)


def werdykt_profili(pa: dict | None, pb: dict | None) -> tuple[str | None, list[str]]:
    if not pa or not pb:
        return None, []
    sprzeczne = veto(pa, pb, CECHY)
    if "metoda" in sprzeczne:
        return "rozne", sprzeczne
    return ("powiazane" if sprzeczne else "tozsame"), sprzeczne


async def profile_dla(sb: SupabaseService, nazwy: dict[str, dict[str, str | None]]) -> tuple[dict[tuple[str, str], dict], float]:
    """nazwy: branża → {name_key: kategoria}. Sesja per branża (limit czasu sesji)."""
    wynik: dict[tuple[str, str], dict] = {}
    tokeny = 0
    for branza, meta in nazwy.items():
        sesja = ProfileSession(sb)
        subj = [{"name": n, "category_name": k} for n, k in meta.items()]
        prof = await sesja.profiles_for(branza, subj, {})
        wynik.update({(branza, n): p for n, p in prof.items()})
        tokeny += sesja.tokens
        print(f"{branza:<22} nazw {len(meta):>5}  z profilem {len(prof):>5}  nowe tokeny {sesja.tokens:>9}  pominięte {sesja.skipped}", flush=True)
    return wynik, tokeny * destylacja.USD_ZA_TOKEN


def _pary_holdoutu(cli: Any) -> list[dict[str, Any]]:
    rows = cli.table("matching_holdout").select("*").order("id").execute().data or []
    return [{"zrodlo": "holdout", "id": r["id"], "branza": r["branza"],
             "a": r["subject_name"], "a_kat": r.get("subject_category"),
             "b": r["cand_name"], "b_kat": r.get("cand_category"),
             "human": r.get("label_human"), "final": r.get("label_final")} for r in rows]


async def _pary_salonow(sb: SupabaseService, plik: Path) -> list[dict[str, Any]]:
    dane = json.loads(plik.read_text(encoding="utf-8"))
    salony = sorted({w["salon"] for ws in dane.values() for w in ws})
    cenniki = await sb.get_competitor_full_data(salony)
    kat = {bid: {nk(s.get("name")): s.get("category_name") for s in (c.get("services") or [])} for bid, c in cenniki.items()}
    out = []
    for branza, ws in dane.items():
        for w in ws:
            for p in w["probki"]:
                out.append({"zrodlo": "salony", "branza": branza, "salon": w["salon"],
                            "a": w["usluga"], "a_kat": kat.get(w["salon"], {}).get(nk(w["usluga"])),
                            "b": p["nazwa"], "b_kat": p.get("kategoria") or None, "sedzia_v2": p.get("werdykt")})
    return out


def _metryki(pary: list[dict[str, Any]], pole: str) -> dict[str, Any]:
    h = [(p["human"], p[pole]) for p in pary if p.get(pole) and p.get("human") in KLASY]
    f = [(p["final"], p[pole]) for p in pary if p.get(pole) and p.get("final") in KLASY]
    return {"czlowiek_tak_nie": binarnie(h), "czlowiek_3": zgodnosc(h)["zgodnosc_proc"],
            "modele_tak_nie": binarnie(f), "modele_3": zgodnosc(f)["zgodnosc_proc"],
            "pokrycie_proc": round(sum(1 for p in pary if p.get(pole)) / max(len(pary), 1) * 100, 1)}


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    pary = _pary_holdoutu(sb.client) + await _pary_salonow(sb, Path(args.wiersze))
    nazwy: dict[str, dict[str, str | None]] = defaultdict(dict)
    for p in pary:
        for n, k in ((p["a"], p["a_kat"]), (p["b"], p["b_kat"])):
            nazwy[p["branza"]].setdefault(nk(n), k)
    profile, koszt = await profile_dla(sb, dict(nazwy))
    for p in pary:
        p["profile"], p["sprzeczne"] = werdykt_profili(profile.get((p["branza"], nk(p["a"]))), profile.get((p["branza"], nk(p["b"]))))
    sedzia = {w["id"]: w["sedzia"] for w in json.loads(Path(args.sedzia).read_text(encoding="utf-8"))}
    hold = [p for p in pary if p["zrodlo"] == "holdout"]
    for p in hold:
        p["sedzia_v2"] = sedzia.get(p["id"])
    sal = [p for p in pary if p["zrodlo"] == "salony" and p["profile"] and p.get("sedzia_v2")]
    per_branza = defaultdict(lambda: defaultdict(int))
    for p in sal:
        s = per_branza[p["branza"]]
        s["par"] += 1
        s["zgodne"] += p["profile"] == p["sedzia_v2"]
        s["prof_tozsame"] += p["profile"] == "tozsame"
        s["sedzia_tozsame"] += p["sedzia_v2"] == "tozsame"
    raport = {
        "holdout_profile": _metryki(hold, "profile"),
        "holdout_sedzia_v2": _metryki(hold, "sedzia_v2"),
        "holdout_profile_per_branza_modele_tak_nie": {
            b: binarnie([(p["final"], p["profile"]) for p in hold if p["branza"] == b and p["profile"] and p.get("final") in KLASY])["trafnosc_tak_nie_proc"]
            for b in sorted({p["branza"] for p in hold})},
        "salony_profile_vs_sedzia": {b: {"par": s["par"], "zgodnosc_proc": round(s["zgodne"] / s["par"] * 100, 1),
                                         "tozsame_profile_proc": round(s["prof_tozsame"] / s["par"] * 100, 1),
                                         "tozsame_sedzia_proc": round(s["sedzia_tozsame"] / s["par"] * 100, 1)}
                                     for b, s in per_branza.items()},
        "koszt_profili_usd": round(koszt, 4),
    }
    (out / "profile_raport.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "profile_pary.json").write_text(json.dumps([{k: v for k, v in p.items()} for p in pary], ensure_ascii=False), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Werdykt par na zapisanych profilach TypeSafe")
    p.add_argument("--out", required=True)
    p.add_argument("--wiersze", required=True)
    p.add_argument("--sedzia", required=True)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
