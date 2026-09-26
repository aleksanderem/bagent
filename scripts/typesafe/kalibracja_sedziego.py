"""Kalibracja sędziego par TypeSafe na zamrożonym holdoucie (mig 188).

Zanim sędzia zmierzy raporty, musi zgadzać się z człowiekiem. Ocenia wszystkie
pary holdoutu i porównuje z:
  * label_human — oceny Alexa (49 par), miara nadrzędna,
  * label_final — zgodna etykieta dwóch sędziów-modeli (bez „niepewne”).
Oceny zapisują się w matching_pair_verdict — pomiar raportów użyje ich ponownie.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/kalibracja_sedziego.py --out <katalog>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))

from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import MODEL, para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
KLASY = ("tozsame", "powiazane", "rozne")


def zgodnosc(pary: list[tuple[str, str]]) -> dict[str, Any]:
    """pary (wzorzec, sędzia) → zgodność, macierz pomyłek i precyzja „tożsame”."""
    n = len(pary)
    macierz = {w: dict(Counter(s for x, s in pary if x == w)) for w in KLASY}
    uznane = [x for x, s in pary if s == "tozsame"]
    return {
        "par": n,
        "zgodnosc_proc": round(sum(1 for x, s in pary if x == s) / max(n, 1) * 100, 1),
        # najgroźniejsza pomyłka: sędzia mówi „ta sama”, a to inna usługa
        "sedzia_tozsame_a_naprawde_rozne": sum(1 for x in uznane if x == "rozne"),
        "precyzja_tozsame_proc": round(uznane.count("tozsame") / max(len(uznane), 1) * 100, 1),
        "macierz_wzorzec_na_sedzia": macierz,
    }


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cli = SupabaseService().client
    rows = cli.table("matching_holdout").select("*").execute().data or []
    branze = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    bids = sorted({r["cand_booksy_id"] for r in rows if r.get("cand_booksy_id")})
    typ: dict[int, str] = {}
    for i in range(0, len(bids), 200):
        for s in cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", bids[i : i + 200]).execute().data or []:
            typ[s["booksy_id"]] = branze.get(s.get("primary_category_id"), "")
    strony = [
        (strona(r["subject_name"], r.get("subject_category"), r["branza"]),
         strona(r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), "")))
        for r in rows
    ]
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        sesja = OcenaPar(None if args.bez_zapisu else cli, client, budzet_usd=args.budzet)
        oceny = await sesja.ocen(strony)
    wyniki = []
    for r, (a, b) in zip(rows, strony):
        o = oceny.get(para_klucz(a, b)) or {}
        wyniki.append({"id": r["id"], "branza": r["branza"], "podmiot": a["nazwa"], "kandydat": b["nazwa"],
                       "human": r.get("label_human"), "final": r.get("label_final"), "sedzia": o.get("werdykt"),
                       "score": o.get("score"), "cechy": o.get("cechy")})
    z_sedzia = [w for w in wyniki if w["sedzia"]]
    raport = {
        "vs_czlowiek": zgodnosc([(w["human"], w["sedzia"]) for w in z_sedzia if w["human"] in KLASY]),
        "vs_sedziowie_modele": zgodnosc([(w["final"], w["sedzia"]) for w in z_sedzia if w["final"] in KLASY]),
        "per_branza_vs_sedziowie": {
            b: zgodnosc([(w["final"], w["sedzia"]) for w in z_sedzia if w["branza"] == b and w["final"] in KLASY])["zgodnosc_proc"]
            for b in sorted({w["branza"] for w in z_sedzia})
        },
        "koszt_usd": round(sesja.koszt_usd, 4), "par_z_pamieci": sesja.z_pamieci, "par_nowych": sesja.nowych,
        "par_bez_oceny": len(wyniki) - len(z_sedzia),
    }
    (out / "kalibracja.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "kalibracja_pary.json").write_text(json.dumps(wyniki, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Kalibracja sędziego par TypeSafe na holdoucie")
    p.add_argument("--out", required=True)
    p.add_argument("--budzet", type=float, default=0.5)
    p.add_argument("--bez-zapisu", action="store_true")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
