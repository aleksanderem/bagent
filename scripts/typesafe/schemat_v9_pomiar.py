"""Schemat v8 vs v9 na tych samych parach i ocenach sędziego (salony pomiaru wariantów 27.09,
nieużywane przy diagnozie schematu). Ten sam tryb destylacji (pełne opisy, usługa po usłudze),
żeby zmierzyć sam schemat. Fakty par: tylko nazwa (pary.json nie trzyma czasu trwania)."""
import asyncio, json, os, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import warianty_destylacji as wd  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci, jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

D25 = wd.sk.DANE / "2026-09-25"
OUT = wd.OUT
pary = json.loads((OUT / "pary.json").read_text(encoding="utf-8"))
p8 = json.loads((D25 / "podzial.json").read_text(encoding="utf-8"))
p9 = json.loads((D25 / "podzial_v9.json").read_text(encoding="utf-8"))
do_dest = {}
for p in pary:
    for k, s in ((p["ka"], p["a"]), (p["kb"], p["b"])):
        do_dest[k] = (s["nazwa"], s["kategoria_w_cenniku"] or None, s["typ_salonu"] or None)


def werdykty(pam: dict, pod: dict) -> list[str]:
    sch = jako_schemat(pod)
    return [porownaj_v8(pam.get(p["ka"]), pam.get(p["kb"]), pod, {"nazwa": p["a"]["nazwa"]}, {"nazwa": p["b"]["nazwa"]}, _sch=sch)[0] for p in pary]


async def main():
    plik9 = OUT / "destylacje_v9.json"
    pam9 = json.loads(plik9.read_text(encoding="utf-8")) if plik9.exists() else {}
    tr, tc = [0], [0]
    if len(pam9) < len(do_dest):
        key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
        async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
            sek = await wd.destyluj(client, "A", do_dest, p9, dzieci(p9), pam9, tr, tc)
        plik9.write_text(json.dumps(pam9, ensure_ascii=False), encoding="utf-8")
        print(f"v9: {len(do_dest)} usług, rodzaj {tr[0]/len(do_dest):.0f} + cechy {tc[0]/len(do_dest):.0f} tok./usł., "
              f"{(tr[0]+tc[0])*wd.CENA_TOK:.3f} USD, {sek:.0f} s", flush=True)
    pam8 = json.loads((OUT / "destylacje_A.json").read_text(encoding="utf-8"))
    w8, w9 = werdykty(pam8, p8), werdykty(pam9, p9)
    for p, a, b in zip(pary, w8, w9):
        p["v8"], p["v9"] = a, b
    wyn = {"lacznie": {"v8": wd.metryki(pary, "v8"), "v9": wd.metryki(pary, "v9")},
           "per_branza": {br: {v: wd.metryki([p for p in pary if p["branza"] == br], v) for v in ("v8", "v9")}
                          for br in sorted({p["branza"] for p in pary})}}
    ok = lambda w, s: (w == "tozsame") == (s == "tozsame")  # noqa: E731
    oc = [p for p in pary if p["sedzia"] in ("tozsame", "powiazane", "rozne")]
    wyn["bilans"] = {"poprawione": sum(1 for p in oc if not ok(p["v8"], p["sedzia"]) and ok(p["v9"], p["sedzia"])),
                     "pogorszone": sum(1 for p in oc if ok(p["v8"], p["sedzia"]) and not ok(p["v9"], p["sedzia"]))}
    wyn["pogorszone_przyklady"] = [[p["a"]["nazwa"], p["b"]["nazwa"], p["v8"], p["v9"], p["sedzia"]] for p in oc
                                   if ok(p["v8"], p["sedzia"]) and not ok(p["v9"], p["sedzia"])][:15]
    wyn["koszt_v9"] = {"tok_rodzaj": round(tr[0] / max(len(do_dest), 1)), "tok_cechy": round(tc[0] / max(len(do_dest), 1)),
                       "usd": round((tr[0] + tc[0]) * wd.CENA_TOK, 3)}
    (OUT / "schemat_v9_pomiar.json").write_text(json.dumps(wyn, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(wyn, ensure_ascii=False, indent=1))

asyncio.run(main())
