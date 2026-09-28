"""Paczki z PEŁNYMI opisami (services/typesafe_drzewo/paczki.py) vs usługa po usłudze, schemat v9,
te same usługi, pary i oceny sędziego co schemat_v9_pomiar."""
import asyncio, json, os, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import warianty_destylacji as wd  # noqa: E402
from services.typesafe_drzewo.paczki import destyluj_paczkami  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci, jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, stan  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

OUT = wd.OUT
pary = json.loads((OUT / "pary.json").read_text(encoding="utf-8"))
p9 = json.loads((wd.sk.DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
do_dest = {}
for p in pary:
    for k, s in ((p["ka"], p["a"]), (p["kb"], p["b"])):
        do_dest[k] = (s["nazwa"], s["kategoria_w_cenniku"] or None, s["typ_salonu"] or None)


def metr(pam):
    sch = jako_schemat(p9)
    for p in pary:
        p["X"] = porownaj_v8(pam.get(p["ka"]), pam.get(p["kb"]), p9, {"nazwa": p["a"]["nazwa"]}, {"nazwa": p["b"]["nazwa"]}, _sch=sch)[0]
    return wd.metryki(pary, "X")


async def main():
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    stany = {k: stan(*v, None, None) for k, v in do_dest.items()}
    tok = [0]
    t0 = time.monotonic()
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=120.0)) as client:
        pam = await destyluj_paczkami(client, stany, p9, dzieci(p9), tok, paczka=5, rownolegle=10)
    sek = time.monotonic() - t0
    (OUT / "destylacje_v9_paczki.json").write_text(json.dumps(pam, ensure_ascii=False), encoding="utf-8")
    pojed = json.loads((OUT / "destylacje_v9.json").read_text(encoding="utf-8"))
    zg = sum(1 for k in pam if k in pojed and pam[k]["rodzaj"] == pojed[k]["rodzaj"])
    wyn = {"usług": len(pam), "brak": len(do_dest) - len(pam), "tok_na_usluge": round(tok[0] / max(len(pam), 1)),
           "usd": round(tok[0] * wd.CENA_TOK, 3), "sekund": round(sek),
           "zgodnosc_rodzaju_z_pojedynczymi_proc": round(zg / max(len(pam), 1) * 100, 1),
           "paczki": metr(pam), "pojedynczo": metr(pojed)}
    (OUT / "paczki_pelne_pomiar.json").write_text(json.dumps(wyn, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(wyn, ensure_ascii=False, indent=1))

asyncio.run(main())
