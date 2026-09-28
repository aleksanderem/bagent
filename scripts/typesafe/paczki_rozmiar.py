"""Czy mniejsze paczki odzyskują czułość? Wariant C (opisy raz w danych) przy paczkach 5 i 10
na tych samych usługach i parach co warianty_destylacji (27.09), porównanie z A i C25 w tych
samych warunkach (fakty par: tylko nazwa — czasu trwania pary.json nie przechowuje)."""
import asyncio, json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import warianty_destylacji as wd  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci, jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

OUT = wd.OUT
pary = json.loads((OUT / "pary.json").read_text(encoding="utf-8"))
podzial = json.loads((wd.sk.DANE / "2026-09-25" / "podzial.json").read_text(encoding="utf-8"))
dz, sch8 = dzieci(podzial), jako_schemat(podzial)
do_dest = {}
for p in pary:
    for k, s in ((p["ka"], p["a"]), (p["kb"], p["b"])):
        do_dest[k] = (s["nazwa"], s["kategoria_w_cenniku"] or None, s["typ_salonu"] or None)


def metr(pam: dict) -> dict:
    for p in pary:
        p["_w"] = porownaj_v8(pam.get(p["ka"]), pam.get(p["kb"]), podzial, {"nazwa": p["a"]["nazwa"]}, {"nazwa": p["b"]["nazwa"]}, _sch=sch8)[0]
    return wd.metryki([{**p, "X": p["_w"]} for p in pary], "X")


async def main():
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    wyn = {}
    for w in ("A", "C"):
        wyn[f"{w}{'25' if w == 'C' else ''}"] = metr(json.loads((OUT / f"destylacje_{w}.json").read_text(encoding="utf-8")))
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        for n in (10, 5):
            wd.PACZKA = n
            pam, tr, tc = {}, [0], [0]
            sek = await wd.destyluj(client, "C", do_dest, podzial, dz, pam, tr, tc)
            (OUT / f"destylacje_C{n}.json").write_text(json.dumps(pam, ensure_ascii=False), encoding="utf-8")
            m = metr(pam)
            m["tok_rodzaj_na_usluge"] = round(tr[0] / len(do_dest))
            m["usd_na_usluge"] = round((tr[0] + tc[0]) * wd.CENA_TOK / len(do_dest), 6)
            m["sekund"] = round(sek)
            wyn[f"C{n}"] = m
            print(f"C{n}: {m}", flush=True)
    (OUT / "paczki_rozmiar.json").write_text(json.dumps(wyn, ensure_ascii=False, indent=1), encoding="utf-8")
    for k, m in wyn.items():
        print(f"{k:<4} precyzja {m['precyzja_ta_sama_proc']:>5}  czułość {m['czulosc_ta_sama_proc']:>5}  trafność {m['trafnosc_proc']:>5}"
              + (f"  rodzaj {m['tok_rodzaj_na_usluge']} tok./usł.  {m['usd_na_usluge']} USD/usł." if "usd_na_usluge" in m else ""))

asyncio.run(main())
