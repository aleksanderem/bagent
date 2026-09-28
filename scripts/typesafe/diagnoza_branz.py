"""Diagnoza wybranych branż na świeżych salonach: schemat v9, jedna usługa na wywołanie, sędzia ocenia wszystkie pary.
Wynik w formacie warianty_destylacji (pary.json + destylacje_v9.json) — czyta go rozbior_branzy.py."""
import argparse, asyncio, json, os, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import warianty_destylacji as wd  # noqa: E402
from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import MODEL, para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20260928


def uzyte() -> set[int]:
    u = wd.sk.uzyte_salony()
    for f in (wd.sk.DANE / "2026-09-26" / "siec" / "salony.json", wd.OUT / "salony.json"):
        if f.exists():
            u |= {b for _br, b, _m in json.loads(f.read_text(encoding="utf-8"))}
    return u


async def main(a):
    out = wd.sk.DANE / "2026-09-27" / "diagnoza"
    out.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService(); cli = sb.client
    rng = random.Random(SEED); pomin = uzyte(); miasta = set(); salony = []
    for branza in a.branze:
        cat = wd.ot.BRANZE[branza]
        kand = cli.table("salons").select("booksy_id,city").eq("primary_category_id", cat).not_.is_("city", "null").order("booksy_id").limit(3000).execute().data or []
        rng.shuffle(kand); n = 0
        for s in kand:
            if n >= a.na_branze: break
            bid, city = s.get("booksy_id"), s.get("city")
            if not bid or bid in pomin or city in miasta: continue
            dane = (await sb.get_competitor_full_data([bid])).get(bid) or {}
            if len([x for x in dane.get("services") or [] if x.get("is_active", True) and x.get("price_grosze")]) < wd.ot.MIN_USLUG: continue
            salony.append((branza, bid, city, {**dane, "booksy_id": bid})); miasta.add(city); n += 1
    (out / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print("salony:", ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)
    branze_nazwy = wd.ot._nazwy_branz(cli)
    do_dest, pary = {}, []
    for branza, bid, miasto, dane in salony:
        uslugi = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        uslugi, _i, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, uslugi, [int(s["id"]) for s in uslugi], bid)
        uslugi = [s for s in uslugi if int(s["id"]) in emb]
        uslugi = rng.sample(uslugi, min(a.uslug, len(uslugi)))
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, 15) if b != bid]
        kand = search_twins([int(s["id"]) for s in uslugi], pula, subject_embeddings=emb, limit=80, min_similarity=0.68, exact=True)
        typy = wd.ot._typy_salonow(cli, {c["booksy_id"] for cs in kand.values() for c in cs} | {bid}, branze_nazwy)
        for s in uslugi:
            ka = wd.sk.klucz(s.get("name"), s.get("category_name"), typy.get(bid))
            do_dest[ka] = (s.get("name") or "", s.get("category_name"), typy.get(bid))
            for c in kand.get(int(s["id"]), []):
                t = typy.get(c["booksy_id"]); kb = wd.sk.klucz(c["service_name"], c.get("category_name"), t)
                do_dest[kb] = (c["service_name"], c.get("category_name"), t)
                pary.append({"branza": branza, "salon": bid, "ka": ka, "kb": kb, "sim": c["similarity"],
                             "a": strona(s.get("name"), s.get("category_name"), typy.get(bid)), "b": strona(c["service_name"], c.get("category_name"), t),
                             "czas_a": s.get("duration_minutes"), "czas_b": c.get("duration_minutes")})
        print(f"  {branza:<12} {miasto:<18} pula {len(pula):>5} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    p9 = json.loads((wd.sk.DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    szac = len(do_dest) * 8300 * wd.CENA_TOK
    print(f"usług {len(do_dest)}, par {len(pary)}, szac. destylacja {szac:.2f} USD", flush=True)
    if szac > a.budzet: raise SystemExit("ponad budżet")
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    pam, tr, tc = {}, [0], [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        await wd.destyluj(client, "A", do_dest, p9, dzieci(p9), pam, tr, tc)
        sedzia = OcenaPar(cli, client, budzet_usd=0.5)
        oc = await sedzia.ocen([(p["a"], p["b"]) for p in pary])
    for p in pary:
        p["sedzia"] = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("werdykt")
    (out / "destylacje_v9.json").write_text(json.dumps(pam, ensure_ascii=False), encoding="utf-8")
    (out / "pary.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    print(f"koszt: destylacja {(tr[0]+tc[0])*wd.CENA_TOK:.3f} USD, sędzia {sedzia.koszt_usd:.3f} USD", flush=True)

p = argparse.ArgumentParser()
p.add_argument("--branze", nargs="+", default=["Podologia", "Masaż"])
p.add_argument("--na-branze", type=int, default=4)
p.add_argument("--uslug", type=int, default=10)
p.add_argument("--budzet", type=float, default=1.8)
asyncio.run(main(p.parse_args()))
