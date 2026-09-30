"""Ile salonów w promieniu ma usługę o (prawie) tej samej nazwie — przy limicie 120 kandydatów vs bez limitu.
Tylko odczyt (Supabase + baza wektorów), 0 USD."""
import sys, json, asyncio
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
from dotenv import load_dotenv
load_dotenv(B / ".env")
import sprawdzian7 as s7
from services.similarity_pricing import report_pricing as rp
from services.similarity_pricing.qdrant_search import search_twins
from services.supabase import SupabaseService
PROG = 0.95
async def main(wy):
    s7.OUT = s7.OUT.parent / wy
    us, pary, salony = s7.dane()
    sb = SupabaseService()
    w120 = {}
    for q in pary:
        if q["sim"] >= PROG:
            w120.setdefault(q["a"], set()).add(q["cand_salon"])
    wynik = []
    po_salonie = {}
    for q in pary:
        po_salonie.setdefault(q["salon"], set()).add(q["a"])
    for bid, sids in po_salonie.items():
        rows = sb.client.table("salon_scrape_services").select("id,name,price_grosze,is_active,booksy_id").in_("id", sorted(sids)).execute().data or []
        rows, ids, emb = await rp._fetch_subject_embeddings_with_chain_head_fallback(sb, rows, [int(r["id"]) for r in rows], bid)
        pula = [b for b in rp._geo_competitor_booksy_ids(sb, bid, 15) if b != bid]
        kand = search_twins(ids, pula, subject_embeddings=emb, limit=2000, min_similarity=PROG, exact=True)
        for sid in sids:
            wszystkie = {c["booksy_id"] for c in kand.get(int(sid), [])}
            wynik.append((sid, us[sid]["nazwa"], len(w120.get(sid, set())), len(wszystkie)))
            print(f"  {us[sid]['nazwa'][:50]!r}: przy limicie 120 → {len(w120.get(sid, set()))} salonów, bez limitu → {len(wszystkie)}", flush=True)
    n = len(wynik)
    print(f"{wy}: usług {n}; ≥3 salony z prawie tą samą nazwą: limit 120 → {sum(a >= 3 for *_x, a, _b in wynik)}, bez limitu → {sum(b >= 3 for *_x, b in wynik)}")
asyncio.run(main(sys.argv[1]))
