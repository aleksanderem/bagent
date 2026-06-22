#!/usr/bin/env python3
"""A/B: stary silnik cenowy (zapisane wyniki) vs nowy similarity-first + tożsamość.

Dla realnego raportu konkurencji:
  - STARE: czyta competitor_pricing_comparisons (już policzone starym pipeline)
  - NOWE: bierze te same usługi subjektu + tych samych konkurentów, liczy nowym
          silnikiem (fn_find_related_v2 + engine.compute_market_price)
Zestawia parami (po subject_price_grosze + nazwie), sortuje po największej zmianie
ceny rynkowej. NIC nie zapisuje do bazy.

Użycie: python scripts/ab_pricing_compare.py [report_id]   (domyślnie 181)
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.similarity_pricing.engine import compute_market_price  # noqa: E402

PSQL = "docker exec -i ba-supabase-db psql -U postgres -d postgres -tAc"
REPORT_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 181


def psql(sql: str) -> str:
    out = subprocess.run(["ssh", "root@tytan", f"{PSQL} {json.dumps(sql)}"],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise RuntimeError(f"psql err: {out.stderr[:400]}")
    return out.stdout.strip()


def _zl(g):
    return f"{g/100:.0f}" if g is not None else "—"


def main() -> None:
    # subject booksy
    subj_booksy = int(psql(
        f"SELECT s.booksy_id FROM competitor_reports cr JOIN salons s ON s.id=cr.subject_salon_id WHERE cr.id={REPORT_ID};"))
    # konkurenci raportu (z competitor_samples JSONB)
    comp_raw = psql(
        f"SELECT string_agg(DISTINCT sample->>'booksy_id', ',') FROM competitor_pricing_comparisons cpc, "
        f"jsonb_array_elements(coalesce(cpc.competitor_samples,'[]'::jsonb)) sample "
        f"WHERE cpc.report_id={REPORT_ID} AND sample->>'booksy_id' IS NOT NULL;")
    competitors = [int(x) for x in comp_raw.split(",") if x.strip()]
    print(f"Raport {REPORT_ID}: subject booksy={subj_booksy}, {len(competitors)} konkurentów", file=sys.stderr)

    # usługi subjektu (chain-head, embedding, cena)
    subj_rows = json.loads(psql(
        f"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT s.id as service_id, s.name as service_name, "
        f"s.price_grosze, s.duration_minutes, s.category_name, coalesce(s.is_package,false) is_package "
        f"FROM salon_scrape_services s JOIN salon_scrapes sc ON sc.id=s.scrape_id "
        f"WHERE sc.booksy_id={subj_booksy} AND sc.is_chain_head AND s.name_embedding IS NOT NULL "
        f"AND s.price_grosze>0 AND s.is_active) t;"))
    subj_ids = [r["service_id"] for r in subj_rows]
    print(f"  {len(subj_ids)} usług subjektu", file=sys.stderr)

    # NOWE: fn_v2 batch → klastry → compute per usługa
    cids = ",".join(str(c) for c in competitors)
    sids = ",".join(str(i) for i in subj_ids)
    rel = json.loads(psql(
        f"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM "
        f"fn_find_related_competitor_services_v2(ARRAY[{sids}]::bigint[], ARRAY[{cids}]::int[], 60, 0.82) t;"))
    clusters: dict[int, list] = {}
    for r in rel:
        clusters.setdefault(r["subject_service_id"], []).append({
            "service_id": r["service_id"], "booksy_id": r["booksy_id"], "salon_name": r["salon_name"],
            "service_name": r["service_name"], "price_grosze": r["price_grosze"],
            "duration_minutes": r["duration_minutes"], "similarity": r["similarity"],
            "category_name": r["category_name"], "is_package": r["is_package"],
        })

    new_by_key: dict[tuple, dict] = {}  # (price, name_lower) -> {svc,res}
    for svc in subj_rows:
        res = compute_market_price(svc, clusters.get(svc["service_id"], []))
        new_by_key[(svc["price_grosze"], (svc["service_name"] or "").lower())] = {"svc": svc, "res": res}

    # STARE wiersze (z ceną rynkową)
    old_rows = json.loads(psql(
        f"SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT treatment_name, subject_price_grosze, "
        f"subject_duration_minutes, market_median_grosze, deviation_pct, sample_size, comparison_tier, verification_status "
        f"FROM competitor_pricing_comparisons WHERE report_id={REPORT_ID} AND market_median_grosze IS NOT NULL) t;"))

    # dopasuj stare→nowe po subject_price (+ fuzzy nazwa), zbierz pary
    pairs = []
    for old in old_rows:
        op = old["subject_price_grosze"]
        on = (old["treatment_name"] or "").lower()
        best = None
        for (np_, nn), v in new_by_key.items():
            if np_ == op:
                # ta sama cena subjektu; preferuj zbieżność nazwy
                score = len(set(on.split()) & set(nn.split()))
                if best is None or score > best[0]:
                    best = (score, v)
        if best:
            pairs.append((old, best[1]))

    # sortuj po największej zmianie ceny rynkowej (stare vs nowe)
    def delta(pair):
        old, new = pair
        nm = new["res"].market_price_grosze
        om = old["market_median_grosze"]
        if nm is None or om is None:
            return -1
        return abs(nm - om)
    pairs.sort(key=delta, reverse=True)

    print("\n" + "=" * 124)
    print(f"A/B raport {REPORT_ID} — STARY silnik (zapisane) vs NOWY (similarity+tożsamość). Sort: największa zmiana ceny rynkowej.")
    print("=" * 124)
    print(f"{'usługa':30} | {'Twoja':>6} | STARE: {'rynek':>6} {'odch':>6} {'n':>4} {'tier/status':>22} | "
          f"NOWE: {'rynek':>6} {'odch':>6} {'salony':>6} {'status':>12}")
    print("-" * 124)
    shown = 0
    for old, new in pairs:
        if shown >= 28:
            break
        res = new["res"]
        nm = res.market_price_grosze
        new_dev = ('%+.0f%%' % res.deviation_pct) if res.deviation_pct is not None else '—'
        old_dev = ('%+.0f%%' % old["deviation_pct"]) if old["deviation_pct"] is not None else '—'
        ts = f"{old['comparison_tier'] or '?'}/{(old['verification_status'] or '?')[:11]}"
        print(f"{(old['treatment_name'] or '')[:30]:30} | {_zl(old['subject_price_grosze']):>6} | "
              f"       {_zl(old['market_median_grosze']):>6} {old_dev:>6} {old['sample_size']:>4} {ts:>22} | "
              f"      {_zl(nm):>6} {new_dev:>6} {res.n_unique_salons:>6} {res.status:>12}")
        shown += 1
    print("=" * 124)
    print(f"Sparowano {len(pairs)} usług (po subject_price + nazwie). Pokazano top {shown} wg zmiany ceny.")


if __name__ == "__main__":
    main()
