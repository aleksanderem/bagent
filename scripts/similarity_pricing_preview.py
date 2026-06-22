#!/usr/bin/env python3
"""Narzędzie zestawień silnika cenowego similarity-first (read-only, A/B wag).

Dla próbki usług subject + puli konkurentów:
  1. woła fn_find_related_competitor_services_v2 (prod, przez ssh psql) → klaster bliźniaków
  2. przepuszcza przez engine.compute_market_price dla kilku wartości w_category
  3. drukuje zestawienie obok siebie — żeby ocenić jak waga taksonomii zmienia
     klaster i cenę rynkową.

NIC nie zapisuje do bazy. Uruchamiane lokalnie; SQL leci na prod tytana.

Użycie:
  python scripts/similarity_pricing_preview.py
  (subjekty i konkurenci dobierane automatycznie — patrz _pick_sample)
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# import silnika (lokalny bagent)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.similarity_pricing.engine import compute_market_price  # noqa: E402

PSQL = "docker exec -i ba-supabase-db psql -U postgres -d postgres -tAc"
WEIGHTS = [0.0, 0.5, 1.0]


def _psql(sql: str) -> str:
    """Uruchom SQL na prod przez ssh, zwróć surowy stdout."""
    out = subprocess.run(
        ["ssh", "root@tytan", f"{PSQL} {json.dumps(sql)}"],
        capture_output=True, text=True, timeout=300,
    )
    if out.returncode != 0:
        raise RuntimeError(f"psql err: {out.stderr[:400]}")
    return out.stdout.strip()


def _pick_competitor_pool(n: int = 500) -> list[int]:
    """Pula konkurentów = n pseudolosowych chain-head salonów z embeddingiem."""
    sql = (
        "SELECT string_agg(booksy_id::text, ',') FROM ("
        "  SELECT sc.booksy_id FROM salon_scrapes sc "
        "  JOIN salon_scrape_services s ON s.scrape_id=sc.id "
        "  WHERE sc.is_chain_head AND s.name_embedding IS NOT NULL "
        f"  GROUP BY sc.booksy_id ORDER BY md5(sc.booksy_id::text) LIMIT {n}) q;"
    )
    raw = _psql(sql)
    return [int(x) for x in raw.split(",") if x.strip()]


def _pick_subjects() -> list[int]:
    """Subjekty: presoterapia ref + trudne przypadki dobrane po nazwie."""
    fixed = [6094762]  # presoterapia
    # GENERYCZNE (pokazują efekt wagi) + SPECYFICZNE (embedding wystarcza)
    patterns = [
        "Konsultacja", "Wizyta kontrolna",            # generyczne — waga zadziała
        "kwas hialuronowy", "Pedicure klasyczny",     # specyficzne
        "Modelowanie ust", "Strzyżenie damskie",
    ]
    ids = list(fixed)
    for pat in patterns:
        pat_sql = "'%" + pat.replace("'", "''") + "%'"
        sql = (
            "SELECT s.id FROM salon_scrape_services s JOIN salon_scrapes sc ON sc.id=s.scrape_id "
            f"WHERE sc.is_chain_head AND s.name ILIKE {pat_sql} "
            "AND s.name_embedding IS NOT NULL AND s.price_grosze>0 AND s.duration_minutes>0 "
            "AND s.category_name IS NOT NULL "
            "ORDER BY md5(s.id::text) LIMIT 1;"
        )
        r = _psql(sql)
        if r.strip():
            ids.append(int(r.strip()))
    return ids


def _fetch_clusters(subject_ids: list[int], competitor_ids: list[int]) -> dict[int, list[dict]]:
    """fn_v2 batch → {subject_id: [samples]}."""
    sids = ",".join(str(i) for i in subject_ids)
    cids = ",".join(str(i) for i in competitor_ids)
    sql = (
        "SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM "
        f"fn_find_related_competitor_services_v2(ARRAY[{sids}]::bigint[], "
        f"ARRAY[{cids}]::int[], 60, 0.82) t;"
    )
    rows = json.loads(_psql(sql) or "[]")
    by_subj: dict[int, list[dict]] = {}
    subjects_meta: dict[int, dict] = {}
    for r in rows:
        sid = r["subject_service_id"]
        by_subj.setdefault(sid, []).append({
            "service_id": r["service_id"], "booksy_id": r["booksy_id"],
            "salon_id": r["salon_id"], "salon_name": r["salon_name"],
            "service_name": r["service_name"], "price_grosze": r["price_grosze"],
            "duration_minutes": r["duration_minutes"], "similarity": r["similarity"],
            "category_name": r["category_name"], "is_package": r["is_package"],
        })
        subjects_meta[sid] = {
            "service_id": sid, "service_name": r["subject_name"],
            "category_name": r["subject_category_name"],
            "price_grosze": r["subject_price_grosze"],
            "duration_minutes": r["subject_duration_minutes"],
        }
    return by_subj, subjects_meta


def _zl(grosze) -> str:
    return f"{grosze/100:.0f}zł" if grosze is not None else "—"


def main() -> None:
    print("Dobieram konkurentów (2500 salonów) i subjekty...", file=sys.stderr)
    competitors = _pick_competitor_pool(2500)
    subjects = _pick_subjects()
    print(f"  {len(competitors)} konkurentów, {len(subjects)} subjektów", file=sys.stderr)

    by_subj, subjects_meta = _fetch_clusters(subjects, competitors)

    print("\n" + "=" * 116)
    print("ZESTAWIENIE silnika similarity-first + DRUGIE PRAWO (test tożsamości, surowość dobierana per usługa)")
    print("=" * 116)
    for sid in subjects:
        subj = subjects_meta.get(sid)
        samples = by_subj.get(sid, [])
        if subj is None:
            print(f"\n[{sid}] brak danych (subject bez embeddingu lub poza pulą)")
            continue
        res = compute_market_price(subj, samples)
        gen = "GENERYCZNA" if res.subject_generic else "specyficzna"
        twoja = _zl(subj["price_grosze"])
        print(f"\n▸ {subj['service_name'][:44]:44} | kat: {(subj['category_name'] or '—')[:18]:18}"
              f" | Twoja: {twoja}/{subj['duration_minutes']}min  [{gen}]")
        print(f"    TOŻSAMOŚĆ: surowość={res.identity_strictness:<4} | "
              f"tożsamych {res.n_identity_kept}/{res.n_raw_samples} bliźniaków | "
              f"czystość={res.identity_purity:.2f} | powód={res.provenance['identity']['chosen_reason']}")
        odch = ('%+.0f%%' % res.deviation_pct) if res.deviation_pct is not None else '—'
        print(f"    RYNEK:     salony={res.n_unique_salons:3} | status={res.status:12} | "
              f"cena={_zl(res.market_price_grosze):>7} | p25-p75={_zl(res.p25_grosze)}-{_zl(res.p75_grosze)} | "
              f"Twoja vs rynek={odch}")
    print("\n" + "=" * 116)


if __name__ == "__main__":
    main()
