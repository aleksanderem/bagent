#!/usr/bin/env python3
"""One-shot: backfill comparison_tier='structured' rows for report_id=34
(Beauty4ever audit). Uses fn_pricing_samples_structured (mig 101). Replaces
the polluted tier='treatment' rows visible in the UI screenshots.

Idempotent: deletes existing tier='structured' rows for report 34 first.
"""
from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
import statistics

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env")

from services.supabase import SupabaseService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_structured")

REPORT_ID = 34
SUBJECT_SALON_NAME_LIKE = "%Beauty4ever%Wo%"
MIN_DIRECT = 3


def main():
    sb = SupabaseService()

    # Resolve subject salon + chain head services
    subj = sb.client.table("salons").select("id, booksy_id, name").ilike("name", SUBJECT_SALON_NAME_LIKE).execute().data
    if not subj:
        log.error("Subject salon not found")
        sys.exit(1)
    subj = subj[0]
    log.info("Subject: %s (id=%s booksy_id=%s)", subj["name"], subj["id"], subj["booksy_id"])

    # Chain head scrape
    sc = sb.client.table("salon_scrapes").select("id").eq("salon_ref_id", subj["id"]).eq("is_chain_head", True).order("scraped_at", desc=True).limit(1).execute().data
    if not sc:
        log.error("No chain-head scrape for subject")
        sys.exit(1)
    scrape_id = sc[0]["id"]

    # Subject services
    svc_res = sb.client.table("salon_scrape_services").select("id, name, price_grosze, duration_minutes, booksy_treatment_id, is_active").eq("scrape_id", scrape_id).eq("is_active", True).execute().data
    log.info("Loaded %d subject services", len(svc_res))

    # Competitor booksy ids from existing report's samples
    raw = sb.client.rpc("execute_sql", {}).execute() if False else None  # not using
    # Direct query to extract competitor booksy IDs
    import httpx
    SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
    SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"}
    with httpx.Client(timeout=60.0) as client:
        # Pull distinct booksy_ids from competitor_samples JSONB
        # Use a custom RPC-like via PostgREST: not trivial. Hardcode the 15 known.
        # From earlier query: [132893,282494,161091,239350,103642,117745,116829,
        #                       106076,142912,171153,78029,156127,239382,67964,15467]
        pass
    competitor_booksy_ids = [132893,282494,161091,239350,103642,117745,116829,106076,142912,171153,78029,156127,239382,67964,15467]
    log.info("Using %d competitors", len(competitor_booksy_ids))

    # Delete existing tier=structured rows for this report
    sb.client.table("competitor_pricing_comparisons").delete().eq("report_id", REPORT_ID).eq("comparison_tier", "structured").execute()
    log.info("Cleared existing structured rows for report %d", REPORT_ID)

    PACKAGE_KW = ("pakiet", "abonament", "karnet", "voucher", "bon ", "x zabieg", "zabiegów")

    emitted = 0
    direct = 0
    related_only = 0
    skipped = 0

    for svc in svc_res:
        sid = svc["id"]
        name = (svc.get("name") or "").strip()
        price = svc.get("price_grosze")
        dur = svc.get("duration_minutes")
        booksy_tid = svc.get("booksy_treatment_id")
        if not name or price is None or price <= 0:
            skipped += 1
            continue
        if dur is None or dur < 5 or dur > 240:
            skipped += 1
            continue
        n_lower = name.lower()
        if any(kw in n_lower for kw in PACKAGE_KW):
            skipped += 1
            continue

        # Call structural RPC
        try:
            res = sb.client.rpc("fn_pricing_samples_structured", {
                "p_subject_service_id": int(sid),
                "p_competitor_booksy_ids": competitor_booksy_ids,
                "p_limit": 500,
                "p_radius_km": 16.0,
            }).execute()
            samples = res.data or []
        except Exception as e:
            log.error("RPC failed for service %s: %s", sid, e)
            continue

        t12 = [s for s in samples if s.get("tier") in (1, 2)]
        t3 = [s for s in samples if s.get("tier") == 3]

        def to_sample(s):
            return {
                "salon_id": s.get("salon_id"),
                "salon_name": s.get("salon_name"),
                "booksy_id": s.get("booksy_id"),
                "service_id": s.get("service_id"),
                "service_name": s.get("service_name"),
                "price_grosze": int(s["price_grosze"]),
                "duration_minutes": s.get("duration_minutes"),
                "name_similarity": float(s.get("similarity") or 0.0),
                "brand_marker": s.get("brand_family"),
                "method_marker": s.get("method_canonical"),
                "structured_tier": int(s.get("tier") or 0),
            }

        direct_samples = [to_sample(s) for s in t12]
        related_samples = [{**to_sample(s), "relation": "same_category"} for s in t3]

        if len(direct_samples) >= MIN_DIRECT:
            prices = sorted([s["price_grosze"] for s in direct_samples])
            n = len(prices)
            market_min = prices[0]
            market_max = prices[-1]
            market_p25 = prices[int(0.25 * (n - 1))]
            market_median = prices[int(0.50 * (n - 1))]
            market_p75 = prices[int(0.75 * (n - 1))]
            cheaper = sum(1 for p in prices if p < price)
            percentile = round(100.0 * cheaper / n, 2)
            deviation = round(100.0 * (price - market_median) / market_median, 2) if market_median > 0 else 0.0
            if abs(deviation) < 10:
                action = "hold"
            elif deviation < 0:
                action = "raise"
            else:
                action = "lower"
            row = {
                "report_id": REPORT_ID,
                "comparison_tier": "structured",
                "booksy_treatment_id": booksy_tid,
                "variant_id": None,
                "treatment_method_id": None,
                "treatment_name": name,
                "subject_price_grosze": price,
                "subject_is_from_price": False,
                "subject_duration_minutes": dur,
                "subject_price_per_min_grosze": round(price / dur, 2) if dur and dur > 0 else None,
                "market_min_grosze": market_min,
                "market_p25_grosze": market_p25,
                "market_median_grosze": market_median,
                "market_p75_grosze": market_p75,
                "market_max_grosze": market_max,
                "deviation_pct": deviation,
                "subject_percentile": percentile,
                "sample_size": n,
                "recommended_action": action,
                "verification_status": "structured_direct",
                "verification_details": {
                    "direct_tier1_2_count": len(direct_samples),
                    "related_tier3_count": len(related_samples),
                },
                "competitor_samples": direct_samples,
                "related_samples": related_samples,
            }
            direct += 1
        else:
            row = {
                "report_id": REPORT_ID,
                "comparison_tier": "structured",
                "booksy_treatment_id": booksy_tid,
                "variant_id": None,
                "treatment_method_id": None,
                "treatment_name": name,
                "subject_price_grosze": price,
                "subject_is_from_price": False,
                "subject_duration_minutes": dur,
                "subject_price_per_min_grosze": round(price / dur, 2) if dur and dur > 0 else None,
                "market_min_grosze": None,
                "market_p25_grosze": None,
                "market_median_grosze": None,
                "market_p75_grosze": None,
                "market_max_grosze": None,
                "deviation_pct": None,
                "subject_percentile": None,
                "sample_size": len(direct_samples),
                "recommended_action": "subject_only",
                "verification_status": "structured_subject_only",
                "verification_details": {
                    "direct_tier1_2_count": len(direct_samples),
                    "related_tier3_count": len(related_samples),
                    "reason": "insufficient_direct_samples",
                },
                "competitor_samples": direct_samples,
                "related_samples": related_samples,
            }
            related_only += 1
        sb.client.table("competitor_pricing_comparisons").insert(row).execute()
        emitted += 1
        if emitted % 10 == 0:
            log.info("Progress: emitted=%d (direct=%d related_only=%d)", emitted, direct, related_only)

    log.info("DONE — emitted=%d direct=%d related_only=%d skipped=%d", emitted, direct, related_only, skipped)


if __name__ == "__main__":
    main()
