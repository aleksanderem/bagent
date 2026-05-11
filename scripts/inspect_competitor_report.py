"""Inspect a competitor_reports row + its children for sanity-check after a
pipeline run. Used to verify that all sections populated correctly.

Usage:
    .venv/bin/python -m scripts.inspect_competitor_report --report-id 35
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings
from services.sb_client import make_supabase_client


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-id", type=int, required=True)
    args = ap.parse_args()

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Header
    rep_rows = (
        client.table("competitor_reports")
        .select("*")
        .eq("id", args.report_id)
        .execute()
        .data
        or []
    )
    if not rep_rows:
        print(f"No competitor_reports row id={args.report_id}")
        return 1
    rep = rep_rows[0]
    rd = rep.get("report_data") or {}

    print(f"=== competitor_reports id={rep['id']} ===")
    print(f"  audit_id        = {rep['convex_audit_id']}")
    print(f"  user_id         = {rep['convex_user_id']}")
    print(f"  subject_salon   = {rep['subject_salon_id']}")
    print(f"  status          = {rep['status']}")
    print(f"  tier            = {rep['tier']}")
    print(f"  competitor #    = {rep['competitor_count']}")
    print(f"  created_at      = {rep['created_at']}")
    print(f"  updated_at      = {rep['updated_at']}")
    print()

    print("=== report_data keys (Etap 5 enrichment) ===")
    for k, v in rd.items():
        if isinstance(v, list):
            shape = f"list[{len(v)}]"
        elif isinstance(v, dict):
            shape = f"dict[{len(v)}]"
        elif isinstance(v, str):
            shape = f"str[{len(v)}]"
        else:
            shape = type(v).__name__
        print(f"  {k:<32} {shape}")
    print()

    # narrative + swot from rd
    if rd.get("narrative"):
        nar = rd["narrative"]
        print(f"=== narrative ({len(nar)} chars) ===")
        print(nar[:400] + ("..." if len(nar) > 400 else ""))
        print()
    if rd.get("swot"):
        sw = rd["swot"]
        print(f"=== SWOT (quadrants={list(sw.keys()) if isinstance(sw, dict) else 'list'}) ===")
        if isinstance(sw, dict):
            for q, items in sw.items():
                if isinstance(items, list):
                    print(f"  {q} ({len(items)}):")
                    for it in items[:3]:
                        if isinstance(it, dict):
                            tit = it.get("title") or it.get("text") or ""
                            print(f"    - {tit[:80]}")
                        else:
                            print(f"    - {str(it)[:80]}")
        print()

    if rd.get("summary"):
        s = rd["summary"]
        print(f"=== summary (top metrics) ===")
        for k in ("position", "competitorsCount", "priceVsMedianPct", "gapsCount", "estimatedUpliftZl", "overallScore"):
            if k in s:
                print(f"  {k:<24} {s[k]}")
        print()

    # Children counts
    print("=== children counts ===")
    for tab in (
        "competitor_matches",
        "competitor_pricing_comparisons",
        "competitor_service_gaps",
        "competitor_dimensional_scores",
        "competitor_recommendations",
    ):
        cnt = (
            client.table(tab)
            .select("id", count="exact")
            .eq("report_id", args.report_id)
            .execute()
        )
        n = cnt.count if cnt.count is not None else len(cnt.data or [])
        print(f"  {tab:<35} {n}")
    print()

    # Top 5 recommendations summary
    recs = (
        client.table("competitor_recommendations")
        .select("action_title, category, impact, effort, confidence, sort_order")
        .eq("report_id", args.report_id)
        .order("sort_order")
        .limit(5)
        .execute()
        .data
        or []
    )
    print("=== top 5 recommendations ===")
    for r in recs:
        print(
            f"  [{r.get('impact','?'):<6}/{r.get('effort','?'):<6}] "
            f"({r.get('category','?'):<12}) "
            f"{r.get('action_title','')[:80]}"
        )
    print()

    # Top 5 matches
    matches = (
        client.table("competitor_matches")
        .select("competitor_salon_id, composite_score, bucket, distance_km, sort_order")
        .eq("report_id", args.report_id)
        .order("sort_order")
        .limit(5)
        .execute()
        .data
        or []
    )
    salon_ids = [m["competitor_salon_id"] for m in matches]
    if salon_ids:
        salons = (
            client.table("salons")
            .select("id, name, city, reviews_rank, reviews_count")
            .in_("id", salon_ids)
            .execute()
            .data
            or []
        )
        by_id = {s["id"]: s for s in salons}
        print("=== top competitor matches ===")
        for m in matches:
            s = by_id.get(m["competitor_salon_id"], {})
            dist_km = m.get("distance_km")
            dist_txt = f"{dist_km:.2f}km" if isinstance(dist_km, (int, float)) else "—"
            print(
                f"  [{m.get('bucket','?'):<8}] score={m.get('composite_score'):.3f} "
                f"dist={dist_txt:<7} ⭐{s.get('reviews_rank','?'):<5} "
                f"({s.get('reviews_count','?')}rev)  {s.get('name','')[:60]}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
