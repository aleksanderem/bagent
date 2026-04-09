"""Supabase client for reading scraped data and writing reports."""

import logging
from typing import Any

from supabase import Client, ClientOptions, create_client

from config import settings

logger = logging.getLogger(__name__)


def _normalize_name(name: str) -> str:
    """Mirror convex/audit/booksyParser.normalizeServiceName: lowercase,
    strip diacritics, collapse non-alnum to spaces. Used for fuzzy matching
    between optimized and scrape services without schema mismatch."""
    import unicodedata
    import re
    decomposed = unicodedata.normalize("NFD", (name or "").lower())
    without_diacritics = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", without_diacritics)
    return re.sub(r"\s+", " ", cleaned).strip()


def _format_duration_minutes(minutes: int | None) -> str | None:
    """Mirror convex/auditReportStorage.formatDurationMinutes."""
    if minutes is None:
        return None
    if minutes < 60:
        return f"{minutes}min"
    h, m = divmod(minutes, 60)
    return f"{h}h" if m == 0 else f"{h}h {m}min"


def _normalize_variant(v: dict) -> dict:
    """Transform a raw Booksy variant JSONB into the legacy ServiceVariant
    pydantic shape expected by bagent: { label, price, duration? }.

    Raw Booksy variants store price as float/int and duration as minutes (int).
    We serialize them to display strings (e.g. "2200 zł", "45min") to match
    what convertBooksyApiToScrapedData in convex/audit/scraping.ts produces.
    """
    price_float = v.get("price") if isinstance(v.get("price"), (int, float)) else None
    service_price = v.get("service_price") if isinstance(v.get("service_price"), str) else None
    price_str = service_price or (f"{price_float} zł" if price_float is not None else "")

    duration_min = v.get("duration") if isinstance(v.get("duration"), (int, float)) else None
    duration_str = _format_duration_minutes(int(duration_min)) if duration_min is not None else None

    return {
        "label": v.get("label") if isinstance(v.get("label"), str) else "",
        "price": price_str,
        "duration": duration_str,
    }


def _svc_row_to_dict(svc: dict) -> dict:
    """Convert a salon_scrape_services row to the legacy ScrapedService shape
    that bagent's ScrapedData pydantic model expects.

    Uses the raw `price` display string when present, falls back to
    price_grosze/100, and formats duration from duration_minutes. Variants
    JSONB is normalized to string-typed label/price/duration entries.
    """
    price_str = svc.get("price")
    if not price_str:
        pg = svc.get("price_grosze")
        if pg is not None:
            price_str = f"{pg / 100:.2f} zł"
        else:
            price_str = ""

    duration_str = _format_duration_minutes(svc.get("duration_minutes"))

    raw_variants = svc.get("variants")
    variants: list[dict] | None = None
    if isinstance(raw_variants, list) and raw_variants:
        variants = [_normalize_variant(v) for v in raw_variants if isinstance(v, dict)]
        if not variants:
            variants = None

    return {
        "name": svc.get("name", ""),
        "price": price_str,
        "duration": duration_str,
        "description": svc.get("description"),
        "imageUrl": svc.get("image_url"),
        "variants": variants,

        # Provenance + canonical taxonomy — propagated through the pipeline
        # into optimized_services so we maintain parity with salon_scrape_services.
        "scrape_service_id": svc.get("id"),
        "canonical_id": svc.get("canonical_id"),
        "booksy_treatment_id": svc.get("booksy_treatment_id"),
        "booksy_service_id": svc.get("booksy_service_id"),
        "treatment_name": svc.get("treatment_name"),
        "treatment_parent_id": svc.get("treatment_parent_id"),
        "body_part": svc.get("body_part"),
        "target_gender": svc.get("target_gender"),
        "technology": svc.get("technology"),
        "classification_confidence": svc.get("classification_confidence"),
        "price_grosze": svc.get("price_grosze"),
        "is_from_price": svc.get("is_from_price"),
        "duration_minutes": svc.get("duration_minutes"),
    }


def _coerce_int(value: object, default: int = 0) -> int:
    """Safely coerce an AI-generated value to int.

    The LLM sometimes returns text like "wiele" (Polish: "many") or "kilka"
    for fields our Postgres schema types as INTEGER, which then crashes the
    insert with `invalid input syntax for type integer`. This helper accepts
    only real numerics and falls back to `default` for anything else.
    """
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
        # Try to pull a leading integer ("12 usług" -> 12)
        import re
        match = re.match(r"-?\d+", stripped)
        if match:
            return int(match.group())
    return default


class SupabaseService:
    def __init__(self) -> None:
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
            options=ClientOptions(headers={"ngrok-skip-browser-warning": "true"}),
        )

    async def get_scraped_data(self, convex_audit_id: str) -> dict:
        """Read scraped data for a convex audit ID.

        Preferred source: the normalized `salon_scrapes` + `salon_scrape_services`
        tables written by Convex audit scraping. Falls back to the legacy
        `audit_scraped_data` blob table for pre-migration audits.

        Returns a dict matching the ScrapedData pydantic shape used by bagent
        pipelines: { salonName, salonAddress, salonLogoUrl, categories, totalServices }.
        Each category is { name, services }, each service is
        { name, price, duration, description, imageUrl, variants }.
        """
        # 1. Try the new normalized tables first. Multiple scrapes per audit
        # can exist (for time-series price tracking) — pick the newest.
        scrape_res = (
            self.client.table("salon_scrapes")
            .select("*")
            .eq("convex_audit_id", convex_audit_id)
            .order("scraped_at", desc=True)
            .limit(1)
            .execute()
        )
        if scrape_res.data:
            scrape_row = scrape_res.data[0]
            scrape_id = scrape_row["id"]

            services_res = (
                self.client.table("salon_scrape_services")
                .select("*")
                .eq("scrape_id", scrape_id)
                .order("category_sort_order", desc=False)
                .order("sort_order", desc=False)
                .execute()
            )

            # Group services by category_name preserving insertion order
            categories: list[dict] = []
            category_index: dict[str, int] = {}
            for svc in services_res.data or []:
                cat_name = svc.get("category_name") or "Bez kategorii"
                if cat_name not in category_index:
                    category_index[cat_name] = len(categories)
                    categories.append({"name": cat_name, "services": []})
                categories[category_index[cat_name]]["services"].append(
                    _svc_row_to_dict(svc)
                )

            total_services = sum(len(c["services"]) for c in categories)
            return {
                "salonName": scrape_row.get("salon_name"),
                "salonAddress": scrape_row.get("salon_address"),
                "salonLogoUrl": scrape_row.get("salon_logo_url"),
                "categories": categories,
                "totalServices": total_services,
                "primaryCategoryId": scrape_row.get("primary_category_id"),
                "salonCity": scrape_row.get("salon_city"),
            }

        # 2. Legacy fallback — pre-migration audits only.
        legacy = (
            self.client.table("audit_scraped_data")
            .select("*")
            .eq("convex_audit_id", convex_audit_id)
            .execute()
        )
        if not legacy.data:
            raise ValueError(f"No scraped data found for audit {convex_audit_id}")
        row = legacy.data[0]
        return {
            "salonName": row.get("salon_name"),
            "salonAddress": row.get("salon_address"),
            "salonLogoUrl": row.get("salon_logo_url"),
            "categories": row.get("categories_json", []),
            "totalServices": row.get("total_services", 0),
        }

    async def get_audit_report(self, convex_audit_id: str) -> dict | None:
        """Read full audit report from Supabase (the report bagent itself wrote).

        Returns the reconstructed EnhancedAuditReport dict with all child data
        (issues, transformations, SEO keywords, quick wins, competitors).
        """
        result = self.client.rpc("get_audit_report", {"p_convex_audit_id": convex_audit_id}).execute()
        if not result.data:
            return None
        return result.data

    async def get_report_category_mapping(self, convex_audit_id: str) -> dict:
        """Fetch category_mapping + category_changes for an audit.

        Direct SELECT on audit_reports — intentionally bypasses the
        get_audit_report RPC so we don't have to change the RPC contract
        to add the new columns (Etap 1 of Unified Report Pipeline).

        Returns {"mapping": {...}, "changes": [...]} or empty structures
        on miss. Used by BAGENT #2 (cennik) to load the mapping that
        BAGENT #1 (report) produced, instead of running its own agent loop.
        """
        result = (
            self.client.table("audit_reports")
            .select("category_mapping, category_changes")
            .eq("convex_audit_id", convex_audit_id)
            .limit(1)
            .execute()
        )
        if not result.data:
            return {"mapping": {}, "changes": []}
        row = result.data[0]
        return {
            "mapping": row.get("category_mapping") or {},
            "changes": row.get("category_changes") or [],
        }

    async def save_report(
        self,
        convex_audit_id: str,
        convex_user_id: str,
        report: dict,
        salon_name: str,
        salon_address: str,
        source_url: str,
    ) -> str:
        """Save audit report to Supabase normalized tables.

        Upserts parent row in audit_reports, then inserts child rows in
        audit_issues, audit_transformations, audit_seo_keywords,
        audit_quick_wins, audit_competitors.
        Returns the report row ID.
        """
        # 1. Upsert main audit_reports row
        location = report.get("salonLocation") or {}
        parent_row = {
            "convex_audit_id": convex_audit_id,
            "convex_user_id": convex_user_id,
            "total_score": _coerce_int(report.get("totalScore", 0)),
            "score_breakdown": report.get("scoreBreakdown", {}),
            "stats": report.get("stats", {}),
            "industry_comparison": report.get("industryComparison", {}),
            "competitor_context": report.get("competitorContext"),
            "salon_lat": location.get("lat"),
            "salon_lng": location.get("lng"),
            "summary": report.get("summary", ""),
            "version": report.get("version", "v2"),
            "salon_name": salon_name or None,
            "salon_address": salon_address or None,
            "source_url": source_url or None,
            # Etap 1 of Unified Report Pipeline: category restructuring is
            # produced by BAGENT #1 instead of BAGENT #2. Persist the mapping
            # + changes so cennik can load them deterministically.
            "category_mapping": report.get("categoryMapping", {}),
            "category_changes": report.get("categoryChanges", []),
            # Etap 3 of Unified Report Pipeline: per-agent coverage counts
            # (totalChecked / optimized / alreadyOptimal / rejected) for the
            # naming and descriptions agents. Frontend Raport tab uses these
            # to render "Agent sprawdził N usług: poprawił X, uznał Y za
            # optymalne".
            "coverage": report.get("coverage", {}),
        }

        result = (
            self.client.table("audit_reports")
            .upsert(parent_row, on_conflict="convex_audit_id")
            .execute()
        )
        if not result.data:
            raise ValueError("Failed to upsert audit_reports")
        report_id = result.data[0]["id"]
        logger.info("Upserted audit_reports row: id=%s, score=%s", report_id, report.get("totalScore"))

        # 2. Delete existing child rows (idempotent for retries)
        for table in ("audit_issues", "audit_transformations", "audit_seo_keywords", "audit_quick_wins", "audit_competitors"):
            self.client.table(table).delete().eq("audit_report_id", report_id).execute()

        # 3. Insert child rows
        # Issues — INSERTED FIRST so we can capture their DB IDs and use
        # them to resolve the caused_by_issue_id FK on audit_transformations
        # (Etap 2 traceability). The Supabase Python client returns the
        # inserted rows in the same order they were sent.
        issues = report.get("topIssues", [])
        issue_id_by_global_index: dict[int, int] = {}
        if issues:
            issue_rows = [
                {
                    "audit_report_id": report_id,
                    "severity": iss.get("severity", "minor"),
                    "dimension": iss.get("dimension", "naming"),
                    "issue": iss.get("issue", ""),
                    "impact": iss.get("impact", ""),
                    # AI sometimes returns "wiele"/"kilka" for affectedCount —
                    # coerce to int or default to 0 to satisfy the schema.
                    "affected_count": _coerce_int(iss.get("affectedCount", 0)),
                    "example": iss.get("example", ""),
                    "fix": iss.get("fix", ""),
                    "sort_order": i,
                }
                for i, iss in enumerate(issues)
            ]
            res = self.client.table("audit_issues").insert(issue_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_issues")
            else:
                logger.info("Inserted %d audit_issues", len(issue_rows))
                # Supabase returns the inserted rows in submission order. We
                # key them by their list index (which is the "global index"
                # that transformations point to via causedByIssueGlobalIndex).
                for i, row in enumerate(res.data):
                    row_id = row.get("id")
                    if isinstance(row_id, int):
                        issue_id_by_global_index[i] = row_id

        # Transformations — use issue_id_by_global_index to resolve
        # causedByIssueGlobalIndex → integer PK of audit_issues.
        transformations = report.get("transformations", [])
        inserted_trans_rows: list[dict] = []
        if transformations:
            trans_rows = []
            for i, t in enumerate(transformations):
                global_idx = t.get("causedByIssueGlobalIndex")
                caused_by_id: int | None = None
                if global_idx is not None:
                    caused_by_id = issue_id_by_global_index.get(global_idx)
                trans_rows.append({
                    "audit_report_id": report_id,
                    "type": t.get("type", "name"),
                    "service_name": t.get("serviceName", ""),
                    "before_text": t.get("before", ""),
                    "after_text": t.get("after", ""),
                    "reason": t.get("reason", ""),
                    "impact_score": max(1, min(10, _coerce_int(t.get("impactScore", 5), 5))),
                    "sort_order": i,
                    "caused_by_issue_id": caused_by_id,
                })
            res = self.client.table("audit_transformations").insert(trans_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_transformations")
            else:
                inserted_trans_rows = res.data
                linked_count = sum(
                    1 for row in inserted_trans_rows if row.get("caused_by_issue_id")
                )
                logger.info(
                    "Inserted %d audit_transformations (%d with caused_by_issue_id)",
                    len(trans_rows), linked_count,
                )

        # After transformations are in the DB, populate the reverse index:
        # audit_issues.resolved_by_transformation_ids gets the list of
        # integer transformation IDs that point back to each issue. This
        # denormalization lets the frontend render "rozwiązane przez N
        # transformacji" without a join.
        if inserted_trans_rows:
            from collections import defaultdict
            resolved_map: dict[int, list[int]] = defaultdict(list)
            for trans_row in inserted_trans_rows:
                caused_by = trans_row.get("caused_by_issue_id")
                trans_id = trans_row.get("id")
                if isinstance(caused_by, int) and isinstance(trans_id, int):
                    resolved_map[caused_by].append(trans_id)
            for issue_id, trans_ids in resolved_map.items():
                try:
                    self.client.table("audit_issues").update(
                        {"resolved_by_transformation_ids": trans_ids}
                    ).eq("id", issue_id).execute()
                except Exception as e:
                    logger.warning(
                        "Failed to update audit_issues.resolved_by_transformation_ids for id=%s: %s",
                        issue_id, e,
                    )
            if resolved_map:
                logger.info(
                    "Linked %d issues with resolved_by_transformation_ids back-references",
                    len(resolved_map),
                )

        # SEO Keywords
        seo_keywords = report.get("missingSeoKeywords", [])
        if seo_keywords:
            seo_rows = [
                {
                    "audit_report_id": report_id,
                    "keyword": kw.get("keyword", ""),
                    "search_volume": kw.get("searchVolume", "medium"),
                    "suggested_placement": kw.get("suggestedPlacement", ""),
                    "reason": kw.get("reason"),
                    "sort_order": i,
                }
                for i, kw in enumerate(seo_keywords)
            ]
            res = self.client.table("audit_seo_keywords").insert(seo_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_seo_keywords")
            else:
                logger.info("Inserted %d audit_seo_keywords", len(seo_rows))

        # Quick Wins
        quick_wins = report.get("quickWins", [])
        if quick_wins:
            qw_rows = [
                {
                    "audit_report_id": report_id,
                    "action": qw.get("action", ""),
                    "effort": qw.get("effort", "medium"),
                    "impact": qw.get("impact", "medium"),
                    "example": qw.get("example", ""),
                    "affected_services": _coerce_int(qw.get("affectedServices", 0)),
                    "sort_order": i,
                }
                for i, qw in enumerate(quick_wins)
            ]
            res = self.client.table("audit_quick_wins").insert(qw_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_quick_wins")
            else:
                logger.info("Inserted %d audit_quick_wins", len(qw_rows))

        # Competitors
        competitors = report.get("competitors", []) or []
        if competitors:
            comp_rows = [
                {
                    "audit_report_id": report_id,
                    "name": c.get("name", ""),
                    "lat": c.get("lat", 0),
                    "lng": c.get("lng", 0),
                    "distance_km": c.get("distanceKm", 0),
                    "reviews_rank": c.get("reviewsRank"),
                    "reviews_count": c.get("reviewsCount", 0),
                    "service_count": c.get("serviceCount", 0),
                    "pricing_level": c.get("pricingLevel"),
                    "booksy_id": c.get("booksyId", 0),
                    "thumbnail_photo": c.get("thumbnailPhoto"),
                    "competition_score": c.get("competitionScore", 0),
                    "city": c.get("city"),
                    "sort_order": i,
                }
                for i, c in enumerate(competitors)
            ]
            res = self.client.table("audit_competitors").insert(comp_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_competitors")
            else:
                logger.info("Inserted %d audit_competitors", len(comp_rows))

        logger.info(
            "Report saved: score=%d, issues=%d, transformations=%d, seo=%d, quickwins=%d, competitors=%d",
            report.get("totalScore", 0), len(issues), len(transformations),
            len(seo_keywords), len(quick_wins), len(competitors),
        )
        return report_id

    async def get_benchmarks(
        self,
        city: str | None = None,
        primary_category_id: int | None = None,
    ) -> dict:
        """Get industry comparison data from the benchmarks table.

        The benchmarks table is keyed by (scope, metric). Supported scopes
        (populated by compute_all_benchmarks Postgres RPC):
          - 'national'        — default benchmark across all salons
          - 'city:<lowered>'  — per-city (only cities with 10+ salons)
          - 'category:<id>'   — per Booksy primary_category_id (50+ salons)

        Metrics available: description_rate, avg_service_count,
        avg_category_count, composite_score, fixed_price_rate,
        duration_coverage_rate.

        Priority lookup: city → category → national → hardcoded defaults.
        Returns a dict with both the legacy keys expected by report.py
        (industry_average, top_performers, sample_size) and the raw
        metric values for richer comparisons.
        """
        scopes_to_try: list[str] = []
        if city:
            scopes_to_try.append(f"city:{city.lower()}")
        if primary_category_id is not None:
            scopes_to_try.append(f"category:{primary_category_id}")
        scopes_to_try.append("national")

        for scope in scopes_to_try:
            try:
                result = (
                    self.client.table("benchmarks")
                    .select("metric, value, sample_size")
                    .eq("scope", scope)
                    .execute()
                )
                if not result.data:
                    continue
                metrics = {row["metric"]: row for row in result.data}
                if not metrics:
                    continue

                description_rate = metrics.get("description_rate", {}).get("value")
                composite_score = metrics.get("composite_score", {}).get("value")
                avg_service_count = metrics.get("avg_service_count", {}).get("value")
                avg_category_count = metrics.get("avg_category_count", {}).get("value")
                fixed_price_rate = metrics.get("fixed_price_rate", {}).get("value")
                duration_coverage_rate = metrics.get("duration_coverage_rate", {}).get("value")

                # Prefer composite_score as the "industry average" signal
                # because it's the cross-metric aggregate used by bagent scoring.
                # Fall back to description_rate if composite not available at
                # this scope, then to 52 as an absolute floor.
                if composite_score is not None:
                    industry_avg = float(composite_score)
                elif description_rate is not None:
                    industry_avg = float(description_rate)
                else:
                    industry_avg = 52.0

                # Use the largest sample_size from any metric in this scope
                sample_size = max(
                    (int(row.get("sample_size") or 0) for row in result.data),
                    default=500,
                )

                return {
                    "industry_average": round(industry_avg, 1),
                    "top_performers": round(min(industry_avg + 20, 100.0), 1),
                    "sample_size": sample_size,
                    "composite_score": float(composite_score) if composite_score is not None else None,
                    "description_rate": float(description_rate) if description_rate is not None else None,
                    "avg_service_count": float(avg_service_count) if avg_service_count is not None else None,
                    "avg_category_count": float(avg_category_count) if avg_category_count is not None else None,
                    "fixed_price_rate": float(fixed_price_rate) if fixed_price_rate is not None else None,
                    "duration_coverage_rate": float(duration_coverage_rate) if duration_coverage_rate is not None else None,
                    "scope": scope,
                }
            except Exception as e:
                logger.warning("Failed to fetch benchmarks for scope=%s: %s", scope, e)
                continue

        return {"industry_average": 52, "top_performers": 78, "sample_size": 500}

    async def get_competitors(
        self, lat: float, lng: float, radius_km: int, service_names: list[str]
    ) -> list[dict]:
        """Get competitor salons within radius via RPC."""
        try:
            result = self.client.rpc(
                "get_nearby_salons",
                {"p_lat": lat, "p_lng": lng, "p_radius_km": radius_km, "p_limit": 20},
            ).execute()
            return result.data or []
        except Exception as e:
            logger.warning("Failed to fetch competitors: %s", e)
            return []

    async def call_rpc(self, rpc_name: str, params: dict) -> list[dict]:
        """Generic RPC caller. Returns list of dicts or empty list on error."""
        try:
            result = self.client.rpc(rpc_name, params).execute()
            data = result.data
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except Exception as e:
            logger.warning("RPC %s failed: %s", rpc_name, e)
            return []

    async def geocode_salon(self, salon_name: str | None, address: str | None) -> dict | None:
        """Find salon coordinates by name/address match. Returns {lat, lng} or None."""
        if not salon_name and not address:
            return None
        try:
            query = self.client.table("salons").select("lat, lng, name, address")
            if salon_name:
                query = query.ilike("name", f"%{salon_name}%")
            result = query.limit(5).execute()
            if result.data:
                for row in result.data:
                    if row.get("lat") and row.get("lng"):
                        return {"lat": row["lat"], "lng": row["lng"]}
        except Exception as e:
            logger.warning("Failed to geocode salon: %s", e)
        return None

    async def get_salon_with_services(self, salon_id: int) -> dict:
        """Load salon metadata + services + categories for a Booksy salon ID.
        Returns: {salon: {...} | None, services: [...], categories: [...]}
        Always returns a dict (never None). salon=None means not found."""
        try:
            salon_result = self.client.table("salons").select("*").eq("booksy_id", salon_id).limit(1).execute()
            if not salon_result.data:
                return {"salon": None, "services": [], "categories": []}
            salon = salon_result.data[0]

            services_result = self.client.table("services").select("*").eq("salon_id", salon["id"]).execute()
            services = services_result.data or []
            categories = sorted({s.get("category", "") for s in services if s.get("category")})

            return {"salon": salon, "services": services, "categories": categories}
        except Exception as e:
            logger.warning("Failed to load salon %d: %s", salon_id, e)
            return {"salon": None, "services": [], "categories": []}

    async def get_salon_basic(self, salon_id: int) -> dict | None:
        """Load just salon metadata (no services)."""
        try:
            result = self.client.table("salons").select("*").eq("booksy_id", salon_id).limit(1).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.warning("Failed to load salon basic %d: %s", salon_id, e)
            return None

    async def call_rpc(self, rpc_name: str, params: dict) -> list[dict]:
        """Generic RPC caller. Returns list of dicts or empty list on error."""
        try:
            result = self.client.rpc(rpc_name, params).execute()
            return result.data or []
        except Exception as e:
            logger.warning("RPC %s failed: %s", rpc_name, e)
            return []

    async def save_competitor_report(
        self,
        convex_audit_id: str,
        convex_user_id: str,
        subject_booksy_id: int,
        report_data: dict,
        competitor_count: int = 0,
    ) -> int:
        """Save competitor report to competitor_reports table.

        subject_salon_id is FK to salons.id (internal), so we translate
        from booksy_id first.
        """
        # Translate booksy_id → internal salon id
        salon = await self.get_salon_basic(subject_booksy_id)
        internal_id = salon["id"] if salon else None

        row = {
            "convex_audit_id": convex_audit_id,
            "convex_user_id": convex_user_id,
            "subject_salon_id": internal_id,
            "report_data": report_data,
            "status": "completed",
            "competitor_count": competitor_count,
            "metadata": {"aiProvider": "minimax", "pipeline": "bagent-v2"},
        }
        result = self.client.table("competitor_reports").upsert(row, on_conflict="convex_audit_id").execute()
        if not result.data:
            raise ValueError("Failed to save competitor report")
        return result.data[0]["id"]

    async def save_optimized_pricelist(
        self,
        convex_audit_id: str,
        optimization_data: dict,
        salon_name: str = "",
    ) -> int:
        """Save optimized pricelist to optimized_pricelists + children.

        Writes the full normalized tree:
          - optimized_pricelists (header, upsert on convex_audit_id)
          - optimized_categories (N rows, delete+insert for idempotency)
          - optimized_services   (M rows with source_scrape_service_id FK
            and Booksy canonical taxonomy propagated from salon_scrape_services
            so optimized_services has full parity with the scrape tables.
            No rozjazd between "original" and "optimized" data.)

        The optimization_data.pricelist.categories[].services[] dicts carry
        provenance fields from the pipeline: scrape_service_id, canonical_id,
        booksy_treatment_id, body_part, etc. — see pipelines/cennik.py.
        """
        # 0. Find the source_scrape_id to link pricelist -> salon_scrapes
        source_scrape = (
            self.client.table("salon_scrapes")
            .select("id")
            .eq("convex_audit_id", convex_audit_id)
            .order("scraped_at", desc=True)
            .limit(1)
            .execute()
        )
        source_scrape_id = source_scrape.data[0]["id"] if source_scrape.data else None

        summary = optimization_data.get("summary", {})
        pricelist = optimization_data.get("pricelist") or {}
        categories = pricelist.get("categories") or []
        total_optimized_services = sum(len(c.get("services") or []) for c in categories)

        # 1. Upsert optimized_pricelists header row
        header_row = {
            "convex_audit_id": convex_audit_id,
            "salon_name": salon_name or None,
            "quality_score": _coerce_int(optimization_data.get("qualityScore", 0)),
            "total_changes": _coerce_int(summary.get("totalChanges", 0)),
            "names_improved": _coerce_int(summary.get("namesImproved", 0)),
            "descriptions_added": _coerce_int(summary.get("descriptionsAdded", 0)),
            "categories_restructured": _coerce_int(summary.get("categoriesOptimized", 0)),
            "duplicates_merged": _coerce_int(summary.get("duplicatesFound", 0)),
            "optimized_service_count": total_optimized_services,
            "pipeline_version": "v2-bagent",
            "source_scrape_id": source_scrape_id,
        }
        result = (
            self.client.table("optimized_pricelists")
            .upsert(header_row, on_conflict="convex_audit_id")
            .execute()
        )
        if not result.data:
            raise ValueError("Failed to save optimized_pricelists header")
        pricelist_id = result.data[0]["id"]

        # 2. Delete existing categories + services for this pricelist (idempotent).
        # optimized_categories.pricelist_id cascades to optimized_services.category_id.
        self.client.table("optimized_categories").delete().eq("pricelist_id", pricelist_id).execute()

        # 3. Insert categories and services
        total_services_inserted = 0
        total_categories_inserted = 0
        for cat_idx, cat in enumerate(categories):
            cat_name = cat.get("name") or "Bez kategorii"
            cat_row = {
                "pricelist_id": pricelist_id,
                "name": cat_name,
                "normalized_name": _normalize_name(cat_name),
                "sort_order": cat_idx,
            }
            cat_result = self.client.table("optimized_categories").insert(cat_row).execute()
            if not cat_result.data:
                logger.warning("Failed to insert optimized_categories row for %r", cat_name)
                continue
            category_id = cat_result.data[0]["id"]
            total_categories_inserted += 1

            svc_rows = []
            for svc_idx, svc in enumerate(cat.get("services") or []):
                name = svc.get("name", "")
                svc_rows.append({
                    "pricelist_id": pricelist_id,
                    "category_id": category_id,
                    "convex_audit_id": convex_audit_id,

                    # Core identity
                    "name": name,
                    "normalized_name": _normalize_name(name),
                    "description": svc.get("description"),
                    "price": svc.get("price"),
                    "duration": svc.get("duration"),
                    "sort_order": svc_idx,

                    # Provenance — link back to the source scrape service row
                    "source_scrape_service_id": svc.get("scrape_service_id"),

                    # Booksy canonical taxonomy (propagated from scrape)
                    "booksy_id": None,  # business-level; not per-service
                    "canonical_id": svc.get("canonical_id"),
                    "booksy_treatment_id": svc.get("booksy_treatment_id"),
                    "booksy_service_id": svc.get("booksy_service_id"),
                    "treatment_name": svc.get("treatment_name"),
                    "treatment_parent_id": svc.get("treatment_parent_id"),
                    "body_part": svc.get("body_part"),
                    "target_gender": svc.get("target_gender"),
                    "technology": svc.get("technology"),
                    "classification_confidence": svc.get("classification_confidence"),

                    # Parsed pricing/duration (preserved from scrape)
                    "price_grosze": svc.get("price_grosze"),
                    "is_from_price": svc.get("is_from_price"),
                    "duration_minutes": svc.get("duration_minutes"),

                    # Variants JSONB (may have been transformed)
                    "variants": svc.get("variants"),

                    # Change-tracking flags — set by cennik pipeline
                    "was_renamed": bool(svc.get("_was_renamed")),
                    "was_description_changed": bool(svc.get("_was_description_changed")),
                    "was_recategorized": bool(svc.get("_was_recategorized")),
                    "was_seo_enriched": bool(svc.get("_was_seo_enriched")),
                    "was_price_normalized": bool(svc.get("_was_price_normalized")),
                    "is_new_service": bool(svc.get("_is_new_service")),
                    "was_deduplicated": bool(svc.get("_was_deduplicated")),

                    # Original snapshot (for Lista zmian diff view)
                    "original_description": svc.get("_original_description"),
                    "original_category": svc.get("_original_category"),
                    "original_price": svc.get("_original_price"),
                })

            if svc_rows:
                CHUNK = 200
                for i in range(0, len(svc_rows), CHUNK):
                    chunk = svc_rows[i : i + CHUNK]
                    res = self.client.table("optimized_services").insert(chunk).execute()
                    if not res.data:
                        logger.warning(
                            "Failed to insert optimized_services chunk %d-%d for category %r",
                            i, i + len(chunk), cat_name,
                        )
                    else:
                        total_services_inserted += len(chunk)

        logger.info(
            "Saved optimized pricelist: id=%s, categories=%d, services=%d, source_scrape=%s",
            pricelist_id, total_categories_inserted, total_services_inserted, source_scrape_id,
        )
        return pricelist_id

    # ---- Competitor selection read helpers (Comp Etap 1) -----------------

    async def get_subject_salon_for_audit(self, convex_audit_id: str) -> dict | None:
        """Load the subject salon's full context for competitor selection.

        Joins the latest salon_scrapes row (for the given convex_audit_id)
        with the salons table (by booksy_id) and returns a flat dict with
        the fields competitor selection needs: salon_id (internal PK),
        booksy_id, name, city, salon_lat, salon_lng, primary_category_id,
        business_categories (jsonb from the scrape), reviews_count,
        reviews_rank, partner_system.

        Returns None if there is no scrape for this audit_id. Never raises —
        caller decides how to handle missing subjects.
        """
        scrape_res = (
            self.client.table("salon_scrapes")
            .select(
                "id,booksy_id,salon_name,salon_city,salon_lat,salon_lng,"
                "primary_category_id,business_categories,reviews_count,"
                "reviews_rank,partner_system,scraped_at"
            )
            .eq("convex_audit_id", convex_audit_id)
            .order("scraped_at", desc=True)
            .limit(1)
            .execute()
        )
        if not scrape_res.data:
            return None
        scrape = scrape_res.data[0]

        # Translate booksy_id -> salons.id (internal PK). This is required so
        # downstream queries on salon_top_services (keyed by salon_id) work.
        salon_res = (
            self.client.table("salons")
            .select("id")
            .eq("booksy_id", scrape["booksy_id"])
            .limit(1)
            .execute()
        )
        salon_id = salon_res.data[0]["id"] if salon_res.data else None
        if salon_id is None:
            logger.warning(
                "Subject salon scrape %s exists but no salons row for booksy_id=%s",
                scrape["id"], scrape["booksy_id"],
            )
            return None

        return {
            "salon_id": salon_id,
            "booksy_id": scrape["booksy_id"],
            "name": scrape.get("salon_name"),
            "city": scrape.get("salon_city"),
            "salon_lat": scrape.get("salon_lat"),
            "salon_lng": scrape.get("salon_lng"),
            "primary_category_id": scrape.get("primary_category_id"),
            "business_categories": scrape.get("business_categories") or [],
            "reviews_count": scrape.get("reviews_count") or 0,
            "reviews_rank": scrape.get("reviews_rank"),
            "partner_system": scrape.get("partner_system") or "native",
            "scraped_at": scrape.get("scraped_at"),
        }

    async def get_candidate_salons(
        self,
        *,
        lat: float,
        lng: float,
        primary_category_id: int,
        radius_km: float,
        exclude_booksy_id: int,
        limit: int = 200,
    ) -> list[dict]:
        """Return candidate salons within radius_km of (lat, lng) in the
        given primary_category_id, excluding the subject.

        Uses the PostGIS find_nearby_salons RPC (tytan Supabase) which does
        ST_Distance on the salons.geom column and filters by category. The
        RPC returns distance_m — we convert to distance_km for the caller
        and drop the subject row.

        Returns a list of dicts with keys: salon_id, booksy_id, name, city,
        distance_km, reviews_rank, reviews_count.
        """
        try:
            result = self.client.rpc(
                "find_nearby_salons",
                {
                    "p_category_id": primary_category_id,
                    "p_lat": lat,
                    "p_lng": lng,
                    "p_radius_m": int(radius_km * 1000),
                    "p_limit": limit,
                },
            ).execute()
        except Exception as e:
            logger.warning(
                "find_nearby_salons RPC failed (cat=%s, lat=%s, lng=%s): %s",
                primary_category_id, lat, lng, e,
            )
            return []

        rows: list[dict] = []
        for row in result.data or []:
            bid = row.get("booksy_id")
            if bid is None or bid == exclude_booksy_id:
                continue
            distance_m = row.get("distance_m") or 0
            rows.append({
                "salon_id": row.get("id"),
                "booksy_id": bid,
                "name": row.get("name") or "",
                "city": row.get("city"),
                "distance_km": float(distance_m) / 1000.0,
                "reviews_rank": row.get("reviews_rank"),
                "reviews_count": row.get("reviews_count") or 0,
                "service_count": row.get("service_count") or 0,
            })
        return rows

    async def get_salon_top_services(self, salon_id: int) -> list[dict]:
        """Return the top_services rows for a given internal salon_id.

        Each row is a dict with the columns persisted by the ingester:
        booksy_service_id, booksy_treatment_id, name, sort_order, variants, etc.
        Returns empty list when the salon has no top_services (~28% of salons).
        """
        try:
            result = (
                self.client.table("salon_top_services")
                .select("booksy_service_id,booksy_treatment_id,name,sort_order")
                .eq("salon_id", salon_id)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load salon_top_services for salon_id=%s: %s", salon_id, e)
            return []
        return list(result.data or [])

    async def get_latest_top_services_for_salon_ids(
        self, salon_ids: list[int],
    ) -> dict[int, list[dict]]:
        """Bulk load salon_top_services for many candidate salons at once.

        Returns a {salon_id: [top_services_rows]} map. Candidates with no
        top_services rows are simply absent from the map (callers should
        .get(sid, []) for safety).
        """
        if not salon_ids:
            return {}
        try:
            result = (
                self.client.table("salon_top_services")
                .select("salon_id,booksy_service_id,booksy_treatment_id,name,sort_order")
                .in_("salon_id", salon_ids)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load bulk salon_top_services (n=%d): %s",
                len(salon_ids), e,
            )
            return {}
        by_salon: dict[int, list[dict]] = {}
        for row in result.data or []:
            sid = row.get("salon_id")
            if sid is None:
                continue
            by_salon.setdefault(sid, []).append(row)
        return by_salon

    async def get_latest_business_categories_for_booksy_ids(
        self, booksy_ids: list[int],
    ) -> dict[int, list[dict]]:
        """Bulk load the latest business_categories jsonb per booksy_id.

        salon_scrapes is time-series (multiple rows per booksy_id). We pull
        all rows for the given booksy_ids in one query, then pick the most
        recent row per booksy_id client-side (sorted by scraped_at DESC).
        The business_categories payload is a list of {id, name, female_weight}
        dicts as ingested by scripts/ingest_salon_jsons.py.

        Returns a {booksy_id: business_categories_list} map. Missing ids are
        absent from the map.
        """
        if not booksy_ids:
            return {}
        try:
            result = (
                self.client.table("salon_scrapes")
                .select("booksy_id,business_categories,scraped_at")
                .in_("booksy_id", booksy_ids)
                .order("scraped_at", desc=True)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load bulk business_categories (n=%d): %s",
                len(booksy_ids), e,
            )
            return {}
        latest: dict[int, list[dict]] = {}
        for row in result.data or []:
            bid = row.get("booksy_id")
            if bid is None or bid in latest:
                continue  # first occurrence wins (already sorted desc)
            bc = row.get("business_categories") or []
            latest[bid] = bc
        return latest

    async def get_latest_partner_system_for_booksy_ids(
        self, booksy_ids: list[int],
    ) -> dict[int, str]:
        """Bulk load the latest partner_system ('native' or 'versum') per booksy_id.

        Used so the CompetitorCandidate carries its source system — useful
        for downstream UI filtering and for skipping versum salons in the
        pricing comparison step (since their treatment_id coverage is poor).

        Returns a {booksy_id: partner_system_string} map. Rows with NULL
        partner_system default to 'native'.
        """
        if not booksy_ids:
            return {}
        try:
            result = (
                self.client.table("salon_scrapes")
                .select("booksy_id,partner_system,scraped_at")
                .in_("booksy_id", booksy_ids)
                .order("scraped_at", desc=True)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load bulk partner_system (n=%d): %s",
                len(booksy_ids), e,
            )
            return {}
        latest: dict[int, str] = {}
        for row in result.data or []:
            bid = row.get("booksy_id")
            if bid is None or bid in latest:
                continue
            latest[bid] = row.get("partner_system") or "native"
        return latest

    async def get_business_categories_with_female_weight(self) -> dict[int, int]:
        """Return a {category_id: female_weight} map for the whole reference table.

        Useful when a candidate's business_categories jsonb is missing the
        female_weight field (possible in older scrapes before the ingester
        started preserving it inline). Callers can look up the id in this
        map to recompute the average.
        """
        try:
            result = (
                self.client.table("business_categories")
                .select("id,female_weight")
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load business_categories reference: %s", e)
            return {}
        result_map: dict[int, int] = {}
        for row in result.data or []:
            cid = row.get("id")
            fw = row.get("female_weight")
            if isinstance(cid, int) and isinstance(fw, (int, float)):
                result_map[cid] = int(fw)
        return result_map

    # ---- Competitor analysis write helpers (Comp Etap 4) ----------------

    async def create_competitor_report(
        self,
        *,
        convex_audit_id: str,
        convex_user_id: str,
        subject_salon_id: int,
        tier: str,
        selection_mode: str,
        competitor_count: int,
        metadata: dict | None = None,
    ) -> int:
        """Upsert a competitor_reports row and return its integer id.

        Uses UPSERT on convex_audit_id so re-running Comp Etap 4 for the
        same audit replaces the previous report header atomically. Children
        (competitor_matches, pricing_comparisons, service_gaps,
        dimensional_scores) are FK-cascaded via ON DELETE CASCADE — caller
        should delete them explicitly before re-inserting to keep the DB
        clean (we do this in compute_competitor_analysis).
        """
        row = {
            "convex_audit_id": convex_audit_id,
            "convex_user_id": convex_user_id,
            "subject_salon_id": subject_salon_id,
            "report_data": {},
            "status": "processing",
            "competitor_count": competitor_count,
            "tier": tier,
            "selection_mode": selection_mode,
            "metadata": metadata or {},
        }
        result = (
            self.client.table("competitor_reports")
            .upsert(row, on_conflict="convex_audit_id")
            .execute()
        )
        if not result.data:
            raise ValueError(
                f"Failed to create competitor_reports row for audit={convex_audit_id}"
            )
        return int(result.data[0]["id"])

    async def delete_competitor_report_children(self, report_id: int) -> None:
        """Delete all child rows for a competitor_reports row.

        Called before re-computing so idempotent re-runs don't duplicate
        matches/pricing/gaps/dimensions. The ON DELETE CASCADE also handles
        this automatically, but we delete explicitly because we keep the
        header row (upsert by convex_audit_id).
        """
        for table in (
            "competitor_matches",
            "competitor_pricing_comparisons",
            "competitor_service_gaps",
            "competitor_dimensional_scores",
        ):
            try:
                self.client.table(table).delete().eq("report_id", report_id).execute()
            except Exception as e:
                logger.warning(
                    "Failed to delete %s for report_id=%s: %s", table, report_id, e,
                )

    async def insert_competitor_matches(
        self, report_id: int, candidates: list[Any],
    ) -> int:
        """Insert competitor_matches rows from a list of CompetitorCandidate.

        The `composite_score` column is numeric(5,4) (max 9.9999) so we
        divide Etap 1's raw 0-100ish score by 100 before persisting. The
        display layer multiplies by 100 to show the human-readable score.
        """
        if not candidates:
            return 0
        rows = []
        for idx, c in enumerate(candidates):
            rows.append({
                "report_id": report_id,
                "competitor_salon_id": c.salon_id,
                "composite_score": round(float(c.composite_score) / 100.0, 4),
                "bucket": c.bucket,
                "counts_in_aggregates": bool(c.counts_in_aggregates),
                "similarity_scores": c.similarity_scores or {},
                "distance_km": round(float(c.distance_km), 3),
                "sort_order": idx,
            })
        result = self.client.table("competitor_matches").insert(rows).execute()
        return len(result.data or [])

    async def insert_competitor_pricing_comparisons(
        self, rows: list[dict],
    ) -> int:
        """Batch-insert competitor_pricing_comparisons rows. Returns count."""
        if not rows:
            return 0
        result = (
            self.client.table("competitor_pricing_comparisons").insert(rows).execute()
        )
        return len(result.data or [])

    async def insert_competitor_service_gaps(self, rows: list[dict]) -> int:
        """Batch-insert competitor_service_gaps rows. Returns count."""
        if not rows:
            return 0
        result = self.client.table("competitor_service_gaps").insert(rows).execute()
        return len(result.data or [])

    async def insert_competitor_dimensional_scores(self, rows: list[dict]) -> int:
        """Batch-insert competitor_dimensional_scores rows. Returns count."""
        if not rows:
            return 0
        result = (
            self.client.table("competitor_dimensional_scores").insert(rows).execute()
        )
        return len(result.data or [])

    async def update_competitor_report_status(
        self,
        report_id: int,
        status: str,
        metadata_extras: dict | None = None,
    ) -> None:
        """Update competitor_reports.status. Optionally merge extras into metadata."""
        payload: dict[str, Any] = {"status": status}
        if metadata_extras:
            # Fetch existing metadata first so we merge not replace
            existing = (
                self.client.table("competitor_reports")
                .select("metadata")
                .eq("id", report_id)
                .limit(1)
                .execute()
            )
            current_meta = {}
            if existing.data and isinstance(existing.data[0].get("metadata"), dict):
                current_meta = existing.data[0]["metadata"]
            payload["metadata"] = {**current_meta, **metadata_extras}
        self.client.table("competitor_reports").update(payload).eq(
            "id", report_id,
        ).execute()

    # ---- Competitor analysis read helpers (Comp Etap 4) -----------------

    async def get_subject_full_data(self, convex_audit_id: str) -> dict[str, Any]:
        """Load all data needed to compute dimensional scores for the subject.

        Returns {scrape, services, reviews, top_services, open_hours} where
        `scrape` has the flat columns plus structural fields extracted from
        raw_response (open_hours, facebook_url, etc.) when the top-level
        column is NULL (common for audit-triggered scrapes that ran before
        Etap 0.1 extended the column set).

        Raises ValueError when no scrape exists for the audit_id.
        """
        scrape_res = (
            self.client.table("salon_scrapes")
            .select(
                "id,booksy_id,salon_name,salon_description,salon_lat,salon_lng,"
                "reviews_count,reviews_rank,partner_system,scraped_at,website,"
                "facebook_url,instagram_url,booking_max_modification_time,"
                "booking_max_lead_time,deposit_cancel_days,pos_pay_by_app,"
                "pos_market_pay,has_online_services,has_online_vouchers,"
                "has_safety_rules,salon_subdomain,raw_response,primary_category_id,"
                "business_categories"
            )
            .eq("convex_audit_id", convex_audit_id)
            .order("scraped_at", desc=True)
            .limit(1)
            .execute()
        )
        if not scrape_res.data:
            raise ValueError(f"No salon_scrapes for convex_audit_id={convex_audit_id}")
        scrape = scrape_res.data[0]
        scrape_id = scrape["id"]
        booksy_id = scrape["booksy_id"]

        # Translate booksy_id → salons.id (internal) for joins to salon_reviews etc.
        salon_res = (
            self.client.table("salons")
            .select("id,description,reviews_count,reviews_rank,"
                    "facebook_url,instagram_url,website,phone,email")
            .eq("booksy_id", booksy_id)
            .limit(1)
            .execute()
        )
        salon_row = salon_res.data[0] if salon_res.data else {}
        salon_id = salon_row.get("id") if salon_row else None

        # Enrich the scrape dict: when a structured column is NULL, try to
        # pull it out of raw_response.business (the audit flow didn't
        # populate these columns, only the batch ingester did).
        self._enrich_scrape_from_raw(scrape)

        # Prefer salons.* values when scrape's version is missing
        for key in ("reviews_count", "reviews_rank", "facebook_url", "instagram_url", "website"):
            if scrape.get(key) in (None, "") and salon_row.get(key):
                scrape[key] = salon_row[key]
        if not scrape.get("salon_description") and salon_row.get("description"):
            scrape["salon_description"] = salon_row["description"]

        services = self._load_services_for_scrape(scrape_id)
        reviews = self._load_reviews_for_salon(salon_id) if salon_id else []
        top_services = self._load_top_services_for_salon(salon_id) if salon_id else []

        return {
            "salon_id": salon_id,
            "booksy_id": booksy_id,
            "scrape": scrape,
            "services": services,
            "reviews": reviews,
            "top_services": top_services,
            "partner_system": scrape.get("partner_system") or "native",
        }

    async def get_competitor_full_data(
        self, booksy_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """Bulk-load full data for multiple competitors keyed by booksy_id.

        For each booksy_id, returns the same shape as get_subject_full_data
        (minus convex_audit_id linkage). Uses the LATEST salon_scrapes row
        per booksy_id (ordered by scraped_at DESC). Salons that have no
        scrape are absent from the result map.
        """
        if not booksy_ids:
            return {}

        # Fetch ALL scrapes for these booksy_ids (may be multiple per salon)
        # then pick the most recent one client-side.
        scrape_res = (
            self.client.table("salon_scrapes")
            .select(
                "id,booksy_id,salon_name,salon_description,salon_lat,salon_lng,"
                "reviews_count,reviews_rank,partner_system,scraped_at,website,"
                "facebook_url,instagram_url,booking_max_modification_time,"
                "booking_max_lead_time,deposit_cancel_days,pos_pay_by_app,"
                "pos_market_pay,has_online_services,has_online_vouchers,"
                "has_safety_rules,salon_subdomain,raw_response,primary_category_id,"
                "business_categories"
            )
            .in_("booksy_id", booksy_ids)
            .order("scraped_at", desc=True)
            .execute()
        )
        latest_per_booksy: dict[int, dict[str, Any]] = {}
        for row in scrape_res.data or []:
            bid = row.get("booksy_id")
            if bid is not None and bid not in latest_per_booksy:
                latest_per_booksy[bid] = row

        # Resolve booksy_id → salons.id for reviews/top_services joins
        salon_res = (
            self.client.table("salons")
            .select("id,booksy_id,description,reviews_count,reviews_rank,"
                    "facebook_url,instagram_url,website")
            .in_("booksy_id", booksy_ids)
            .execute()
        )
        booksy_to_salon: dict[int, dict[str, Any]] = {}
        for s in salon_res.data or []:
            if s.get("booksy_id") is not None:
                booksy_to_salon[s["booksy_id"]] = s

        result: dict[int, dict[str, Any]] = {}
        for bid, scrape in latest_per_booksy.items():
            scrape_id = scrape["id"]
            salon_row = booksy_to_salon.get(bid) or {}
            salon_id = salon_row.get("id")

            self._enrich_scrape_from_raw(scrape)

            # Fallback to salons.* for missing fields
            for key in ("reviews_count", "reviews_rank", "facebook_url", "instagram_url", "website"):
                if scrape.get(key) in (None, "") and salon_row.get(key):
                    scrape[key] = salon_row[key]
            if not scrape.get("salon_description") and salon_row.get("description"):
                scrape["salon_description"] = salon_row["description"]

            services = self._load_services_for_scrape(scrape_id)
            reviews = self._load_reviews_for_salon(salon_id) if salon_id else []
            top_services = self._load_top_services_for_salon(salon_id) if salon_id else []

            result[bid] = {
                "salon_id": salon_id,
                "booksy_id": bid,
                "scrape": scrape,
                "services": services,
                "reviews": reviews,
                "top_services": top_services,
                "partner_system": scrape.get("partner_system") or "native",
            }
        return result

    def _enrich_scrape_from_raw(self, scrape: dict[str, Any]) -> None:
        """Mutate scrape in-place: pull structured fields out of raw_response
        when the top-level columns are NULL.

        The batch ingester populates the extra columns directly, but
        audit-flow scrapes (written by the convex pipeline) leave them NULL.
        This helper is the single place where we fall back to raw_response
        so callers see a consistent shape either way. Also extracts
        `open_hours` which is not a materialized column at all.
        """
        raw = scrape.get("raw_response")
        if not isinstance(raw, dict):
            scrape["open_hours"] = []
            return
        business = raw.get("business") if isinstance(raw.get("business"), dict) else raw

        # open_hours is a list of {day_of_week, open_from, open_till}
        scrape["open_hours"] = business.get("open_hours") or []

        # Backfill NULL structured columns from raw_response
        def _fill_if_none(column_name: str, raw_key: str) -> None:
            if scrape.get(column_name) is None:
                val = business.get(raw_key)
                if val is not None:
                    scrape[column_name] = val

        _fill_if_none("booking_max_modification_time", "booking_max_modification_time")
        _fill_if_none("booking_max_lead_time", "booking_max_lead_time")
        deposit = business.get("deposit_cancel_time")
        if scrape.get("deposit_cancel_days") is None and isinstance(deposit, dict):
            scrape["deposit_cancel_days"] = deposit.get("days")
        _fill_if_none("pos_pay_by_app", "pos_pay_by_app_enabled")
        _fill_if_none("has_online_services", "has_online_services")
        _fill_if_none("has_online_vouchers", "has_online_vouchers")
        _fill_if_none("has_safety_rules", "has_safety_rules")
        _fill_if_none("website", "website")
        _fill_if_none("facebook_url", "facebook_link")
        _fill_if_none("instagram_url", "instagram_link")

        if scrape.get("salon_description") is None:
            desc = business.get("description")
            if desc:
                scrape["salon_description"] = desc

        # Drop raw_response from the dict so downstream code doesn't carry
        # megabytes of unused JSON. The structured columns above are all
        # that the dimension computation needs.
        scrape.pop("raw_response", None)

    def _load_services_for_scrape(self, scrape_id: Any) -> list[dict[str, Any]]:
        """Load all salon_scrape_services rows for a given scrape_id."""
        try:
            res = (
                self.client.table("salon_scrape_services")
                .select(
                    "id,category_name,name,booksy_treatment_id,booksy_service_id,"
                    "treatment_name,treatment_parent_id,price_grosze,is_from_price,"
                    "duration_minutes,is_active,is_promo,omnibus_price_grosze,"
                    "description,description_type,photos,combo_type,variants"
                )
                .eq("scrape_id", scrape_id)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load services for scrape %s: %s", scrape_id, e)
            return []
        return list(res.data or [])

    def _load_reviews_for_salon(self, salon_id: int) -> list[dict[str, Any]]:
        """Load salon_reviews rows for a salon_id. Small sample (3-50 rows)."""
        try:
            res = (
                self.client.table("salon_reviews")
                .select(
                    "id,rank,review_text,reply_content,review_created_at,"
                    "services,staff"
                )
                .eq("salon_id", salon_id)
                .order("review_created_at", desc=True)
                .limit(100)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load reviews for salon %s: %s", salon_id, e)
            return []
        return list(res.data or [])

    def _load_top_services_for_salon(self, salon_id: int) -> list[dict[str, Any]]:
        """Load salon_top_services rows for a salon_id."""
        try:
            res = (
                self.client.table("salon_top_services")
                .select(
                    "booksy_service_id,booksy_treatment_id,name,category_name,"
                    "variants,sort_order"
                )
                .eq("salon_id", salon_id)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load top_services for salon %s: %s", salon_id, e)
            return []
        return list(res.data or [])

    async def get_versum_mappings(
        self, salon_ids: list[int],
    ) -> dict[tuple[int, int], int]:
        """Return user-provided Versum service mappings.

        Key: (salon_id, booksy_service_id). Value: mapped_treatment_id.
        Used by Comp Etap 4 to resolve treatment_id for Versum salons
        whose services have NULL booksy_treatment_id. Missing mappings
        simply result in those services being skipped from pricing
        comparisons (graceful degradation per plan doc).
        """
        if not salon_ids:
            return {}
        try:
            res = (
                self.client.table("versum_service_mappings")
                .select("salon_id,booksy_service_id,mapped_treatment_id")
                .in_("salon_id", salon_ids)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load versum_service_mappings for salons=%s: %s",
                salon_ids, e,
            )
            return {}
        out: dict[tuple[int, int], int] = {}
        for row in res.data or []:
            sid = row.get("salon_id")
            bsid = row.get("booksy_service_id")
            tid = row.get("mapped_treatment_id")
            if sid is not None and bsid is not None and tid is not None:
                out[(int(sid), int(bsid))] = int(tid)
        return out

    # ---- Competitor synthesis helpers (Comp Etap 5) ---------------------

    async def get_competitor_report_by_id(
        self, report_id: int,
    ) -> dict[str, Any] | None:
        """Load a competitor_reports row by its integer id.

        Returns None if not found. Used by Comp Etap 5 synthesis to read the
        header (subject_salon_id, tier, metadata) before loading children.
        """
        try:
            res = (
                self.client.table("competitor_reports")
                .select("*")
                .eq("id", report_id)
                .limit(1)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load competitor_reports id=%s: %s", report_id, e)
            return None
        if not res.data:
            return None
        return res.data[0]

    async def get_competitor_matches(self, report_id: int) -> list[dict[str, Any]]:
        """Load competitor_matches for a report with salon identity fields.

        Returns a list enriched with booksy_id / salon_name / reviews_rank /
        reviews_count / distance_km so the synthesis prompt can render human
        context (not just salon PKs).
        """
        try:
            res = (
                self.client.table("competitor_matches")
                .select(
                    "id,competitor_salon_id,composite_score,bucket,"
                    "counts_in_aggregates,similarity_scores,distance_km,sort_order"
                )
                .eq("report_id", report_id)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load competitor_matches for report=%s: %s", report_id, e)
            return []

        matches = list(res.data or [])
        if not matches:
            return []

        salon_ids = [m["competitor_salon_id"] for m in matches if m.get("competitor_salon_id")]
        if not salon_ids:
            return matches

        try:
            salon_res = (
                self.client.table("salons")
                .select("id,booksy_id,name,reviews_rank,reviews_count,city")
                .in_("id", salon_ids)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load salons for competitor_matches: %s", e)
            return matches

        salon_by_id: dict[int, dict[str, Any]] = {
            s["id"]: s for s in (salon_res.data or []) if s.get("id") is not None
        }
        for m in matches:
            sid = m.get("competitor_salon_id")
            s = salon_by_id.get(sid) if sid is not None else None
            if s:
                m["booksy_id"] = s.get("booksy_id")
                m["salon_name"] = s.get("name")
                m["reviews_rank"] = s.get("reviews_rank")
                m["reviews_count"] = s.get("reviews_count")
                m["city"] = s.get("city")
        return matches

    async def get_competitor_pricing_comparisons(
        self, report_id: int,
    ) -> list[dict[str, Any]]:
        """Load competitor_pricing_comparisons rows for a report."""
        try:
            res = (
                self.client.table("competitor_pricing_comparisons")
                .select("*")
                .eq("report_id", report_id)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load competitor_pricing_comparisons for report=%s: %s",
                report_id, e,
            )
            return []
        return list(res.data or [])

    async def get_competitor_service_gaps(
        self, report_id: int,
    ) -> list[dict[str, Any]]:
        """Load competitor_service_gaps rows for a report, ordered by sort_order."""
        try:
            res = (
                self.client.table("competitor_service_gaps")
                .select("*")
                .eq("report_id", report_id)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load competitor_service_gaps for report=%s: %s",
                report_id, e,
            )
            return []
        return list(res.data or [])

    async def get_competitor_dimensional_scores(
        self, report_id: int,
    ) -> list[dict[str, Any]]:
        """Load competitor_dimensional_scores rows for a report, ordered."""
        try:
            res = (
                self.client.table("competitor_dimensional_scores")
                .select("*")
                .eq("report_id", report_id)
                .order("sort_order", desc=False)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to load competitor_dimensional_scores for report=%s: %s",
                report_id, e,
            )
            return []
        return list(res.data or [])

    async def get_subject_salon_context(self, salon_id: int) -> dict[str, Any]:
        """Load identity fields for the subject salon (for prompt context).

        Joins salons to business_categories to resolve the primary category name.
        Returns a dict with salon_name, salon_city, primary_category_name,
        reviews_count, reviews_rank, total_services.
        """
        context: dict[str, Any] = {
            "salon_id": salon_id,
            "salon_name": None,
            "salon_city": None,
            "primary_category_name": None,
            "reviews_count": 0,
            "reviews_rank": 0.0,
            "total_services": 0,
        }
        try:
            salon_res = (
                self.client.table("salons")
                .select("id,name,city,reviews_count,reviews_rank,primary_category_id")
                .eq("id", salon_id)
                .limit(1)
                .execute()
            )
        except Exception as e:
            logger.warning("Failed to load subject salon %s: %s", salon_id, e)
            return context
        if not salon_res.data:
            return context
        s = salon_res.data[0]
        context["salon_name"] = s.get("name")
        context["salon_city"] = s.get("city")
        context["reviews_count"] = s.get("reviews_count") or 0
        context["reviews_rank"] = float(s.get("reviews_rank") or 0.0)

        primary_cat_id = s.get("primary_category_id")
        if primary_cat_id:
            try:
                cat_res = (
                    self.client.table("business_categories")
                    .select("id,name")
                    .eq("id", primary_cat_id)
                    .limit(1)
                    .execute()
                )
                if cat_res.data:
                    context["primary_category_name"] = cat_res.data[0].get("name")
            except Exception as e:
                logger.warning("Failed to load primary category %s: %s", primary_cat_id, e)

        # Total services from the latest scrape
        try:
            latest_scrape_res = (
                self.client.table("salon_scrapes")
                .select("id")
                .eq("booksy_id", s.get("id"))
                .order("scraped_at", desc=True)
                .limit(1)
                .execute()
            )
            # salon_scrapes is keyed by booksy_id, but salons.id != booksy_id —
            # refetch using the salons.booksy_id column if needed.
        except Exception:
            latest_scrape_res = None

        try:
            booksy_res = (
                self.client.table("salons")
                .select("booksy_id")
                .eq("id", salon_id)
                .limit(1)
                .execute()
            )
            if booksy_res.data:
                booksy_id = booksy_res.data[0].get("booksy_id")
                if booksy_id is not None:
                    scrape_res = (
                        self.client.table("salon_scrapes")
                        .select("id")
                        .eq("booksy_id", booksy_id)
                        .order("scraped_at", desc=True)
                        .limit(1)
                        .execute()
                    )
                    if scrape_res.data:
                        scrape_id = scrape_res.data[0]["id"]
                        count_res = (
                            self.client.table("salon_scrape_services")
                            .select("id", count="exact")
                            .eq("scrape_id", scrape_id)
                            .eq("is_active", True)
                            .execute()
                        )
                        # supabase-py returns count in response.count
                        count_attr = getattr(count_res, "count", None)
                        if isinstance(count_attr, int):
                            context["total_services"] = count_attr
                        elif count_res.data:
                            context["total_services"] = len(count_res.data)
        except Exception as e:
            logger.debug("Failed to resolve total_services for salon %s: %s", salon_id, e)
        return context

    async def update_competitor_report_data(
        self, report_id: int, data: dict[str, Any],
    ) -> None:
        """Merge `data` into competitor_reports.report_data (jsonb column).

        Used by Comp Etap 5 to write positioning_narrative + swot without
        wiping the rest of the report_data shape (if any previous synthesis
        wrote extra fields).
        """
        try:
            existing = (
                self.client.table("competitor_reports")
                .select("report_data")
                .eq("id", report_id)
                .limit(1)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Failed to read report_data for competitor_reports id=%s: %s",
                report_id, e,
            )
            existing = None

        current: dict[str, Any] = {}
        if existing and existing.data:
            raw = existing.data[0].get("report_data")
            if isinstance(raw, dict):
                current = raw

        merged = {**current, **data}

        self.client.table("competitor_reports").update(
            {"report_data": merged},
        ).eq("id", report_id).execute()

    async def delete_competitor_recommendations(self, report_id: int) -> None:
        """Wipe previous recommendations for a report (idempotent re-synthesis)."""
        try:
            self.client.table("competitor_recommendations").delete().eq(
                "report_id", report_id,
            ).execute()
        except Exception as e:
            logger.warning(
                "Failed to delete competitor_recommendations for report=%s: %s",
                report_id, e,
            )

    async def insert_competitor_recommendations(
        self, rows: list[dict[str, Any]],
    ) -> int:
        """Batch-insert competitor_recommendations rows. Returns inserted count."""
        if not rows:
            return 0
        try:
            result = (
                self.client.table("competitor_recommendations")
                .insert(rows)
                .execute()
            )
        except Exception as e:
            logger.error(
                "Failed to insert %d competitor_recommendations: %s", len(rows), e,
            )
            raise
        return len(result.data or [])
