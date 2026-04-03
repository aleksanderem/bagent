"""Supabase client for reading scraped data and writing reports."""

import logging

from supabase import Client, ClientOptions, create_client

from config import settings

logger = logging.getLogger(__name__)


class SupabaseService:
    def __init__(self) -> None:
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
            options=ClientOptions(headers={"ngrok-skip-browser-warning": "true"}),
        )

    async def get_scraped_data(self, convex_audit_id: str) -> dict:
        """Read scraped data from audit_scraped_data table by convex audit ID."""
        result = self.client.table("audit_scraped_data").select("*").eq("convex_audit_id", convex_audit_id).execute()
        if not result.data:
            raise ValueError(f"No scraped data found for audit {convex_audit_id}")
        row = result.data[0]
        # Reconstruct ScrapedData shape from normalized columns
        return {
            "salonName": row.get("salon_name"),
            "salonAddress": row.get("salon_address"),
            "salonLogoUrl": row.get("salon_logo_url"),
            "categories": row.get("categories_json", []),
            "totalServices": row.get("total_services", 0),
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
            "total_score": report.get("totalScore", 0),
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
        # Issues
        issues = report.get("topIssues", [])
        if issues:
            issue_rows = [
                {
                    "audit_report_id": report_id,
                    "severity": iss.get("severity", "minor"),
                    "dimension": iss.get("dimension", "naming"),
                    "issue": iss.get("issue", ""),
                    "impact": iss.get("impact", ""),
                    "affected_count": iss.get("affectedCount", 0),
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

        # Transformations
        transformations = report.get("transformations", [])
        if transformations:
            trans_rows = [
                {
                    "audit_report_id": report_id,
                    "type": t.get("type", "name"),
                    "service_name": t.get("serviceName", ""),
                    "before_text": t.get("before", ""),
                    "after_text": t.get("after", ""),
                    "reason": t.get("reason", ""),
                    "impact_score": max(1, min(10, t.get("impactScore", 5))),
                    "sort_order": i,
                }
                for i, t in enumerate(transformations)
            ]
            res = self.client.table("audit_transformations").insert(trans_rows).execute()
            if not res.data:
                logger.warning("Failed to insert audit_transformations")
            else:
                logger.info("Inserted %d audit_transformations", len(trans_rows))

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
                    "affected_services": qw.get("affectedServices", 0),
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

    async def get_benchmarks(self, city: str | None = None) -> dict:
        """Get industry comparison data."""
        try:
            query = self.client.table("audit_benchmarks").select("*")
            if city:
                query = query.eq("city", city)
            result = query.limit(1).execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            logger.warning("Failed to fetch benchmarks: %s", e)
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

    async def get_salon_with_services(self, salon_id: int) -> dict | None:
        """Load salon with its services and categories."""
        try:
            salon_result = self.client.table("salons").select("*").eq("id", salon_id).limit(1).execute()
            if not salon_result.data:
                logger.warning("Salon %d not found", salon_id)
                return None
            salon = salon_result.data[0]

            services_result = self.client.table("services").select("*").eq("salon_id", salon_id).execute()
            services = services_result.data or []

            categories: list[str] = sorted({s.get("category", "") for s in services if s.get("category")})

            return {"salon": salon, "services": services, "categories": categories}
        except Exception as e:
            logger.warning("Failed to load salon %d with services: %s", salon_id, e)
            return None

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
        """Load salon metadata + all services for a given Booksy salon ID.
        Returns: {salon: {...}, services: [...]}"""
        try:
            salon_result = self.client.table("salons").select("*").eq("booksy_id", salon_id).limit(1).execute()
            if not salon_result.data:
                return {"salon": None, "services": []}
            salon = salon_result.data[0]

            services_result = self.client.table("services").select("*").eq("salon_id", salon["id"]).execute()
            return {"salon": salon, "services": services_result.data or []}
        except Exception as e:
            logger.warning("Failed to load salon %d: %s", salon_id, e)
            return {"salon": None, "services": []}

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
        """Save optimized pricelist to optimized_pricelists table.

        Maps optimization result to the actual table schema:
        convex_audit_id, salon_name, quality_score, total_changes,
        names_improved, descriptions_added, categories_restructured,
        original_service_count, optimized_service_count, duplicates_merged,
        quality_checks, pipeline_version, processing_time_ms.
        """
        summary = optimization_data.get("summary", {})
        row = {
            "convex_audit_id": convex_audit_id,
            "salon_name": salon_name or None,
            "quality_score": optimization_data.get("qualityScore", 0),
            "total_changes": summary.get("totalChanges", 0),
            "names_improved": summary.get("namesImproved", 0),
            "descriptions_added": summary.get("descriptionsAdded", 0),
            "categories_restructured": summary.get("categoriesOptimized", 0),
            "duplicates_merged": summary.get("duplicatesFound", 0),
            "pipeline_version": "v2-bagent",
        }
        result = self.client.table("optimized_pricelists").upsert(row, on_conflict="convex_audit_id").execute()
        if not result.data:
            raise ValueError("Failed to save optimized pricelist")
        return result.data[0]["id"]
