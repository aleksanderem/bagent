"""Supabase client for reading scraped data and writing reports."""

import logging

from supabase import Client, create_client

from config import settings

logger = logging.getLogger(__name__)


class SupabaseService:
    def __init__(self) -> None:
        self.client: Client = create_client(settings.supabase_url, settings.supabase_service_key)

    async def get_scraped_data(self, convex_audit_id: str) -> dict:
        """Read scraped data from audit_scraped_data table by convex audit ID.

        Returns the raw dict (caller converts to ScrapedData model).
        """
        result = self.client.table("audit_scraped_data").select("*").eq("convex_audit_id", convex_audit_id).execute()
        if not result.data:
            raise ValueError(f"No scraped data found for audit {convex_audit_id}")
        return result.data[0].get("scraped_data", {})

    async def save_report(
        self,
        convex_audit_id: str,
        convex_user_id: str,
        report: dict,
        salon_name: str,
        salon_address: str,
        source_url: str,
    ) -> str:
        """Save audit report to audit_reports table. Returns the report row ID."""
        row = {
            "convex_audit_id": convex_audit_id,
            "convex_user_id": convex_user_id,
            "report_data": report,
            "salon_name": salon_name,
            "salon_address": salon_address,
            "source_url": source_url,
            "version": "v2",
        }
        result = self.client.table("audit_reports").insert(row).execute()
        if not result.data:
            raise ValueError("Failed to save report to Supabase")
        return result.data[0]["id"]

    async def get_benchmarks(self, city: str | None = None) -> dict:
        """Get industry comparison data (average scores from benchmarks table).

        Returns dict with industry_average, top_performers, sample_size.
        """
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
        """Get competitor salons from the salons table within radius.

        Uses PostGIS-style distance calculation via RPC.
        """
        try:
            result = self.client.rpc(
                "get_nearby_salons",
                {
                    "p_lat": lat,
                    "p_lng": lng,
                    "p_radius_km": radius_km,
                    "p_limit": 20,
                },
            ).execute()
            return result.data or []
        except Exception as e:
            logger.warning("Failed to fetch competitors: %s", e)
            return []

    async def geocode_salon(self, salon_name: str | None, address: str | None) -> dict | None:
        """Find salon coordinates by name/address match in salons table.

        Returns {"lat": ..., "lng": ...} or None.
        """
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
