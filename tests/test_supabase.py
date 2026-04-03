"""Integration tests for SupabaseService.

All tests require real Supabase credentials and are marked with
@pytest.mark.integration so they can be excluded with:
    pytest -m "not integration"
"""

import pytest

from services.supabase import SupabaseService


@pytest.mark.integration
class TestSupabaseService:
    def setup_method(self) -> None:
        self.service = SupabaseService()

    async def test_get_scraped_data_missing(self) -> None:
        """Reading non-existent audit should raise ValueError."""
        with pytest.raises(ValueError, match="No scraped data found"):
            await self.service.get_scraped_data("nonexistent_id_12345")

    async def test_get_benchmarks_default(self) -> None:
        """Get benchmarks without city filter -- should return fallback at minimum."""
        result = await self.service.get_benchmarks()
        assert "industry_average" in result or "industryAverage" in result

    async def test_get_benchmarks_with_city(self) -> None:
        """Get benchmarks with city filter -- should return dict."""
        result = await self.service.get_benchmarks(city="Warszawa")
        assert isinstance(result, dict)
        assert "industry_average" in result or "industryAverage" in result

    async def test_geocode_salon_returns_none_for_unknown(self) -> None:
        """Geocoding unknown salon should return None gracefully."""
        result = await self.service.geocode_salon("NONEXISTENT_SALON_XYZ_12345", None)
        assert result is None

    async def test_geocode_salon_returns_none_for_empty_inputs(self) -> None:
        """Geocoding with no name and no address should return None."""
        result = await self.service.geocode_salon(None, None)
        assert result is None

    async def test_get_competitors_empty_area(self) -> None:
        """Getting competitors in empty area should return empty list."""
        result = await self.service.get_competitors(0.0, 0.0, 5, [])
        assert isinstance(result, list)
