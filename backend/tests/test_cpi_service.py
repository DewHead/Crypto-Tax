import pytest
import httpx
from datetime import date, timedelta
from app.services.cpi import CPIService

from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_cpi_fetch_no_404():
    """
    Verifies that the CPIService fetches data using the correct URL endpoint
    and doesn't encounter a 404 error from the BOI API.
    """
    cpi_service = CPIService()
    
    # Mock DB methods to avoid needing a real database
    cpi_service._load_from_db = AsyncMock()
    cpi_service._save_to_db = AsyncMock()
    
    # We use dates that are guaranteed to be in the past to fetch valid data.
    # We'll fetch data for 3 months ago to ensure it's published.
    end_date = date.today().replace(day=1) - timedelta(days=1)
    start_date = end_date - timedelta(days=60)
    
    # Run the prefetch_rates which previously produced the 404 logs.
    await cpi_service.prefetch_rates(start_date, end_date)
    
    # The cache should be populated and no exceptions or 404s should occur.
    # We verify that at least one date was parsed successfully into the cache.
    assert len(cpi_service.cache) > 0, "CPI cache should have been populated."
