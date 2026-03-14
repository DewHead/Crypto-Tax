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

@pytest.mark.asyncio
async def test_madad_yadu_a_rule():
    """
    Verifies the "Known CPI" (Madad Yadu'a) rule which depends on whether
    the transaction date is before or after the 15th of the month.
    """
    cpi_service = CPIService()
    # Mock cache with some values
    # Jan 2025: 101.0
    # Feb 2025: 102.0
    # Mar 2025: 103.0
    # Apr 2025: 104.0
    # May 2025: 105.0
    cpi_service.cache = {
        date(2024, 11, 1): 99.0,
        date(2024, 12, 1): 100.0,
        date(2025, 1, 1): 101.0,
        date(2025, 2, 1): 102.0,
        date(2025, 3, 1): 103.0,
        date(2025, 4, 1): 104.0,
        date(2025, 5, 1): 105.0,
    }
    
    # May 10, 2025 -> Before 15th -> Known CPI is from 2 months ago (March)
    # Expected: 103.0
    val = await cpi_service.get_cpi_index(date(2025, 5, 10))
    assert val == 103.0, f"Expected 103.0 for May 10, but got {val}"
    
    # May 16, 2025 -> After 15th -> Known CPI is from 1 month ago (April)
    # Expected: 104.0
    val = await cpi_service.get_cpi_index(date(2025, 5, 16))
    assert val == 104.0, f"Expected 104.0 for May 16, but got {val}"

    # Jan 10, 2025 -> Before 15th -> Known CPI is from 2 months ago (Nov 2024)
    val = await cpi_service.get_cpi_index(date(2025, 1, 10))
    assert val == 99.0, f"Expected 99.0 for Jan 10, but got {val}"

    # Jan 20, 2025 -> After 15th -> Known CPI is from 1 month ago (Dec 2024)
    val = await cpi_service.get_cpi_index(date(2025, 1, 20))
    assert val == 100.0, f"Expected 100.0 for Jan 20, but got {val}"
