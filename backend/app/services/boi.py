import httpx
import csv
import io
from datetime import date, datetime, timedelta
from typing import Dict, Optional, List
import asyncio
import logging
from sqlalchemy import select, insert, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.models.ils_rate import ILSRate

logger = logging.getLogger(__name__)

class BOIService:
    def __init__(self):
        self.cache: Dict[date, float] = {}
        self.missing_dates: set = set() # Cache for dates that definitely have no rates (weekends/holidays)
        self.prefetched_ranges: List[tuple] = [] # Cache for ranges already prefetched from API
        self.base_url = "https://edge.boi.org.il/FusionEdgeServer/sdmx/v2/data/dataflow/BOI.STATISTICS/EXR/1.0"
        self._lock = asyncio.Lock()

    async def _load_from_db(self, start_date: date, end_date: date, db: Optional[AsyncSession] = None):
        """Loads rates from the database into the memory cache for a given range."""
        if db:
            stmt = select(ILSRate).where(
                and_(ILSRate.date >= start_date, ILSRate.date <= end_date)
            )
            result = await db.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                self.cache[r.date] = r.rate
        else:
            async with AsyncSessionLocal() as db_session:
                stmt = select(ILSRate).where(
                    and_(ILSRate.date >= start_date, ILSRate.date <= end_date)
                )
                result = await db_session.execute(stmt)
                rows = result.scalars().all()
                for r in rows:
                    self.cache[r.date] = r.rate

    async def _save_to_db(self, rates: Dict[date, float], db: Optional[AsyncSession] = None):
        """Saves a batch of rates to the database, ignoring duplicates."""
        if not rates:
            return
        
        # Always use a dedicated session for background rate updates
        # to avoid committing or rolling back the caller's session.
        async with AsyncSessionLocal() as db_session:
            await self._save_to_db_internal(rates, db_session)

    async def _save_to_db_internal(self, rates: Dict[date, float], db: AsyncSession):
        try:
            # Filter out rates that already exist in DB to avoid primary key conflicts
            existing_dates_stmt = select(ILSRate.date).where(ILSRate.date.in_(list(rates.keys())))
            existing_dates_result = await db.execute(existing_dates_stmt)
            existing_dates = set(existing_dates_result.scalars().all())
            
            new_rates = [
                {"date": d, "rate": r}
                for d, r in rates.items()
                if d not in existing_dates
            ]
            
            if new_rates:
                await db.execute(insert(ILSRate).values(new_rates))
                await db.commit()
        except Exception as e:
            logger.error(f"Error saving rates to DB: {e}")
            await db.rollback()

    async def prefetch_rates(self, start_date: date, end_date: date, db: Optional[AsyncSession] = None):
        """
        Fetches all rates for a given range. Checks DB first, then fetches missing from API.
        """
        if end_date > date.today():
             end_date = date.today()
        
        async with self._lock:
            # Check if this range (or a larger one) was already prefetched in this session
            for p_start, p_end in self.prefetched_ranges:
                if p_start <= start_date and p_end >= end_date:
                    return

            # 1. Load what we have from DB into memory cache
            await self._load_from_db(start_date, end_date, db=db)
            
            # 2. Check for missing days (approximate, since weekends have no rates)
            # We check if we have "enough" coverage for the range.
            expected_days = (end_date - start_date).days
            covered_days = sum(1 for d in self.cache if start_date <= d <= end_date)
            
            # BOI only publishes on business days (~5/7). 
            # If we have less than 50% coverage, we probably need a fetch.
            if covered_days < (expected_days * 0.5) and expected_days > 2:
                try:
                    print(f"Fetching BOI rates from {start_date} to {end_date}...")
                    params = {
                        "startperiod": start_date.isoformat(),
                        "endperiod": end_date.isoformat(),
                        "format": "csv"
                    }
                    
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(self.base_url, params=params)
                        if response.status_code == 200:
                            new_rates = self._parse_csv_to_cache(response.text)
                            # Save the new rates to DB
                            await self._save_to_db(new_rates, db=db)
                            self.prefetched_ranges.append((start_date, end_date))
                        else:
                            logger.error(f"BOI API returned status {response.status_code}")
                except Exception as e:
                    logger.error(f"Error fetching range from BOI API: {e}")

    async def get_usd_ils_rate(self, target_date: date, db: Optional[AsyncSession] = None) -> Optional[float]:
        # Don't try to fetch future rates
        if target_date > date.today():
            return 3.65

        # Check memory cache
        if target_date in self.cache:
            return self.cache[target_date]
        
        if target_date in self.missing_dates:
            return 3.65

        # Check database directly for this single date
        if db:
            stmt = select(ILSRate).where(ILSRate.date == target_date)
            result = await db.execute(stmt)
            row = result.scalars().first()
            if row:
                self.cache[target_date] = row.rate
                return row.rate
        else:
            async with AsyncSessionLocal() as db_session:
                stmt = select(ILSRate).where(ILSRate.date == target_date)
                result = await db_session.execute(stmt)
                row = result.scalars().first()
                if row:
                    self.cache[target_date] = row.rate
                    return row.rate

        # If not in cache or DB, fetch a window
        start_date = target_date - timedelta(days=180)
        end_date = min(target_date + timedelta(days=180), date.today())
        
        await self.prefetch_rates(start_date, end_date, db=db)

        # After fetching/parsing/loading, check if we have the specific date
        if target_date in self.cache:
            return self.cache[target_date]
        
        # If not found (weekend/holiday), mark as missing and look for the most recent previous rate
        self.missing_dates.add(target_date)

        for i in range(1, 15):
            prev_date = target_date - timedelta(days=i)
            # Recursively calls get_usd_ils_rate but cache/missing_dates should catch it
            rate = await self.get_usd_ils_rate(prev_date, db=db)
            if rate and rate != 3.65: # 3.65 is the ultimate fallback
                # Cache the found rate for the missing date too, to avoid re-recursion
                self.cache[target_date] = rate
                return rate
        
        return 3.65 # Ultimate fallback

    def _parse_csv_to_cache(self, csv_text: str) -> Dict[date, float]:
        """
        Parses BOI SDMX CSV format and updates internal cache.
        Returns the parsed rates as a dictionary.
        """
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)
        parsed_rates = {}
        for row in reader:
            if row.get('SERIES_CODE') == 'RER_USD_ILS':
                try:
                    obs_date = datetime.strptime(row['TIME_PERIOD'], '%Y-%m-%d').date()
                    obs_value = float(row['OBS_VALUE'])
                    self.cache[obs_date] = obs_value
                    parsed_rates[obs_date] = obs_value
                except (ValueError, KeyError):
                    continue
        return parsed_rates

boi_service = BOIService()
