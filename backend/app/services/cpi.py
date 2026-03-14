import httpx
import csv
import io
from datetime import date, datetime, timedelta
from typing import Dict, Optional, List
import asyncio
import logging
from sqlalchemy import select, insert, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.models.cpi_rate import CPIRate

logger = logging.getLogger(__name__)

class CPIService:
    def __init__(self):
        self.cache: Dict[date, float] = {}
        self.prefetched_ranges: List[tuple] = []
        self.base_url = "https://edge.boi.org.il/FusionEdgeServer/sdmx/v2/data/dataflow/BOI.STATISTICS/PRI/1.0/CP"
        self._lock = asyncio.Lock()

    async def _load_from_db(self, start_date: date, end_date: date, db: Optional[AsyncSession] = None):
        if db:
            stmt = select(CPIRate).where(
                and_(CPIRate.date >= start_date, CPIRate.date <= end_date)
            )
            result = await db.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                self.cache[r.date] = r.index_value
        else:
            async with AsyncSessionLocal() as db_session:
                stmt = select(CPIRate).where(
                    and_(CPIRate.date >= start_date, CPIRate.date <= end_date)
                )
                result = await db_session.execute(stmt)
                rows = result.scalars().all()
                for r in rows:
                    self.cache[r.date] = r.index_value

    async def _save_to_db(self, rates: Dict[date, float], db: Optional[AsyncSession] = None):
        if not rates:
            return
        
        if db:
            await self._save_to_db_internal(rates, db)
        else:
            async with AsyncSessionLocal() as db_session:
                await self._save_to_db_internal(rates, db_session)

    async def _save_to_db_internal(self, rates: Dict[date, float], db: AsyncSession):
        try:
            existing_dates_stmt = select(CPIRate.date).where(CPIRate.date.in_(list(rates.keys())))
            existing_dates_result = await db.execute(existing_dates_stmt)
            existing_dates = set(existing_dates_result.scalars().all())
            
            new_rates = [
                {"date": d, "index_value": r}
                for d, r in rates.items()
                if d not in existing_dates
            ]
            
            if new_rates:
                await db.execute(insert(CPIRate).values(new_rates))
                await db.commit()
        except Exception as e:
            logger.error(f"Error saving CPI rates to DB: {e}")
            await db.rollback()

    async def prefetch_rates(self, start_date: date, end_date: date, db: Optional[AsyncSession] = None):
        if end_date > date.today():
             end_date = date.today()
        
        # Shift start_date back slightly to ensure we have the full months
        start_date = start_date.replace(day=1)

        async with self._lock:
            for p_start, p_end in self.prefetched_ranges:
                if p_start <= start_date and p_end >= end_date:
                    return

            await self._load_from_db(start_date, end_date, db=db)
            
            # CPI is monthly. Check if we have data for the requested months.
            # (Crude check: if range is > 31 days and we have 0 cache points, we need to fetch)
            if not self.cache or max(self.cache.keys()) < end_date - timedelta(days=32):
                try:
                    logger.info(f"Fetching BOI CPI rates from {start_date} to {end_date}...")
                    params = {
                        "startperiod": start_date.strftime('%Y-%m'),
                        "endperiod": end_date.strftime('%Y-%m'),
                        "format": "csv"
                    }
                    
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(self.base_url, params=params)
                        if response.status_code == 200:
                            new_rates = self._parse_csv_to_cache(response.text)
                            await self._save_to_db(new_rates, db=db)
                            self.prefetched_ranges.append((start_date, end_date))
                        else:
                            logger.error(f"BOI API returned status {response.status_code} for CPI")
                except Exception as e:
                    logger.error(f"Error fetching CPI range from BOI API: {e}")

    async def get_cpi_index(self, target_date: date, db: Optional[AsyncSession] = None) -> float:
        """
        Returns the CPI index value for a given date.
        Uses the index value of the month containing the date.
        If not yet published, uses the most recent available.
        """
        if target_date > date.today():
            target_date = date.today()

        # Check cache
        month_start = target_date.replace(day=1)
        if month_start in self.cache:
            return self.cache[month_start]

        # Check DB
        if db:
            stmt = select(CPIRate).where(CPIRate.date <= target_date).order_by(desc(CPIRate.date)).limit(1)
            result = await db.execute(stmt)
            row = result.scalars().first()
            if row and row.date.year == target_date.year and row.date.month == target_date.month:
                self.cache[row.date] = row.index_value
                return row.index_value
        
        # Fetch if needed
        start_date = target_date - timedelta(days=365) # Fetch a year's worth
        await self.prefetch_rates(start_date, target_date, db=db)

        # Look for the exact month first
        if month_start in self.cache:
            return self.cache[month_start]
        
        # Fallback to the most recent one before target_date
        past_dates = [d for d in self.cache.keys() if d <= target_date]
        if past_dates:
            most_recent = max(past_dates)
            return self.cache[most_recent]

        # Ultimate fallback (Base 100 or something reasonable)
        return 100.0

    def _parse_csv_to_cache(self, csv_text: str) -> Dict[date, float]:
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)
        parsed_rates = {}
        for row in reader:
            if row.get('SERIES_CODE') == 'CP':
                try:
                    # TIME_PERIOD is usually 'YYYY-MM'
                    time_period = row['TIME_PERIOD']
                    if len(time_period) == 7: # YYYY-MM
                        obs_date = datetime.strptime(time_period, '%Y-%m').date()
                    else:
                        obs_date = datetime.strptime(time_period, '%Y-%m-%d').date().replace(day=1)
                    
                    obs_value = float(row['OBS_VALUE'])
                    self.cache[obs_date] = obs_value
                    parsed_rates[obs_date] = obs_value
                except (ValueError, KeyError):
                    continue
        return parsed_rates

cpi_service = CPIService()
