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
        
        # Always use a dedicated session for background rate updates
        # to avoid committing or rolling back the caller's session.
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
        Returns the "Known CPI" (Madad Yadu'a) index value for a given date.
        According to Israeli tax law, the CPI is published on the 15th for the previous month.
        If target_date < 15, known CPI is from 2 months ago.
        If target_date >= 15, known CPI is from 1 month ago.
        """
        if target_date > date.today():
            target_date = date.today()

        known_cpi_month_start = self.get_known_cpi_date(target_date)

        # Check cache
        if known_cpi_month_start in self.cache:
            return self.cache[known_cpi_month_start]

        # Check DB
        if db:
            stmt = select(CPIRate).where(CPIRate.date == known_cpi_month_start).limit(1)
            result = await db.execute(stmt)
            row = result.scalars().first()
            if row:
                self.cache[row.date] = row.index_value
                return row.index_value
        
        # Fetch if needed
        # Ensure we have a reasonable range
        start_date = known_cpi_month_start - timedelta(days=365)
        await self.prefetch_rates(start_date, known_cpi_month_start, db=db)

        # Look for the exact month first
        if known_cpi_month_start in self.cache:
            return self.cache[known_cpi_month_start]
        
        # Fallback to the most recent one before or on known_cpi_month_start
        past_dates = [d for d in self.cache.keys() if d <= known_cpi_month_start]
        if past_dates:
            most_recent = max(past_dates)
            return self.cache[most_recent]

        # Ultimate fallback (Base 100 or something reasonable)
        return 100.0

    def get_known_cpi_date(self, transaction_date: date) -> date:
        """
        Determines the month for which the CPI is 'known' on a given transaction date.
        Israeli law: CPI is published on the 15th of month M for month M-1.
        """
        # If the transaction is before the 15th, the known CPI is from 2 months ago
        if transaction_date.day < 15:
            # e.g., May 10 -> Month 3 (March)
            month_offset = 2
        else:
            # If the transaction is on or after the 15th, the known CPI is from 1 month ago
            # e.g., May 16 -> Month 4 (April)
            month_offset = 1
            
        known_month = transaction_date.month - month_offset
        known_year = transaction_date.year
        if known_month <= 0:
            known_month += 12
            known_year -= 1
            
        return date(known_year, known_month, 1)

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
