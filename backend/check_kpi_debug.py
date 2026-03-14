
import asyncio
from app.db.session import AsyncSessionLocal
from app.services.tax_engine import TaxEngine

async def check_kpi():
    async with AsyncSessionLocal() as session:
        kpi = await TaxEngine().get_kpi(session, 2025)
        print(kpi)

if __name__ == \"__main__\":
    asyncio.run(check_kpi())
