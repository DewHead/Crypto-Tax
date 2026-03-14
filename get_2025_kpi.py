import asyncio
import sys
import os

# Ensure we are in the backend directory context for relative DB paths
os.chdir('backend')
sys.path.append(os.getcwd())

from app.services.tax_engine import TaxEngine
from app.db.session import AsyncSessionLocal

async def main():
    async with AsyncSessionLocal() as db:
        kpi = await TaxEngine().get_kpi(db, 2025)
        print(\"2025 Tax Report Summary:\")
        for key, value in kpi.items():
            print(f\"  {key}: {value}\")

if __name__ == \"__main__\":
    asyncio.run(main())
