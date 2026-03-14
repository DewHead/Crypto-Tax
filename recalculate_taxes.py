import asyncio
import sys
import os

# Ensure we are in the backend directory context for relative DB paths
os.chdir('backend')
sys.path.append(os.getcwd())

from app.services.tax_engine import TaxEngine
from app.db.session import AsyncSessionLocal
from app.models.transaction import Transaction
from sqlalchemy import select

async def main():
    print(\"Recalculating all taxes with real historical BOI rates...\")
    async with AsyncSessionLocal() as db:
        await TaxEngine().calculate_taxes(db)
        
        # Verify some transactions
        stmt = select(Transaction).filter(Transaction.ils_exchange_rate.is_not(None)).limit(3)
        result = await db.execute(stmt)
        txs = result.scalars().all()
        for tx in txs:
            print(f\"Verified Transaction {tx.tx_hash}:\")
            print(f\"  Date: {tx.timestamp}\")
            print(f\"  ILS Rate: {tx.ils_exchange_rate}\")
            print(f\"  Gain ILS: {tx.capital_gain_ils}\")

    print(\"Recalculation complete.\")

if __name__ == \"__main__\":
    asyncio.run(main())
