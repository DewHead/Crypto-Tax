import asyncio
import os
import sys

# Add the backend directory to sys.path to import app modules
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.ingestion import IngestionService
from app.db.session import AsyncSessionLocal
from app.models.transaction import Transaction
from sqlalchemy import select, func

async def test_kraken_sync():
    service = IngestionService()
    
    print("Starting sync for Kraken and Kraken Futures...")
    # This will sync all keys found in the database, including Kraken ones
    await service.sync_all()
    
    print("\nSync complete. Checking results in database...")
    
    async with AsyncSessionLocal() as db:
        # Check Kraken Spot
        stmt_kraken = select(func.count()).select_from(Transaction).where(Transaction.exchange == 'kraken')
        result_kraken = await db.execute(stmt_kraken)
        count_kraken = result_kraken.scalar()
        print(f"Total transactions for 'kraken': {count_kraken}")
        
        # Check Kraken Futures
        stmt_futures = select(func.count()).select_from(Transaction).where(Transaction.exchange == 'krakenfutures')
        result_futures = await db.execute(stmt_futures)
        count_futures = result_futures.scalar()
        print(f"Total transactions for 'krakenfutures': {count_futures}")
        
        # Show a few recent transactions if any
        if count_kraken > 0 or count_futures > 0:
            print("\nRecent Transactions:")
            stmt_recent = select(Transaction).order_by(Transaction.timestamp.desc()).limit(10)
            result_recent = await db.execute(stmt_recent)
            for tx in result_recent.scalars().all():
                print(f"[{tx.timestamp}] {tx.exchange} | {tx.type} | {tx.amount_from} {tx.asset_from} -> {tx.amount_to} {tx.asset_to} (Hash: {tx.tx_hash})")

if __name__ == "__main__":
    asyncio.run(test_kraken_sync())
