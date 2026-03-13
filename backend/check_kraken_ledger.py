import asyncio
import os
import sys

# Add the backend directory to sys.path to import app modules
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.models.exchange_key import ExchangeKey
from app.db.session import AsyncSessionLocal
from sqlalchemy import select
import ccxt.async_support as ccxt

async def check_kraken_ledger():
    async with AsyncSessionLocal() as db:
        stmt = select(ExchangeKey).filter(ExchangeKey.exchange_name == 'kraken')
        result = await db.execute(stmt)
        key = result.scalars().first()
        
        if not key:
            print("No Kraken key found in DB")
            return

        exchange = ccxt.kraken({
            'apiKey': key.api_key,
            'secret': key.api_secret,
            'enableRateLimit': True,
        })
        
        try:
            print("Fetching Kraken Ledger...")
            ledger = await exchange.fetch_ledger()
            print(f"Found {len(ledger)} entries.")
            if ledger:
                for entry in ledger[:5]: # Show first 5
                    print(entry)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_kraken_ledger())
