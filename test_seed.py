import asyncio
import os
import sys

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.app.services.ingestion import ingestion_service
from backend.app.db.session import AsyncSessionLocal
from backend.app.models.exchange_key import ExchangeKey
from backend.app.models.app_setting import AppSetting
from sqlalchemy import select

async def test():
    async with AsyncSessionLocal() as db:
        keys_result = await db.execute(select(ExchangeKey))
        keys = keys_result.scalars().all()
        
        settings_result = await db.execute(select(AppSetting))
        settings = settings_result.scalars().all()
        
        print(f"Keys before: {[k.exchange_name for k in keys]}")
        print(f"Settings: {[s.key for s in settings]}")
        
    print("\nCalling sync_env_keys...")
    await ingestion_service.sync_env_keys()
    
    async with AsyncSessionLocal() as db:
        keys_result = await db.execute(select(ExchangeKey))
        keys = keys_result.scalars().all()
        print(f"Keys after: {[k.exchange_name for k in keys]}")

if __name__ == '__main__':
    asyncio.run(test())
