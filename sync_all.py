import asyncio
import sys
import os

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.ingestion import ingestion_service

async def main():
    print("Starting full sync of all exchanges...")
    await ingestion_service.sync_all()
    print("Sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
