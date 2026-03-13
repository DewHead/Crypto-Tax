import asyncio
import os
import sys

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.bloxtax_ingestion import bloxtax_ingestion_service

async def main():
    files = [
        '../../Downloads/bloxtax 1.xlsx',
        '../../Downloads/bloxtax 2.xlsx'
    ]
    
    for f in files:
        abs_path = os.path.abspath(f)
        print(f"Ingesting {abs_path}...")
        await bloxtax_ingestion_service.process_excel(abs_path)
    
    print("Ingestion complete.")

if __name__ == "__main__":
    asyncio.run(main())
