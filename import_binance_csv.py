import asyncio
import sys
import os

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.csv_ingestion import csv_ingestion_service

async def main():
    files = [
        "/home/tal/Downloads/202603101011_ec9416d6.zip",
        "/home/tal/Downloads/202603101031_c86b7e7c.zip"
    ]
    
    for f in files:
        print(f"Processing {f}...")
        await csv_ingestion_service.process_zip(f)
    
    print("Ingestion complete.")

if __name__ == "__main__":
    asyncio.run(main())
