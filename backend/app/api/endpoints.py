from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.session import get_db, AsyncSessionLocal
from app.schemas.transaction import Transaction, KPIReport

async def run_sync_and_calculate_background():
    """
    Run ingestion and tax calculation in the background.
    """
    try:
        print("Starting background full sync...")
        await ingestion_service.sync_all()
        async with AsyncSessionLocal() as db:
            await tax_engine.calculate_taxes(db)
        print("Background full sync completed successfully.")
    except Exception as e:
        print(f"Error in background sync: {e}")

async def run_single_key_sync_background(key_id: int):
    """
    Run single key sync and tax calculation in the background.
    """
    try:
        print(f"Starting background sync for key {key_id}...")
        await ingestion_service.sync_one(key_id)
        async with AsyncSessionLocal() as db:
            await tax_engine.calculate_taxes(db)
        print(f"Background sync for key {key_id} completed successfully.")
    except Exception as e:
        print(f"Error in background sync for key {key_id}: {e}")
from app.models.transaction import Transaction as TransactionModel
from app.models.exchange_key import ExchangeKey as ExchangeKeyModel
from app.schemas.exchange_key import ExchangeKeyCreate, ExchangeKeyResponse
from app.services.ingestion import ingestion_service
from app.services.csv_ingestion import csv_ingestion_service
from app.services.tax_engine import tax_engine
from typing import List, Optional
import csv
import io
import os
import shutil
from fastapi.responses import StreamingResponse

router = APIRouter()

async def process_zip_background(temp_path: str):
    """
    Process ZIP file and recalculate taxes in the background.
    """
    try:
        print(f"Processing ZIP file {temp_path} in background...")
        await csv_ingestion_service.process_zip(temp_path)
        async with AsyncSessionLocal() as db:
            await tax_engine.calculate_taxes(db)
        print(f"Successfully processed ZIP file {temp_path} in background.")
    except Exception as e:
        print(f"Error in background ZIP processing: {e}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

@router.post("/upload")
async def upload_binance_zip(background_tasks: BackgroundTasks, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    """
    Uploads a Binance ZIP file (Statements or Trades) for historical ingestion.
    """
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files are supported")
    
    # Save to a temporary location
    temp_path = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Process the ZIP file in background
    background_tasks.add_task(process_zip_background, temp_path)
            
    return {"status": "upload_accepted", "message": f"Successfully uploaded {file.filename}. Processing in background."}

@router.post("/sync")
async def sync_data(background_tasks: BackgroundTasks):
    # Run sync and calculation in background so it doesn't block the request
    # and continues even if the user leaves the page.
    background_tasks.add_task(run_sync_and_calculate_background)
    return {"status": "sync_started"}

@router.post("/sync/{key_id}")
async def sync_one_key(key_id: int, background_tasks: BackgroundTasks):
    """
    Synchronizes a single API key in the background.
    """
    background_tasks.add_task(run_single_key_sync_background, key_id)
    return {"status": "sync_started"}

@router.get("/ledger", response_model=List[Transaction])
async def get_ledger(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(TransactionModel)
        .filter(TransactionModel.is_active == True)
        .order_by(TransactionModel.timestamp.desc())
    )
    return result.scalars().all()

@router.get("/kpi", response_model=KPIReport)
async def get_kpi(year: Optional[int] = None, tax_bracket: float = Query(0.25), db: AsyncSession = Depends(get_db)):
    return await tax_engine.get_kpi(db, year=year, tax_bracket=tax_bracket)

@router.get("/years", response_model=List[int])
async def get_years(db: AsyncSession = Depends(get_db)):
    return await tax_engine.get_years(db)

@router.get("/keys", response_model=List[ExchangeKeyResponse])
async def get_exchange_keys(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExchangeKeyModel).order_by(ExchangeKeyModel.created_at.desc()))
    return result.scalars().all()

@router.post("/keys", response_model=ExchangeKeyResponse)
async def create_exchange_key(key_data: ExchangeKeyCreate, db: AsyncSession = Depends(get_db)):
    # Check if a placeholder for this exchange already exists
    if not key_data.api_key:
        stmt = select(ExchangeKeyModel).filter(
            ExchangeKeyModel.exchange_name == key_data.exchange_name.lower(),
            ExchangeKeyModel.api_key == None
        )
        existing = (await db.execute(stmt)).scalars().first()
        if existing:
            return existing

    new_key = ExchangeKeyModel(
        exchange_name=key_data.exchange_name.lower(),
        api_key=key_data.api_key,
        api_secret=key_data.api_secret
    )
    db.add(new_key)
    await db.commit()
    await db.refresh(new_key)
    
    # Persist to .env for future-proofing
    try:
        env_path = ".env"
        prefix = new_key.exchange_name.upper()
        
        # Read existing lines
        lines = []
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()
        
        # Filter out existing keys for this exchange
        new_lines = [l for l in lines if not l.startswith(f"{prefix}_API_")]
        
        # Append new keys if they exist
        if new_key.api_key:
            new_lines.append(f"{prefix}_API_KEY={new_key.api_key}\n")
        if new_key.api_secret:
            new_lines.append(f"{prefix}_API_SECRET={new_key.api_secret}\n")
        
        with open(env_path, "w") as f:
            f.writelines(new_lines)
            
    except Exception as e:
        print(f"Warning: Failed to persist key to .env: {e}")

    return new_key

@router.get("/data-sources")
async def get_data_sources(db: AsyncSession = Depends(get_db)):
    from sqlalchemy import func
    # 1. Get transaction stats per exchange and source
    stmt = select(
        TransactionModel.exchange, 
        TransactionModel.source, 
        func.count(TransactionModel.id).label("count")
    ).group_by(TransactionModel.exchange, TransactionModel.source)
    result = await db.execute(stmt)
    rows = result.all()
    
    stats = {}
    for exchange, source, count in rows:
        if exchange not in stats:
            stats[exchange] = {"api_count": 0, "csv_count": 0, "has_key": False}
        if source == 'api':
            stats[exchange]["api_count"] = count
        else:
            stats[exchange]["csv_count"] = count
            
    # 2. Get key info
    keys_stmt = select(ExchangeKeyModel)
    keys_result = await db.execute(keys_stmt)
    keys = keys_result.scalars().all()
    for key in keys:
        if key.exchange_name not in stats:
            stats[key.exchange_name] = {"api_count": 0, "csv_count": 0, "has_key": False}
        stats[key.exchange_name]["has_key"] = bool(key.api_key and key.api_secret)
        stats[key.exchange_name]["key_id"] = key.id

    return [{"exchange": k, **v} for k, v in stats.items()]

@router.delete("/data-sources/{exchange_name}")
async def delete_data_source(exchange_name: str, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    from sqlalchemy import delete
    
    # 1. Delete transactions
    await db.execute(delete(TransactionModel).filter(TransactionModel.exchange == exchange_name))
    
    # 2. Delete API keys
    await db.execute(delete(ExchangeKeyModel).filter(ExchangeKeyModel.exchange_name == exchange_name))
    
    # 3. Remove from .env
    try:
        env_path = ".env"
        prefix = exchange_name.upper()
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()
            new_lines = [l for l in lines if not l.startswith(f"{prefix}_API_")]
            with open(env_path, "w") as f:
                f.writelines(new_lines)
    except: pass
    
    # 4. Commit
    await db.commit()

    # 5. Recalculate in background
    background_tasks.add_task(run_sync_and_calculate_background)
    
    return {"status": "deleted"}

@router.delete("/keys/{key_id}")
async def delete_exchange_key(key_id: int, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExchangeKeyModel).filter(ExchangeKeyModel.id == key_id))
    key = result.scalars().first()
    if not key:
        raise HTTPException(status_code=404, detail="Key not found")
    
    # When deleting a key, we also delete all data for that exchange as requested
    exchange_name = key.exchange_name
    
    # Logic from delete_data_source but combined here to use same background_tasks
    from sqlalchemy import delete
    await db.execute(delete(TransactionModel).filter(TransactionModel.exchange == exchange_name))
    await db.execute(delete(ExchangeKeyModel).filter(ExchangeKeyModel.id == key_id))
    
    try:
        env_path = ".env"
        prefix = exchange_name.upper()
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()
            new_lines = [l for l in lines if not l.startswith(f"{prefix}_API_")]
            with open(env_path, "w") as f:
                f.writelines(new_lines)
    except: pass

    await db.commit()
    background_tasks.add_task(run_sync_and_calculate_background)
    
    return {"status": "deleted"}

@router.get("/export/1399")
async def export_1399(db: AsyncSession = Depends(get_db)):
    # CSV generation for Form 1399
    result = await db.execute(select(TransactionModel).filter(TransactionModel.is_taxable_event == 1))
    txs = result.scalars().all()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Asset', 'Date of Purchase', 'Date of Sale', 'Cost Basis (ILS)', 'Proceeds (ILS)', 'Capital Gain (ILS)'])
    
    for tx in txs:
        purchase_date = tx.purchase_date if tx.purchase_date else tx.timestamp.date()
        writer.writerow([
            tx.asset_from,
            purchase_date.strftime('%d/%m/%Y'),
            tx.timestamp.strftime('%d/%m/%Y'),
            round(tx.cost_basis_ils or 0.0, 2),
            round((tx.cost_basis_ils or 0.0) + (tx.capital_gain_ils or 0.0), 2),
            round(tx.capital_gain_ils or 0.0, 2)
        ])
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=form_1399.csv"}
    )
