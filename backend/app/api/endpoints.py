from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.session import get_db, AsyncSessionLocal
from app.schemas.transaction import Transaction, KPIReport, ManualCostBasisUpdate
from app.schemas.app_setting import AppSettingResponse, AppSettingUpdate
from app.models.transaction import Transaction as TransactionModel
from app.models.exchange_key import ExchangeKey as ExchangeKeyModel
from app.models.app_setting import AppSetting as AppSettingModel
from app.schemas.exchange_key import ExchangeKeyCreate, ExchangeKeyResponse
from app.services.ingestion import ingestion_service
from app.services.csv_ingestion import csv_ingestion_service
from app.services.tax_engine import TaxEngine
from app.services.valuation import valuation_service
from app.services.export import export_service
from app.services.status import status_service
from app.services.email import send_notification_email
from typing import List, Optional
import csv
import io
import os
import shutil
from fastapi.responses import StreamingResponse

router = APIRouter()

async def get_notification_email(db: Optional[AsyncSession] = None):
    if db:
        result = await db.execute(select(AppSettingModel).filter(AppSettingModel.key == "notification_email"))
        setting = result.scalars().first()
        return setting.value if setting else None
    
    async with AsyncSessionLocal() as db_session:
        result = await db_session.execute(select(AppSettingModel).filter(AppSettingModel.key == "notification_email"))
        setting = result.scalars().first()
        return setting.value if setting else None

async def run_sync_and_calculate_background():
    """
    Run ingestion and tax calculation in the background.
    """
    try:
        print("Starting background full sync...")
        await ingestion_service.sync_all()
        async with AsyncSessionLocal() as db:
            await TaxEngine().calculate_taxes(db)
        print("Background full sync and tax calculation completed.")
        
        email = await get_notification_email()
        if email:
            await send_notification_email(
                "Crypto Tax: Full Sync Complete",
                "The full synchronization and tax re-calculation has completed successfully. Your dashboard charts are being updated in the background.",
                email
            )
        
        async with AsyncSessionLocal() as db:
            await valuation_service.update_daily_valuations(db)
        print("Background valuation update completed.")
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
            await TaxEngine().calculate_taxes(db)
        print(f"Background sync and tax calculation for key {key_id} completed.")
        
        email = await get_notification_email()
        if email:
            await send_notification_email(
                f"Crypto Tax: Sync Complete (Key {key_id})",
                f"The synchronization for exchange key {key_id} and tax re-calculation has completed successfully. Your dashboard charts are being updated in the background.",
                email
            )
        
        async with AsyncSessionLocal() as db:
            await valuation_service.update_daily_valuations(db)
        print(f"Background valuation update for key {key_id} completed.")
    except Exception as e:
        print(f"Error in background sync for key {key_id}: {e}")

async def process_zip_background(temp_path: str):
    """
    Process ZIP file and recalculate taxes in the background.
    """
    try:
        print(f"Processing ZIP file {temp_path} in background...")
        await csv_ingestion_service.process_zip(temp_path)
        async with AsyncSessionLocal() as db:
            await TaxEngine().calculate_taxes(db)
        print(f"Successfully processed ZIP file {temp_path} and recalculated taxes in background.")
        
        email = await get_notification_email()
        if email:
            await send_notification_email(
                "Crypto Tax: CSV Import Complete",
                f"The import of {os.path.basename(temp_path)} and tax re-calculation has completed successfully. Your dashboard charts are being updated in the background.",
                email
            )
        
        async with AsyncSessionLocal() as db:
            await valuation_service.update_daily_valuations(db)
        print(f"Background valuation update after ZIP processing completed.")
    except Exception as e:
        print(f"Error in background ZIP processing: {e}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

async def run_recalculate_background():
    """
    Run tax calculation in the background.
    """
    try:
        print("Starting background tax recalculation...")
        async with AsyncSessionLocal() as db:
            await TaxEngine().calculate_taxes(db)
            await valuation_service.update_daily_valuations(db)
        print("Background tax recalculation completed successfully.")
        
        email = await get_notification_email()
        if email:
            await send_notification_email(
                "Crypto Tax: Recalculation Complete",
                "The tax re-calculation has completed successfully.",
                email
            )
    except Exception as e:
        print(f"Error in background recalculation: {e}")

@router.post("/recalculate")
async def recalculate_taxes(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_recalculate_background)
    return {"status": "recalculation_started"}

@router.post("/transactions/{tx_id}/manual-cost-basis")
async def update_manual_cost_basis(tx_id: int, update: ManualCostBasisUpdate, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(TransactionModel).filter(TransactionModel.id == tx_id))
    tx = result.scalars().first()
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    tx.manual_cost_basis_ils = update.manual_cost_basis_ils
    tx.manual_purchase_date = update.manual_purchase_date
    await db.commit()
    
    # Trigger recalculation
    background_tasks.add_task(run_sync_and_calculate_background)
    return {"status": "updated"}

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
    background_tasks.add_task(run_sync_and_calculate_background)
    return {"status": "sync_started"}

@router.post("/sync/{key_id}")
async def sync_one_key(key_id: int, background_tasks: BackgroundTasks):
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

@router.get("/status")
async def get_status():
    return {
        "is_recalculating": status_service.is_recalculating
    }

@router.get("/kpi", response_model=KPIReport)
async def get_kpi(year: Optional[int] = None, tax_bracket: float = Query(0.25), db: AsyncSession = Depends(get_db)):
    return await TaxEngine().get_kpi(db, year=year, tax_bracket=tax_bracket)

@router.get("/unrealized-inventory")
async def get_unrealized_inventory(db: AsyncSession = Depends(get_db)):
    return await TaxEngine().get_unrealized_inventory(db)

@router.get("/years", response_model=List[int])
async def get_years(db: AsyncSession = Depends(get_db)):
    return await TaxEngine().get_years(db)

@router.get("/settings/{key}", response_model=AppSettingResponse)
async def get_setting(key: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AppSettingModel).filter(AppSettingModel.key == key))
    setting = result.scalars().first()
    if not setting:
        return AppSettingModel(key=key, value=None)
    return setting

@router.post("/settings/{key}", response_model=AppSettingResponse)
async def update_setting(key: str, update: AppSettingUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AppSettingModel).filter(AppSettingModel.key == key))
    setting = result.scalars().first()
    if setting:
        setting.value = update.value
    else:
        setting = AppSettingModel(key=key, value=update.value)
        db.add(setting)
    await db.commit()
    await db.refresh(setting)
    return setting

@router.get("/keys", response_model=List[ExchangeKeyResponse])
async def get_exchange_keys(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExchangeKeyModel).order_by(ExchangeKeyModel.created_at.desc()))
    return result.scalars().all()

@router.post("/keys", response_model=ExchangeKeyResponse)
async def create_exchange_key(key_data: ExchangeKeyCreate, db: AsyncSession = Depends(get_db)):
    exchange_name = key_data.exchange_name.lower()
    if not key_data.api_key:
        stmt = select(ExchangeKeyModel).filter(
            ExchangeKeyModel.exchange_name == exchange_name,
            ExchangeKeyModel.api_key == None
        )
        existing = (await db.execute(stmt)).scalars().first()
        if existing:
            return existing

    # If manually adding a key, clear the "wiped" flag
    stmt = select(AppSettingModel).filter(AppSettingModel.key == f"wiped_{exchange_name}")
    setting = (await db.execute(stmt)).scalars().first()
    if setting:
        setting.value = "false"

    new_key = ExchangeKeyModel(
        exchange_name=exchange_name,
        api_key=key_data.api_key,
        api_secret=key_data.api_secret
    )
    db.add(new_key)
    await db.commit()
    await db.refresh(new_key)
    return new_key

@router.get("/data-sources")
async def get_data_sources(db: AsyncSession = Depends(get_db)):
    from sqlalchemy import func
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
async def delete_data_source(exchange_name: str, background_tasks: BackgroundTasks, wipe_csv: bool = Query(False), db: AsyncSession = Depends(get_db)):
    from sqlalchemy import delete
    # Delete transactions for this exchange
    if wipe_csv:
        # Full wipe (API + CSV)
        await db.execute(delete(TransactionModel).filter(
            TransactionModel.exchange == exchange_name
        ))
    else:
        # Only delete API-sourced transactions, preserve CSV
        await db.execute(delete(TransactionModel).filter(
            TransactionModel.exchange == exchange_name,
            TransactionModel.source == 'api'
        ))
    
    # Always delete API keys for this exchange
    await db.execute(delete(ExchangeKeyModel).filter(ExchangeKeyModel.exchange_name == exchange_name))
    
    # Mark as wiped to prevent re-seeding from .env on restart
    from app.models.app_setting import AppSetting as AppSettingModel
    stmt = select(AppSettingModel).filter(AppSettingModel.key == f"wiped_{exchange_name}")
    setting = (await db.execute(stmt)).scalars().first()
    if setting:
        setting.value = "true"
    else:
        db.add(AppSettingModel(key=f"wiped_{exchange_name}", value="true"))

    await db.commit()
    background_tasks.add_task(run_sync_and_calculate_background)
    
    return {
        "status": "deleted", 
        "scope": "all" if wipe_csv else "api_only",
        "exchange": exchange_name
    }

@router.delete("/keys/{key_id}")
async def delete_exchange_key(key_id: int, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExchangeKeyModel).filter(ExchangeKeyModel.id == key_id))
    key = result.scalars().first()
    if not key:
        raise HTTPException(status_code=404, detail="Key not found")

    exchange_name = key.exchange_name
    from sqlalchemy import delete
    # Only delete API-sourced transactions for this exchange, preserve CSV
    await db.execute(delete(TransactionModel).filter(
        TransactionModel.exchange == exchange_name,
        TransactionModel.source == 'api'
    ))
    await db.execute(delete(ExchangeKeyModel).filter(ExchangeKeyModel.id == key_id))

    # Mark as wiped to prevent re-seeding from .env on restart
    stmt = select(AppSettingModel).filter(AppSettingModel.key == f"wiped_{exchange_name}")
    setting = (await db.execute(stmt)).scalars().first()
    if setting:
        setting.value = "true"
    else:
        db.add(AppSettingModel(key=f"wiped_{exchange_name}", value="true"))

    await db.commit()
    background_tasks.add_task(run_sync_and_calculate_background)
    return {"status": "deleted_api_key_and_api_data"}
@router.post("/test-email")
async def send_test_email(db: AsyncSession = Depends(get_db)):
    email = await get_notification_email(db)
    if not email:
        raise HTTPException(status_code=400, detail="Notification email not configured")
    
    try:
        await send_notification_email(
            "Crypto Tax: Test Email",
            "This is a test email from your Crypto Tax application. If you received this, your email configuration is working correctly!",
            email
        )
        return {"status": "email_sent", "recipient": email}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send test email: {str(e)}")

@router.get("/export/8659")
async def export_8659(year: Optional[int] = None, db: AsyncSession = Depends(get_db)):
    csv_content = await export_service.generate_form_8659_csv(db, year=year)
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=form_8659_{year if year else 'all'}.csv"}
    )
