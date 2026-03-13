from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import router as api_router
from app.db.session import engine, Base
from app.models.exchange_key import ExchangeKey
from app.models.transaction import Transaction
from app.models.ils_rate import ILSRate
from app.services.ingestion import ingestion_service
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables in SQLite
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Sync keys from environment
    await ingestion_service.sync_env_keys()
    
    # Reset any stale sync flags from previous runs
    await ingestion_service.reset_sync_status()
    
    yield
    await engine.dispose()

app = FastAPI(title="Israeli Crypto Tax API (ITA 2026)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
