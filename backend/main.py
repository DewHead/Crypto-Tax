import time
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import router as api_router
from app.db.session import engine, Base
from app.models.exchange_key import ExchangeKey
from app.models.transaction import Transaction
from app.models.ils_rate import ILSRate
from app.models.daily_valuation import DailyValuation
from app.services.ingestion import ingestion_service
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:     %(message)s",
)
logger = logging.getLogger("app")

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

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Try to find the route description
    description = "API Request"
    for route in app.routes:
        if hasattr(route, "matches"):
            match, _ = route.matches(request.scope)
            from starlette.routing import Match
            if match == Match.FULL:
                if hasattr(route, "endpoint"):
                    # Get docstring or function name
                    doc = route.endpoint.__doc__
                    if doc:
                        description = doc.strip().split("\n")[0]
                    else:
                        description = route.name.replace("_", " ").title()
                break
    
    response = await call_next(request)
    
    process_time = (time.time() - start_time) * 1000
    formatted_process_time = "{0:.2f}".format(process_time)
    
    logger.info(
        f"{request.client.host}:{request.client.port} - \"{request.method} {request.url.path}\" "
        f"{response.status_code} OK | {description} | {formatted_process_time}ms"
    )
    
    return response

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
