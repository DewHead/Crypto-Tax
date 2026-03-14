import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.services.tax_engine import TaxEngine
from app.models.transaction import Transaction, TransactionType
from app.db.session import Base
from datetime import datetime

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_ledger.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope="module")
async def db():
    # Setup
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with TestSessionLocal() as db_session:
        yield db_session
    
    # Teardown
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_fifo_logic(db):
    engine = TaxEngine()
    # Mock data injection
    # ...
    pass
