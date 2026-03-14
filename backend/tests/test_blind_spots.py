import pytest
import pytest_asyncio
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine
from app.db.session import Base
from datetime import datetime
from unittest.mock import AsyncMock, patch

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_ledger_blind.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope="module")
async def db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with TestSessionLocal() as db_session:
        yield db_session
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_missing_cost_basis_ita_default(db: AsyncSession):
    """ITA default for missing cost basis is ZERO."""
    await db.execute(delete(Transaction))
    
    # Sell 1 BTC for 50000 ILS with NO purchase history
    db.add(Transaction(
        exchange="test", timestamp=datetime(2025, 6, 1),
        type=TransactionType.sell, asset_from="BTC", amount_from=1.0,
        asset_to="ILS", amount_to=50000.0, is_active=True
    ))
    await db.commit()
    
    engine = TaxEngine()
    
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0):
        await engine.calculate_taxes(db)
    
    stmt = select(Transaction)
    res = await db.execute(stmt)
    tx = res.scalars().first()
    
    assert tx.is_issue == True
    assert tx.cost_basis_ils == 0.0
    assert tx.capital_gain_ils == 50000.0
