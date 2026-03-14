import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.db.session import Base
from datetime import datetime, timezone
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine, get_jerusalem_date
from unittest.mock import AsyncMock, patch

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_ledger_advanced.db"
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

@pytest.fixture
def tax_engine():
    return TaxEngine()

@pytest.mark.asyncio
async def test_wash_sale_forward_rule(tax_engine, db):
    """
    Test 30-day Wash Sale Forward Rule: Loss on sale is deferred if replacement is bought within 30 days.
    """
    # 1. Buy 1 BTC
    buy1 = Transaction(
        exchange='test', timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='USD', amount_from=50000.0, is_active=True
    )
    db.add(buy1)
    await db.commit()

    # 2. Sell 1 BTC at a loss (Price dropped to 40k)
    sell1 = Transaction(
        exchange='test', timestamp=datetime(2025, 1, 10, tzinfo=timezone.utc),
        type=TransactionType.sell, asset_from='BTC', amount_from=1.0,
        asset_to='USD', amount_to=40000.0, is_active=True
    )
    db.add(sell1)
    await db.commit()

    # 3. Buy replacement 1 BTC within 30 days (on Jan 15)
    buy2 = Transaction(
        exchange='test', timestamp=datetime(2025, 1, 15, tzinfo=timezone.utc),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='USD', amount_from=42000.0, is_active=True
    )
    db.add(buy2)
    await db.commit()

    with patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5), \
         patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0), \
         patch('app.services.price.price_service.get_historical_price', side_effect=[50000.0, 40000.0, 42000.0, 50000.0]):
        
        await tax_engine.calculate_taxes(db, use_wash_sale_rule=True)

    # Verify sell1 has 0 loss (it was deferred)
    await db.refresh(sell1)
    assert sell1.capital_gain_ils == 0.0
    
    sell2 = Transaction(
        exchange='test', timestamp=datetime(2025, 2, 1, tzinfo=timezone.utc),
        type=TransactionType.sell, asset_from='BTC', amount_from=1.0,
        asset_to='USD', amount_to=50000.0, is_active=True
    )
    db.add(sell2)
    await db.commit()
    
    with patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5), \
         patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0), \
         patch('app.services.price.price_service.get_historical_price', side_effect=[50000.0, 40000.0, 42000.0, 50000.0]):
        await tax_engine.calculate_taxes(db, use_wash_sale_rule=True)
    
    await db.refresh(sell2)
    # buy2 cost basis should be 182000 ILS (147000 + 35000)
    assert sell2.cost_basis_ils == 182000.0

@pytest.mark.asyncio
async def test_cpi_madad_adjustment(tax_engine, db):
    """
    Test Madad adjustment for long term hold.
    """
    # Clear DB
    from sqlalchemy import delete
    await db.execute(delete(Transaction))
    await db.commit()

    # Buy in Jan (CPI 100)
    buy = Transaction(
        exchange='test', timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='ILS', amount_from=100000.0, is_active=True
    )
    db.add(buy)
    
    # Sell in Dec (CPI 105)
    sell = Transaction(
        exchange='test', timestamp=datetime(2025, 12, 1, tzinfo=timezone.utc),
        type=TransactionType.sell, asset_from='BTC', amount_from=1.0,
        asset_to='ILS', amount_to=150000.0, is_active=True
    )
    db.add(sell)
    await db.commit()

    async def mock_cpi(date, db_in):
        if date.month == 1: return 100.0
        return 105.0

    with patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0), \
         patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.cpi.cpi_service.get_cpi_index', side_effect=mock_cpi):
        
        await tax_engine.calculate_taxes(db)

    await db.refresh(sell)
    assert sell.capital_gain_ils == 50000.0
    assert sell.inflationary_gain_ils == 5000.0
    assert sell.real_gain_ils == 45000.0

@pytest.mark.asyncio
async def test_jerusalem_timezone_rollover(tax_engine, db):
    """
    Test that Dec 31st 23:30 UTC is Jan 1st in Israel.
    """
    from sqlalchemy import delete
    await db.execute(delete(Transaction))
    await db.commit()

    # Dec 31, 2025 23:30 UTC = Jan 1, 2026 01:30 Jerusalem
    tx = Transaction(
        exchange='test', timestamp=datetime(2025, 12, 31, 23, 30, tzinfo=timezone.utc),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='ILS', amount_from=100000.0, is_active=True
    )
    db.add(tx)
    await db.commit()
    
    assert get_jerusalem_date(tx.timestamp).year == 2026
    
    years = await tax_engine.get_years(db)
    assert 2026 in years
    assert 2025 not in years
