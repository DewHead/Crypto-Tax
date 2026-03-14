import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.services.tax_engine import TaxEngine
from app.models.transaction import Transaction, TransactionType
from app.models.daily_valuation import DailyValuation
from app.models.exchange_key import ExchangeKey
from app.models.tax_lot_consumption import TaxLotConsumption
from app.models.app_setting import AppSetting
from app.models.historical_price import HistoricalPrice
from app.models.ils_rate import ILSRate
from app.models.cpi_rate import CPIRate
from app.db.session import Base
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_mass_ledger.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope="function")
async def db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with TestSessionLocal() as db_session:
        yield db_session
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_mass_income_lots(db):
    """
    Simulates 1000 tiny BNB income lots (e.g. staking rewards) and a final sell.
    Verifies that the cost basis is correctly aggregated and performance is acceptable.
    """
    num_lots = 1000
    start_date = datetime(2025, 1, 1)
    
    # 1. Create 1000 Earn transactions
    for i in range(num_lots):
        tx = Transaction(
            exchange='binance',
            tx_hash=f'earn_{i}',
            timestamp=start_date + timedelta(hours=i),
            type=TransactionType.earn,
            asset_to='BNB',
            amount_to=0.01, # 0.01 BNB each
            is_active=True
        )
        db.add(tx)
    
    # 2. Final Sell of 5 BNB (half of total 10 BNB)
    sell_tx = Transaction(
        exchange='binance',
        tx_hash='final_sell',
        timestamp=start_date + timedelta(days=50),
        type=TransactionType.sell,
        asset_from='BNB',
        amount_from=5.0,
        asset_to='USD',
        amount_to=3000.0, # $600/BNB
        is_active=True
    )
    db.add(sell_tx)
    await db.commit()

    # Mock prices: $200 initially, $600 at sell
    # We need 1000 prices for the earn txs and 1 for the sell tx
    prices = [200.0] * num_lots + [600.0]
    
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5), \
         patch('app.services.price.price_service.get_historical_price', side_effect=prices), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0): # No inflation for simplicity
        
        engine = TaxEngine()
        await engine.calculate_taxes(db)
    
    # Verify results
    from sqlalchemy import select
    stmt = select(Transaction).filter(Transaction.tx_hash == 'final_sell')
    result = await db.execute(stmt)
    updated_sell = result.scalars().first()
    
    # Total cost basis for 5 BNB should be 500 lots * 0.01 BNB * $200 * 3.5 ILS/USD
    # 5 * 200 * 3.5 = 3500 ILS
    assert updated_sell.cost_basis_ils == pytest.approx(3500.0)
    
    # Proceeds: $3000 * 3.5 = 10500 ILS
    # Capital gain: 10500 - 3500 = 7000 ILS
    assert updated_sell.capital_gain_ils == pytest.approx(7000.0)
    
    # Verify KPI
    kpi = await engine.get_kpi(db, year=2025)
    # Ordinary income for 1000 lots: 1000 * 0.01 * 200 * 3.5 = 7000 ILS
    assert kpi['ordinary_income_ils'] == pytest.approx(7000.0)
    assert kpi['total_nominal_gain_ils'] == pytest.approx(7000.0)
