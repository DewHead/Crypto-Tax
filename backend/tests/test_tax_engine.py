import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select
from app.services.tax_engine import TaxEngine
from app.models.transaction import Transaction, TransactionType
from app.models.daily_valuation import DailyValuation
from app.models.exchange_key import ExchangeKey
from app.models.tax_lot_consumption import TaxLotConsumption
from app.db.session import Base
from datetime import datetime
from unittest.mock import AsyncMock, patch

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_ledger.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope="function")
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
async def test_fifo_simple_buy_sell(db):
    # 1. Buy 1 BTC
    buy_tx = Transaction(
        exchange='test',
        tx_hash='0x1',
        timestamp=datetime(2025, 1, 1),
        type=TransactionType.buy,
        asset_from='USD',
        amount_from=10000.0,
        asset_to='BTC',
        amount_to=1.0,
        fee_amount=0.0,
        is_taxable_event=0
    )
    db.add(buy_tx)
    await db.commit()
    
    # 2. Sell 0.5 BTC
    sell_tx = Transaction(
        exchange='test',
        tx_hash='0x2',
        timestamp=datetime(2025, 2, 1),
        type=TransactionType.sell,
        asset_from='BTC',
        amount_from=0.5,
        asset_to='USD',
        amount_to=7000.0,
        fee_amount=0.0,
        is_taxable_event=1
    )
    db.add(sell_tx)
    await db.commit()
    
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5), \
         patch('app.services.price.price_service.get_historical_price', side_effect=[50000.0, 60000.0]):
        await TaxEngine().calculate_taxes(db)
    
    # Check results
    from sqlalchemy import select
    stmt = select(Transaction).filter(Transaction.tx_hash == '0x2')
    result = await db.execute(stmt)
    updated_sell = result.scalars().first()
    
    assert updated_sell.capital_gain_ils > 0
    assert updated_sell.cost_basis_ils > 0

@pytest.mark.asyncio
async def test_transfer_reconciliation(db):
    # 1. Withdrawal from Exchange A
    w_tx = Transaction(
        exchange='Binance',
        tx_hash='0x100',
        timestamp=datetime(2025, 1, 1),
        type=TransactionType.withdrawal,
        asset_from='BTC',
        amount_from=0.1,
        asset_to='BTC',
        amount_to=0.1,
        fee_amount=0.0
    )
    db.add(w_tx)
    
    # 2. Deposit on Exchange B with same hash
    d_tx = Transaction(
        exchange='Kraken',
        tx_hash='0x100',
        timestamp=datetime(2025, 1, 1, 1),
        type=TransactionType.deposit,
        asset_from='BTC',
        amount_from=0.1,
        asset_to='BTC',
        amount_to=0.1,
        fee_amount=0.0
    )
    db.add(d_tx)
    await db.commit()
    
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5):
        await TaxEngine().calculate_taxes(db)
    
    # Check that it's NOT a taxable event
    from sqlalchemy import select
    stmt = select(Transaction).filter(Transaction.tx_hash == '0x100')
    result = await db.execute(stmt)
    txs = result.scalars().all()
    for tx in txs:
        assert tx.is_taxable_event == 0

@pytest.mark.asyncio
async def test_ordinary_income_recognition(db):
    # 1. Staking Reward (Earn)
    earn_tx = Transaction(
        exchange='Binance',
        tx_hash='0x201',
        timestamp=datetime(2025, 3, 1),
        type=TransactionType.earn,
        asset_to='ETH',
        amount_to=0.1,
        is_active=True
    )
    db.add(earn_tx)
    
    # 2. Airdrop
    airdrop_tx = Transaction(
        exchange='Kraken',
        tx_hash='0x202',
        timestamp=datetime(2025, 3, 15),
        type=TransactionType.airdrop,
        asset_to='SOL',
        amount_to=10.0,
        is_active=True
    )
    db.add(airdrop_tx)
    await db.commit()
    
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=3.5), \
         patch('app.services.price.price_service.get_historical_price', side_effect=[2000.0, 100.0]):
        # ETH price = 2000 USD -> 7000 ILS. 0.1 ETH = 700 ILS.
        # SOL price = 100 USD -> 350 ILS. 10 SOL = 3500 ILS.
        await TaxEngine().calculate_taxes(db)
    
    # Verify Earn TX
    stmt_earn = select(Transaction).filter(Transaction.tx_hash == '0x201')
    res_earn = await db.execute(stmt_earn)
    tx_earn = res_earn.scalars().first()
    assert tx_earn.is_taxable_event == 1
    assert tx_earn.ordinary_income_ils == pytest.approx(700.0)
    
    # Verify Airdrop TX
    stmt_airdrop = select(Transaction).filter(Transaction.tx_hash == '0x202')
    res_airdrop = await db.execute(stmt_airdrop)
    tx_airdrop = res_airdrop.scalars().first()
    assert tx_airdrop.is_taxable_event == 1
    assert tx_airdrop.ordinary_income_ils == pytest.approx(3500.0)
    
    # Verify KPI
    kpi = await TaxEngine().get_kpi(db, year=2025)
    assert kpi['ordinary_income_ils'] == pytest.approx(4200.0)
