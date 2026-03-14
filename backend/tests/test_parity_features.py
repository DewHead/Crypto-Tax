import pytest
import pytest_asyncio
from app.services.tax_engine import TaxEngine
from app.models.transaction import Transaction, TransactionType
from app.models.exchange_key import ExchangeKey
from app.models.app_setting import AppSetting
from app.models.cpi_rate import CPIRate
from app.models.daily_valuation import DailyValuation
from app.models.historical_price import HistoricalPrice
from app.models.ils_rate import ILSRate
from app.models.tax_lot_consumption import TaxLotConsumption
from app.db.session import Base
from datetime import datetime, timedelta
from sqlalchemy import select, delete

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Use a separate test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_parity.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest_asyncio.fixture
async def db():
    async with TestSessionLocal() as session:
        yield session

@pytest.mark.asyncio
async def test_avalanche_merge(db):
    T = datetime(2025, 1, 1, 10, 0, 0)
    # 3 sell trades of BTC on Binance occurring at T, T+1s, T+2s
    for i in range(3):
        tx = Transaction(
            exchange='binance',
            tx_hash=f'trade_{i}',
            timestamp=T + timedelta(seconds=i),
            type=TransactionType.sell,
            asset_from='BTC',
            amount_from=0.1,
            asset_to='USDT',
            amount_to=5000.0,
            fee_amount=1.0,
            fee_asset='USDT',
            is_taxable_event=1
        )
        db.add(tx)
    await db.commit()
    
    # We need some inventory to avoid short sale issue notes (though not strictly required for merge test)
    buy_tx = Transaction(
        exchange='binance',
        tx_hash='buy_1',
        timestamp=T - timedelta(days=1),
        type=TransactionType.buy,
        asset_from='USDT',
        amount_from=20000.0,
        asset_to='BTC',
        amount_to=1.0,
        fee_amount=0.0,
        is_taxable_event=0
    )
    db.add(buy_tx)
    await db.commit()

    await TaxEngine().calculate_taxes(db)
    
    # After calculation, internal TaxEngine().calculate_taxes logic merges txs
    # Note: calculate_taxes UPDATES the transactions in DB.
    # However, our current TaxEngine().calculate_taxes implementation in the prompt
    # replaces the local `txs` list with `merged_txs`, but it only `db.add(tx)` for the txs in the merged list.
    # This means the original individual transactions that were NOT the first in a group
    # are NOT updated and NOT added to session. 
    # Wait, if they were already in the DB, they stay in the DB but aren't processed.
    
    # Let's check how many transactions have cost_basis_ils > 0 (which means they were processed)
    stmt = select(Transaction).filter(Transaction.type == TransactionType.sell)
    result = await db.execute(stmt)
    sells = result.scalars().all()
    
    # The first sell (trade_0) should be merged and have the sum of all 3
    merged_sell = next(s for s in sells if s.tx_hash == 'trade_0')
    assert merged_sell.amount_from == pytest.approx(0.3)
    assert merged_sell.amount_to == pytest.approx(15000.0)
    assert merged_sell.fee_amount == pytest.approx(3.0)
    assert "Merged 3 trades" in merged_sell.raw_data
    
    # The other 2 sells should have been skipped (no cost_basis_ils updated from default 0.0)
    other_sells = [s for s in sells if s.tx_hash in ['trade_1', 'trade_2']]
    for s in other_sells:
        assert s.cost_basis_ils == 0.0 or s.cost_basis_ils is None

@pytest.mark.asyncio
async def test_missing_cost_basis(db):
    T = datetime(2025, 2, 1)
    # Sell without prior deposits
    tx = Transaction(
        exchange='binance',
        tx_hash='sell_no_cost',
        timestamp=T,
        type=TransactionType.sell,
        asset_from='ETH',
        amount_from=1.0,
        asset_to='USDT',
        amount_to=3000.0,
        is_taxable_event=1
    )
    db.add(tx)
    await db.commit()
    
    await TaxEngine().calculate_taxes(db)
    
    stmt = select(Transaction).filter(Transaction.tx_hash == 'sell_no_cost')
    result = await db.execute(stmt)
    updated_tx = result.scalars().first()
    
    assert updated_tx.is_issue is True
    assert "Missing cost basis" in updated_tx.issue_notes

@pytest.mark.asyncio
async def test_transfer_linking(db):
    T = datetime(2025, 3, 1)
    # Withdrawal from Binance
    w = Transaction(
        exchange='binance',
        tx_hash='tx_w',
        timestamp=T,
        type=TransactionType.withdrawal,
        asset_from='SOL',
        amount_from=10.0,
        is_taxable_event=0
    )
    # Deposit to Kraken
    d = Transaction(
        exchange='kraken',
        tx_hash='tx_d',
        timestamp=T + timedelta(hours=2),
        type=TransactionType.deposit,
        asset_to='SOL',
        amount_to=9.9, # 1% fee
        is_taxable_event=0
    )
    db.add(w)
    db.add(d)
    await db.commit()
    
    await TaxEngine().calculate_taxes(db)
    
    # Refresh from DB to get IDs
    stmt = select(Transaction).filter(Transaction.tx_hash.in_(['tx_w', 'tx_d']))
    result = await db.execute(stmt)
    txs = result.scalars().all()
    w_db = next(t for t in txs if t.tx_hash == 'tx_w')
    d_db = next(t for t in txs if t.tx_hash == 'tx_d')
    
    assert w_db.linked_transaction_id == d_db.id
    assert d_db.linked_transaction_id == w_db.id
    assert w_db.category == "Transfer"
    assert d_db.category == "Transfer"

@pytest.mark.asyncio
async def test_delete_data_source(db):
    # We need to mock .env for this test or handle the failure
    # For simplicity in testing, we'll just check the DB part
    # Insert mock data
    tx = Transaction(
        exchange='mock_ex',
        tx_hash='mock_tx',
        timestamp=datetime.now(),
        type=TransactionType.buy,
        asset_to='BTC',
        amount_to=1.0,
        source='csv'
    )
    key = ExchangeKey(
        exchange_name='mock_ex',
        api_key='key',
        api_secret='secret'
    )
    db.add(tx)
    db.add(key)
    await db.commit()
    
    # Verify they exist
    assert (await db.execute(select(Transaction).filter(Transaction.exchange == 'mock_ex'))).scalars().first() is not None
    assert (await db.execute(select(ExchangeKey).filter(ExchangeKey.exchange_name == 'mock_ex'))).scalars().first() is not None
    
    # Call the deletion logic (we can call the endpoint directly or the logic)
    from app.api.endpoints import delete_data_source
    from fastapi import BackgroundTasks
    await delete_data_source('mock_ex', BackgroundTasks(), db=db)
    
    # Verify they are gone
    assert (await db.execute(select(Transaction).filter(Transaction.exchange == 'mock_ex'))).scalars().first() is None
    assert (await db.execute(select(ExchangeKey).filter(ExchangeKey.exchange_name == 'mock_ex'))).scalars().first() is None

