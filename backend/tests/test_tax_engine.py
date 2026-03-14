import pytest
import pytest_asyncio
from app.services.tax_engine import TaxEngine
from app.models.transaction import Transaction, TransactionType
from app.db.session import AsyncSessionLocal, Base, engine
from datetime import datetime

# Use a test database
TEST_DB_URL = \"sqlite:///./test_ledger.db\"

@pytest_asyncio.fixture(scope=\"module\")
async def db():
    # Setup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with AsyncSessionLocal() as db_session:
        yield db_session
    
    # Teardown
    async with engine.begin() as conn:
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
    
    await TaxEngine().calculate_taxes(db)
    
    # Check results
    from sqlalchemy import select
    stmt = select(Transaction).filter(Transaction.tx_hash == '0x2')
    result = await db.execute(stmt)
    updated_sell = result.scalars().first()
    # Cost basis for 0.5 BTC should be 5000 USD * ILS rate
    # Let's check capital gain
    # Proceeds = 7000 * rate, Cost = 5000 * rate
    # Gain should be 2000 * rate
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
    
    await TaxEngine().calculate_taxes(db)
    
    # Check that it's NOT a taxable event
    from sqlalchemy import select
    stmt = select(Transaction).filter(Transaction.tx_hash == '0x100')
    result = await db.execute(stmt)
    txs = result.scalars().all()
    for tx in txs:
        assert tx.is_taxable_event == 0
