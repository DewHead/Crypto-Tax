import pytest
from datetime import datetime, timedelta
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine
from app.db.session import AsyncSessionLocal
from sqlalchemy import delete

@pytest.mark.asyncio
async def test_transfer_reconciliation_time_window():
    async with AsyncSessionLocal() as db:
        # Cleanup
        await db.execute(delete(Transaction))
        
        # 1. Withdrawal at T=0
        t0 = datetime(2026, 1, 1, 12, 0)
        w = Transaction(
            exchange="binance",
            timestamp=t0,
            type=TransactionType.withdrawal,
            asset_from="BTC",
            amount_from=1.0,
            amount_to=1.0,
            source="api"
        )
        
        # 2. Deposit at T=+5h
        t1 = t0 + timedelta(hours=5)
        d = Transaction(
            exchange="kraken",
            timestamp=t1,
            type=TransactionType.deposit,
            asset_to="BTC",
            amount_to=0.99, # 1% fee
            amount_from=0.99,
            source="api"
        )
        
        db.add(w)
        db.add(d)
        await db.commit()
        
        engine = TaxEngine()
        await engine.calculate_taxes(db)
        
        # Refresh
        from sqlalchemy import select
        res = await db.execute(select(Transaction).order_by(Transaction.timestamp.asc()))
        txs = res.scalars().all()
        
        assert len(txs) == 2
        assert txs[0].is_taxable_event == 0
        assert txs[1].is_taxable_event == 0

@pytest.mark.asyncio
async def test_passive_income_taxation():
    async with AsyncSessionLocal() as db:
        # Cleanup
        await db.execute(delete(Transaction))
        
        # Earn event
        t0 = datetime(2026, 1, 1, 12, 0)
        e = Transaction(
            exchange="binance",
            timestamp=t0,
            type=TransactionType.earn,
            asset_to="ADA",
            amount_to=100.0,
            amount_from=0.0,
            source="api"
        )
        db.add(e)
        await db.commit()
        
        engine = TaxEngine()
        await engine.calculate_taxes(db)
        
        # Refresh
        from sqlalchemy import select
        res = await db.execute(select(Transaction).filter(Transaction.type == TransactionType.earn))
        tx = res.scalars().first()
        
        assert tx.is_taxable_event == 1
        assert tx.ordinary_income_ils > 0
        assert tx.capital_gain_ils == 0
        assert tx.cost_basis_ils == tx.ordinary_income_ils

@pytest.mark.asyncio
async def test_loss_harvesting_carry_forward():
    async with AsyncSessionLocal() as db:
        # Cleanup
        await db.execute(delete(Transaction))
        
        # Year 2024: Loss of 1000
        # Buy at 2000, sell at 1000
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2024, 1, 1), type=TransactionType.buy,
            asset_to="BTC", amount_to=1.0, amount_from=2000.0, asset_from="ILS"
        ))
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2024, 6, 1), type=TransactionType.sell,
            asset_from="BTC", amount_from=1.0, amount_to=1000.0, asset_to="ILS"
        ))
        
        # Year 2025: Gain of 600
        # Buy at 1000, sell at 1600
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2025, 1, 1), type=TransactionType.buy,
            asset_to="ETH", amount_to=1.0, amount_from=1000.0, asset_from="ILS"
        ))
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2025, 6, 1), type=TransactionType.sell,
            asset_from="ETH", amount_from=1.0, amount_to=1600.0, asset_to="ILS"
        ))
        
        await db.commit()
        
        engine = TaxEngine()
        await engine.calculate_taxes(db)
        
        # KPI for 2025
        kpi_2025 = await engine.get_kpi(db, year=2025)
        
        # 600 gain - 1000 loss = -400 loss (reported as 0 gain, 400 carried forward)
        assert kpi_2025['net_capital_gain_ils'] == 0
        assert kpi_2025['carried_forward_loss_ils'] == 400

@pytest.mark.asyncio
async def test_reconciliation_inventory_fee():
    async with AsyncSessionLocal() as db:
        # Cleanup
        await db.execute(delete(Transaction))
        
        # 1. Buy 1 BTC for 10000 ILS
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2026, 1, 1), type=TransactionType.buy,
            asset_to="BTC", amount_to=1.0, amount_from=10000.0, asset_from="ILS"
        ))
        
        # 2. Reconciled Transfer (1.0 out, 0.99 in)
        t0 = datetime(2026, 2, 1)
        db.add(Transaction(
            exchange="binance",
            timestamp=t0,
            type=TransactionType.withdrawal,
            asset_from="BTC", amount_from=1.0, amount_to=1.0
        ))
        db.add(Transaction(
            exchange="kraken",
            timestamp=t0 + timedelta(hours=1),
            type=TransactionType.deposit,
            asset_to="BTC", amount_to=0.99, amount_from=0.99
        ))
        
        # 3. Sell 0.99 BTC for 15000 ILS
        db.add(Transaction(
            exchange="test",
            timestamp=datetime(2026, 3, 1), type=TransactionType.sell,
            asset_from="BTC", amount_from=0.99, amount_to=15000.0, asset_to="ILS"
        ))
        
        await db.commit()
        
        engine = TaxEngine()
        await engine.calculate_taxes(db)
        
        # Verify sell cost basis
        from sqlalchemy import select
        stmt = select(Transaction).filter(Transaction.type == TransactionType.sell)
        result = await db.execute(stmt)
        sell_tx = result.scalars().first()
        
        # Cost basis should be 9900 (0.99 of the original 10000)
        # Because 1% was lost as fee during transfer
        assert round(sell_tx.cost_basis_ils, 0) == 9900
        assert sell_tx.capital_gain_ils == 15000 - 9900

