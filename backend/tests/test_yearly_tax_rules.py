import pytest
from datetime import datetime, date, timedelta
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine
from sqlalchemy import select
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_loss_harvesting_carry_forward(db):
    # Cleanup
    from app.models.tax_lot_consumption import TaxLotConsumption
    from app.models.daily_valuation import DailyValuation
    await db.execute(TaxLotConsumption.__table__.delete())
    await db.execute(Transaction.__table__.delete())
    await db.execute(DailyValuation.__table__.delete())
    await db.commit()

    engine_inst = TaxEngine()

    # 1. Buy 1 BTC in 2024 for 100,000 ILS
    tx1 = Transaction(
        exchange='binance', timestamp=datetime(2024, 1, 1),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='ILS', amount_from=100000.0,
        is_active=True
    )
    db.add(tx1)
    
    # 2. Sell 1 BTC in 2024 for 90,000 ILS (10,000 loss)
    tx2 = Transaction(
        exchange='binance', timestamp=datetime(2024, 6, 1),
        type=TransactionType.sell, asset_from='BTC', amount_from=1.0,
        asset_to='ILS', amount_to=90000.0,
        is_active=True
    )
    db.add(tx2)
    
    await db.commit()
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0):
        await engine_inst.calculate_taxes(db)
    
    kpi_2024 = await engine_inst.get_kpi(db, year=2024)
    assert kpi_2024['net_capital_gain_ils'] == 0
    assert kpi_2024['carried_forward_loss_ils'] >= 10000 # Loss
    
    loss_2024 = kpi_2024['carried_forward_loss_ils']

    # 3. In 2025, Buy 1 ETH for 5,000 ILS
    tx3 = Transaction(
        exchange='binance', timestamp=datetime(2025, 1, 1),
        type=TransactionType.buy, asset_to='ETH', amount_to=1.0,
        asset_from='ILS', amount_from=5000.0,
        is_active=True
    )
    db.add(tx3)
    
    # 4. Sell 1 ETH for 10,000 ILS (5,000 gain)
    tx4 = Transaction(
        exchange='binance', timestamp=datetime(2025, 6, 1),
        type=TransactionType.sell, asset_from='ETH', amount_from=1.0,
        asset_to='ILS', amount_to=10000.0,
        is_active=True
    )
    db.add(tx4)
    
    await db.commit()
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0):
        await engine_inst.calculate_taxes(db)
    
    kpi_2025 = await engine_inst.get_kpi(db, year=2025)
    
    # Gain was 5000. Carry forward loss from 2024 should offset it.
    assert kpi_2025['net_capital_gain_ils'] == 0
    assert kpi_2025['carried_forward_loss_ils'] < loss_2024

@pytest.mark.asyncio
async def test_multi_year_offset_complex(db):
    from app.models.tax_lot_consumption import TaxLotConsumption
    await db.execute(TaxLotConsumption.__table__.delete())
    await db.execute(Transaction.__table__.delete())
    await db.commit()

    engine_inst = TaxEngine()

    # 2017: 1000 ILS Loss
    db.add(Transaction(
        exchange='binance', timestamp=datetime(2017, 1, 1),
        type=TransactionType.buy, asset_to='L1', amount_to=1.0,
        asset_from='ILS', amount_from=2000.0, is_active=True
    ))
    db.add(Transaction(
        exchange='binance', timestamp=datetime(2017, 2, 1),
        type=TransactionType.sell, asset_from='L1', amount_from=1.0,
        asset_to='ILS', amount_to=1000.0, is_active=True
    ))
    
    # 2025: 600 ILS Gain
    db.add(Transaction(
        exchange='binance', timestamp=datetime(2025, 1, 1),
        type=TransactionType.buy, asset_to='G1', amount_to=1.0,
        asset_from='ILS', amount_from=1000.0, is_active=True
    ))
    db.add(Transaction(
        exchange='binance', timestamp=datetime(2025, 2, 1),
        type=TransactionType.sell, asset_from='G1', amount_from=1.0,
        asset_to='ILS', amount_to=1600.0, is_active=True
    ))

    await db.commit()
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0):
        await engine_inst.calculate_taxes(db)

    kpi_2025 = await engine_inst.get_kpi(db, year=2025)
    
    assert kpi_2025['net_capital_gain_ils'] == 0

@pytest.mark.asyncio
async def test_reconciliation_inventory_fee(db):
    # Cleanup
    from app.models.tax_lot_consumption import TaxLotConsumption
    await db.execute(TaxLotConsumption.__table__.delete())
    await db.execute(Transaction.__table__.delete())
    await db.commit()

    engine_inst = TaxEngine()

    # 1. Buy 1 BTC for 10,000 USD
    db.add(Transaction(
        exchange='binance', timestamp=datetime(2021, 1, 1),
        type=TransactionType.buy, asset_to='BTC', amount_to=1.0,
        asset_from='USD', amount_from=10000.0, is_active=True
    ))
    
    # 2. Transfer 1 BTC to Kraken with 0.01 fee
    w = Transaction(
        exchange='binance', timestamp=datetime(2021, 2, 1, 10, 0, 0),
        type=TransactionType.withdrawal, asset_from='BTC', amount_from=1.01,
        is_active=True
    )
    d = Transaction(
        exchange='kraken', timestamp=datetime(2021, 2, 1, 10, 30, 0),
        type=TransactionType.deposit, asset_to='BTC', amount_to=1.0,
        is_active=True
    )
    db.add(w); db.add(d)
    
    await db.commit()
    with patch('app.services.boi.boi_service.prefetch_rates', AsyncMock()), \
         patch('app.services.price.price_service.prefetch_prices', AsyncMock()), \
         patch('app.services.boi.boi_service.get_usd_ils_rate', return_value=1.0), \
         patch('app.services.cpi.cpi_service.get_cpi_index', return_value=100.0), \
         patch('app.services.price.price_service.get_historical_price', return_value=30000.0):
        await engine_inst.calculate_taxes(db)
    
    # The withdrawal should have a fee_amount of 0.01
    await db.refresh(w)
    assert w.fee_amount == 0.01
    assert w.fee_asset == 'BTC'
    
    assert w.is_taxable_event == 1
    assert d.is_taxable_event == 0
