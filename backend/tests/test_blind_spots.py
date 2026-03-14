import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine, get_jerusalem_date
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_get_jerusalem_date():
    # Dec 31, 2025 22:30 UTC is Jan 1, 2026 00:30 IST
    ts = datetime(2025, 12, 31, 22, 30, tzinfo=timezone.utc)
    jd = get_jerusalem_date(ts)
    assert jd.year == 2026
    assert jd.day == 1
    assert jd.month == 1

@pytest.mark.asyncio
async def test_fee_valuation_fallback():
    engine = TaxEngine()
    db = AsyncMock(spec=AsyncSession)
    
    # Mock price service to return None for an obscure token
    with patch("app.services.tax_engine.price_service.get_historical_price", return_value=None):
        # Case 1: Fee in asset_from
        tx = Transaction(
            id=1,
            timestamp=datetime(2025, 1, 1),
            type=TransactionType.sell,
            asset_from="OBSCURE",
            amount_from=100.0,
            asset_to="USD",
            amount_to=500.0,
            fee_asset="OBSCURE",
            fee_amount=1.0,
            ils_exchange_rate=3.5
        )
        
        async def mock_get_ils_value(asset, amount, tx_date, rate):
            if asset == "OBSCURE": return 0.0
            if asset == "USD": return amount * rate
            return 0.0
            
        with patch.object(TaxEngine, "get_ils_value", side_effect=mock_get_ils_value):
            ledger = AsyncMock()
            ledger.consume_lots = AsyncMock(return_value=(0.0, []))
            reconciled_ids = set()
            
            await engine._process_transaction(tx, ledger, reconciled_ids, db)
            assert tx.capital_gain_ils == 1750.0

@pytest.mark.asyncio
async def test_avalanche_merger_dust_convert():
    engine = TaxEngine()
    db = AsyncMock(spec=AsyncSession)
    
    ts = datetime(2025, 1, 1, 12, 0, 0)
    tx1 = Transaction(id=1, timestamp=ts, type=TransactionType.dust, exchange="binance", asset_from="D1", amount_from=10.0, asset_to="BNB", amount_to=0.1, is_active=True)
    tx2 = Transaction(id=2, timestamp=ts, type=TransactionType.dust, exchange="binance", asset_from="D1", amount_from=10.0, asset_to="BNB", amount_to=0.1, is_active=True)
    
    txs = [tx1, tx2]
    await engine._run_avalanche_merger(txs, db)
    
    assert tx1.amount_from == 20.0
    assert tx1.amount_to == 0.2
    assert tx2.is_active is False
