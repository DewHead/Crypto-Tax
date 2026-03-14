import pytest
import pytest_asyncio
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.transaction import Transaction, TransactionType
from app.services.tax_engine import TaxEngine

@pytest.mark.asyncio
async def test_missing_cost_basis_ita_default(db: AsyncSession):
    """ITA default for missing cost basis is ZERO."""
    await db.execute(delete(Transaction))
    
    # Sell 1 BTC for 50000 ILS with NO purchase history
    db.add(Transaction(
        exchange="test", timestamp=pytest.importorskip("datetime").datetime(2025, 6, 1),
        type=TransactionType.sell, asset_from="BTC", amount_from=1.0,
        asset_to="ILS", amount_to=50000.0, is_active=True
    ))
    await db.commit()
    
    engine = TaxEngine()
    await engine.calculate_taxes(db)
    
    stmt = select(Transaction)
    res = await db.execute(stmt)
    tx = res.scalars().first()
    
    assert tx.is_issue == True
    assert tx.cost_basis_ils == 0.0
    assert tx.capital_gain_ils == 50000.0
