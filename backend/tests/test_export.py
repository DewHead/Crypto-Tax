import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.models.transaction import Transaction, TransactionType
from app.models.tax_lot_consumption import TaxLotConsumption
from app.db.session import Base
from app.services.export import export_service
from datetime import datetime
import csv
import io

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"
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
async def test_export_localization(db):
    """
    Verifies that export logic correctly localizes UTC timestamps to Asia/Jerusalem
    before filtering by year and formatting the dates.
    """
    # 1. Create a Buy Transaction (Jan 2025)
    buy_tx = Transaction(
        id=1,
        exchange='test',
        tx_hash='buy_1',
        timestamp=datetime(2025, 1, 1, 12, 0),
        type=TransactionType.buy,
        asset_to='BTC',
        amount_to=1.0,
        asset_from='USD',
        amount_from=10000.0,
        fee_amount=0.0
    )
    db.add(buy_tx)
    
    # 2. Create a Sell Transaction (Dec 31st 23:30 UTC -> Jan 1st 01:30 Israel)
    sell_tx = Transaction(
        id=2,
        exchange='test',
        tx_hash='sell_1',
        timestamp=datetime(2025, 12, 31, 23, 30),
        type=TransactionType.sell,
        asset_from='BTC',
        amount_from=1.0,
        asset_to='USD',
        amount_to=50000.0,
        fee_amount=0.0
    )
    db.add(sell_tx)
    await db.commit()
    
    # 3. Manually create TaxLotConsumption
    consumption = TaxLotConsumption(
        buy_tx_id=buy_tx.id,
        sell_tx_id=sell_tx.id,
        amount_consumed=1.0,
        ils_value_consumed=36000.0,
        adjusted_cost_basis_ils=36500.0,
        real_gain_ils=143500.0,
        inflationary_gain_ils=500.0
    )
    db.add(consumption)
    await db.commit()
    
    # 4. Export for 2025 - should have 0 data rows (because Dec 31 23:30 UTC is Jan 1st 2026 Israel)
    csv_2025 = await export_service.generate_form_8659_csv(db, year=2025)
    # Separate data from summary
    lines = csv_2025.strip().split('\n')
    data_lines = []
    for line in lines[1:]: // Skip header
        line = line.strip()
        if "--- FORM 1301" in line or not line: break
        data_lines.append(line)
    assert len(data_lines) == 0, "Trade should NOT be in 2025 CSV"

    // 5. Export for 2026 - should have 1 data row
    csv_2026 = await export_service.generate_form_8659_csv(db, year=2026)
    lines_2026 = csv_2026.strip().split('\n')
    data_lines_2026 = []
    for line in lines_2026[1:]:
        line = line.strip()
        if not line: continue
        if "--- FORM 1301" in line: break
        data_lines_2026.append(line)

    assert len(data_lines_2026) == 1, "Trade SHOULD be in 2026 CSV"
    assert "01/01/2026" in data_lines_2026[0]
    assert "01/01/2025" in data_lines_2026[0] // Purchase Date
