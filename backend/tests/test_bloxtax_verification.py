import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.db.session import Base
from app.services.tax_engine import TaxEngine
from app.services.csv_ingestion import csv_ingestion_service
from app.models.transaction import Transaction
import os

import pytest_asyncio
import logging
logging.basicConfig(level=logging.INFO)

# Use a separate test database
TEST_SQLALCHEMY_DATABASE_URL = \"sqlite+aiosqlite:///./test_bloxtax.db\"
engine = create_async_engine(TEST_SQLALCHEMY_DATABASE_URL, connect_args={\"check_same_thread\": False})
TestingSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope=\"module\")
async def db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with TestingSessionLocal() as session:
        yield session
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    if os.path.exists(\"./test_bloxtax.db\"):
        os.remove(\"./test_bloxtax.db\")

@pytest.mark.asyncio(loop_scope=\"module\")
async def test_bloxtax_reconciliation(db: AsyncSession):
    # 1. Load the CSV data
    csv_path = \"../temp_csv/95ea866c-1c69-11f1-8ec7-0688bfc90b95-1.csv\"
    if not os.path.exists(csv_path):
        pytest.fail(f\"CSV file not found at {csv_path}\")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    await csv_ingestion_service._process_binance_statements(lines, db=db)
    
    # 2. Run Tax Calculation
    await TaxEngine().calculate_taxes(db)

    # 3. Assert 2017 KPI
    kpi_2017 = await TaxEngine().get_kpi(db, year=2017)
    kpi_2018 = await TaxEngine().get_kpi(db, year=2018)
    
    print(f\"\\n2017 KPI: {kpi_2017}\")
    print(f\"2018 KPI: {kpi_2018}\")
    
    # 2017 Target: ₪255.27
    assert abs(kpi_2017['net_capital_gain_ils'] - 255.27) < 50.0
    # 2018 Target: ₪371.28
    assert abs(kpi_2018['net_capital_gain_ils'] - 371.28) < 100.0
