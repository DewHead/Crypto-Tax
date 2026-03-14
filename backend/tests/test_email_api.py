import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.db.session import Base, get_db
from app.models.app_setting import AppSetting
from main import app
from datetime import datetime

# Use a test database
TEST_DB_URL = "sqlite+aiosqlite:///./test_email_ledger.db"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture(scope="function")
async def db_session():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with TestSessionLocal() as session:
        yield session
    
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest_asyncio.fixture(scope="function")
async def client(db_session):
    async def override_get_db():
        yield db_session
    
    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_send_test_email_no_config(client: AsyncClient):
    """Test sending test email when not configured."""
    response = await client.post("/api/test-email")
    assert response.status_code == 400
    assert response.json()["detail"] == "Notification email not configured"

@pytest.mark.asyncio
async def test_send_test_email_success(client: AsyncClient, db_session):
    """Test sending test email when configured."""
    # Mock settings
    setting = AppSetting(key="notification_email", value="test@example.com")
    db_session.add(setting)
    await db_session.commit()
    
    # We need to ensure the get_notification_email inside the endpoint uses the same DB
    # Actually get_notification_email uses AsyncSessionLocal() directly, which is problematic for testing
    # Let's check how get_notification_email is implemented.
    
    response = await client.post("/api/test-email")
    assert response.status_code == 200
    assert response.json()["status"] == "email_sent"
    assert response.json()["recipient"] == "test@example.com"
