from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime, timezone
from app.db.session import Base

class ExchangeKey(Base):
    __tablename__ = "exchange_keys"

    id = Column(Integer, primary_key=True, index=True)
    exchange_name = Column(String, index=True)
    api_key = Column(String, nullable=True)
    api_secret = Column(String, nullable=True)
    is_syncing = Column(Integer, default=0) # 0 for false, 1 for true
    last_sync_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
