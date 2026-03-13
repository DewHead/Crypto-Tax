from pydantic import BaseModel
from datetime import datetime

class ExchangeKeyBase(BaseModel):
    exchange_name: str
    api_key: str | None = None

class ExchangeKeyCreate(ExchangeKeyBase):
    api_secret: str | None = None

class ExchangeKeyResponse(BaseModel):
    id: int
    exchange_name: str
    api_key: str | None = None
    is_syncing: int
    last_sync_at: datetime | None
    created_at: datetime
    # Note: api_secret is explicitly omitted for security

    class Config:
        from_attributes = True
