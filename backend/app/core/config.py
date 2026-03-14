from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./ledger.db"
    
    BINANCE_API_KEY: Optional[str] = None
    BINANCE_API_SECRET: Optional[str] = None
    
    KRAKEN_API_KEY: Optional[str] = None
    KRAKEN_API_SECRET: Optional[str] = None
    
    KRAKENFUTURES_API_KEY: Optional[str] = None
    KRAKENFUTURES_API_SECRET: Optional[str] = None
    
    BOI_API_URL: str = "https://boi.org.il/PublicApi/GetExchangeRates"
    
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SMTP_FROM_EMAIL: Optional[str] = "notifications@cryptotax.local"
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
