from sqlalchemy import Column, String, Float, Date
from app.db.session import Base

class HistoricalPrice(Base):
    __tablename__ = "historical_prices"

    asset = Column(String, primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    price_usd = Column(Float, nullable=False)
