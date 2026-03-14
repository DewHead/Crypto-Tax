from sqlalchemy import Column, Float, Date, String, Integer
from app.db.session import Base

class DailyValuation(Base):
    __tablename__ = "daily_valuations"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True, nullable=False)
    exchange = Column(String, index=True, nullable=False)
    ils_value = Column(Float, nullable=False)
