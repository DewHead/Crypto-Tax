from sqlalchemy import Column, Float, Date
from app.db.session import Base

class ILSRate(Base):
    __tablename__ = "ils_rates"

    date = Column(Date, primary_key=True, index=True)
    rate = Column(Float, nullable=False)
