from sqlalchemy import Column, Integer, Date, Float
from app.db.session import Base

class CPIRate(Base):
    __tablename__ = "cpi_rates"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, unique=True, index=True, nullable=False) # Usually the 15th of each month
    index_value = Column(Float, nullable=False)
