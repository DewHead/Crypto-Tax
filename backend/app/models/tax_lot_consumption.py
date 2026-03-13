from sqlalchemy import Column, Integer, Float, ForeignKey
from app.db.session import Base

class TaxLotConsumption(Base):
    __tablename__ = "tax_lot_consumptions"

    id = Column(Integer, primary_key=True, index=True)
    sell_tx_id = Column(Integer, ForeignKey('transactions.id'), nullable=False)
    buy_tx_id = Column(Integer, ForeignKey('transactions.id'), nullable=False)
    amount_consumed = Column(Float, nullable=False)
    ils_value_consumed = Column(Float, nullable=False)
