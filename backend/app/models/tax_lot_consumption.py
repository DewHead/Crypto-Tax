from sqlalchemy import Column, Integer, Float, ForeignKey
from app.db.session import Base

class TaxLotConsumption(Base):
    __tablename__ = "tax_lot_consumptions"

    id = Column(Integer, primary_key=True, index=True)
    sell_tx_id = Column(Integer, ForeignKey('transactions.id'), nullable=False)
    buy_tx_id = Column(Integer, ForeignKey('transactions.id'), nullable=False)
    amount_consumed = Column(Float, nullable=False)
    ils_value_consumed = Column(Float, nullable=False) # Original cost basis in ILS
    
    # Israeli Tax Rules (Inflationary Adjustment)
    adjusted_cost_basis_ils = Column(Float, nullable=True) # Cost basis adjusted by CPI
    inflationary_gain_ils = Column(Float, nullable=True) # Exempt portion
    real_gain_ils = Column(Float, nullable=True) # Taxable portion (25%)
