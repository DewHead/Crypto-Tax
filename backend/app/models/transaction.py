from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Date, Boolean
from sqlalchemy.orm import relationship
from app.db.session import Base
import enum

class TransactionType(str, enum.Enum):
    buy = "buy"
    sell = "sell"
    deposit = "deposit"
    withdrawal = "withdrawal"
    convert = "convert"
    dust = "dust"
    earn = "earn"
    fee = "fee"
    airdrop = "airdrop"
    fork = "fork"

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String)
    tx_hash = Column(String, unique=True, index=True)
    timestamp = Column(DateTime, index=True)
    type = Column(String) # TransactionType
    asset_from = Column(String, nullable=True)
    amount_from = Column(Float, nullable=True)
    asset_to = Column(String, nullable=True)
    amount_to = Column(Float, nullable=True)
    fee_amount = Column(Float, nullable=True)
    fee_asset = Column(String, nullable=True)
    
    source = Column(String, default='api') # 'api' or 'csv'
    category = Column(String, nullable=True) # e.g. 'Trade', 'Transfer', 'Staking'
    
    # Metadata
    is_active = Column(Boolean, default=True)
    parent_tx_id = Column(Integer, nullable=True)
    linked_transaction_id = Column(Integer, ForeignKey('transactions.id'), nullable=True)
    issue_notes = Column(String, nullable=True)
    is_issue = Column(Boolean, default=False)

    # Tax calculation results
    ils_exchange_rate = Column(Float, nullable=True)
    ils_rate_date = Column(Date, nullable=True)
    cost_basis_ils = Column(Float, nullable=True)
    purchase_date = Column(Date, nullable=True)
    capital_gain_ils = Column(Float, nullable=True)
    inflationary_gain_ils = Column(Float, nullable=True) # Exempt portion (Israeli Rules)
    real_gain_ils = Column(Float, nullable=True) # Taxable portion (Israeli Rules)
    ordinary_income_ils = Column(Float, nullable=True, default=0.0)
    is_taxable_event = Column(Integer, default=0) # 0 for false, 1 for true

    linked_tx = relationship("Transaction", remote_side=[id])
