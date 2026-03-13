from sqlalchemy import Column, Integer, String, Float, DateTime, Enum, Index, Date, Boolean, ForeignKey
import enum
from app.db.session import Base

class TransactionType(str, enum.Enum):
    buy = "buy"
    sell = "sell"
    deposit = "deposit"
    withdrawal = "withdrawal"
    convert = "convert"
    dust = "dust"
    earn = "earn"
    fee = "fee"

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String, index=True)
    tx_hash = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    type = Column(Enum(TransactionType))
    asset_from = Column(String)
    amount_from = Column(Float)
    asset_to = Column(String)
    amount_to = Column(Float)
    fee_asset = Column(String, nullable=True)
    fee_amount = Column(Float, default=0.0)
    source = Column(String, index=True, default="api") # 'api' or 'csv'
    raw_data = Column(String, nullable=True) # JSON or string representation for debugging
    
    # ILS calculation fields
    ils_rate_date = Column(Date, nullable=True)
    ils_exchange_rate = Column(Float, nullable=True)
    
    # Tax calculation results
    cost_basis_ils = Column(Float, nullable=True)
    purchase_date = Column(Date, nullable=True)
    capital_gain_ils = Column(Float, nullable=True)
    ordinary_income_ils = Column(Float, nullable=True, default=0.0)
    is_taxable_event = Column(Integer, default=0) # 0 for false, 1 for true

    # Pipeline flags
    is_active = Column(Boolean, default=True)
    parent_tx_id = Column(Integer, ForeignKey('transactions.id'), nullable=True)

    # Coinly Parity fields
    is_issue = Column(Boolean, default=False)
    issue_notes = Column(String, nullable=True)
    category = Column(String, nullable=True)
    linked_transaction_id = Column(Integer, nullable=True)

Index("ix_transaction_timestamp_tx_hash", Transaction.timestamp, Transaction.tx_hash)
