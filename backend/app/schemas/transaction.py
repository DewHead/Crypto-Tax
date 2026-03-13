from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List
from app.models.transaction import TransactionType

class TransactionBase(BaseModel):
    exchange: str
    tx_hash: Optional[str]
    timestamp: datetime
    type: TransactionType
    asset_from: Optional[str] = None
    amount_from: Optional[float] = 0.0
    asset_to: Optional[str] = None
    amount_to: Optional[float] = 0.0
    fee_asset: Optional[str] = None
    fee_amount: float = 0.0
    source: str = "api"
    raw_data: Optional[str] = None
    is_issue: bool = False
    issue_notes: Optional[str] = None
    category: Optional[str] = None
    linked_transaction_id: Optional[int] = None

class TransactionCreate(TransactionBase):
    pass

class Transaction(TransactionBase):
    id: int
    ils_rate_date: Optional[date]
    ils_exchange_rate: Optional[float]
    cost_basis_ils: Optional[float]
    purchase_date: Optional[date]
    capital_gain_ils: Optional[float]
    ordinary_income_ils: Optional[float] = 0.0
    is_taxable_event: int

    class Config:
        from_attributes = True

class KPIReport(BaseModel):
    year: Optional[int] = None
    total_gain_ils: float
    ordinary_income_ils: float
    net_capital_gain_ils: float
    carried_forward_loss_ils: float
    tax_bracket: float
    estimated_tax_ils: float
    trade_count: int
    total_transactions: int
    is_business_threshold_crossed: bool
