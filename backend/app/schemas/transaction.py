from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List

class TransactionBase(BaseModel):
    exchange: str
    tx_hash: str
    timestamp: datetime
    type: str
    asset_from: Optional[str]
    amount_from: Optional[float]
    asset_to: Optional[str]
    amount_to: Optional[float]
    fee_amount: Optional[float]
    fee_asset: Optional[str]
    source: str
    category: Optional[str]
    is_issue: bool = False
    issue_notes: Optional[str] = None

class TransactionCreate(TransactionBase):
    pass

class Transaction(TransactionBase):
    id: int
    ils_exchange_rate: Optional[float]
    ils_rate_date: Optional[date]
    cost_basis_ils: Optional[float]
    purchase_date: Optional[date]
    capital_gain_ils: Optional[float]
    inflationary_gain_ils: Optional[float] = 0.0
    real_gain_ils: Optional[float] = 0.0
    ordinary_income_ils: Optional[float] = 0.0
    is_taxable_event: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True

class KPIReport(BaseModel):
    year: Optional[int] = None
    total_nominal_gain_ils: float
    ordinary_income_ils: float
    net_capital_gain_ils: float
    inflationary_gain_ils: float
    capital_losses_ils: float
    carried_forward_loss_ils: float
    tax_bracket: float
    estimated_tax_ils: float
    trade_count: int
    total_transactions: int
    high_frequency_warning: bool
    issue_count: int = 0
