from pydantic import BaseModel, field_validator
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

    @field_validator('amount_from', 'amount_to', 'fee_amount', mode='before')
    @classmethod
    def set_default_zero(cls, v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

class TransactionCreate(TransactionBase):
    pass

class Transaction(TransactionBase):
    id: int
    ils_rate_date: Optional[date]
    ils_exchange_rate: Optional[float]
    cost_basis_ils: Optional[float]
    purchase_date: Optional[date]
    capital_gain_ils: Optional[float]
    inflationary_gain_ils: Optional[float] = 0.0
    real_gain_ils: Optional[float] = 0.0
    ordinary_income_ils: Optional[float] = 0.0
    is_taxable_event: int = 0
    is_active: bool = True
    parent_tx_id: Optional[int] = None

    @field_validator('ils_exchange_rate', 'cost_basis_ils', 'capital_gain_ils', 'ordinary_income_ils', 'is_taxable_event', mode='before')
    @classmethod
    def set_default_zero_extra(cls, v):
        return v if v is not None else 0

    class Config:
        from_attributes = True

class KPIReport(BaseModel):
    year: Optional[int] = None
    total_nominal_gain_ils: float
    ordinary_income_ils: float
    net_capital_gain_ils: float
    carried_forward_loss_ils: float
    tax_bracket: float
    estimated_tax_ils: float
    trade_count: int
    total_transactions: int
    high_frequency_warning: bool
