from pydantic import BaseModel, field_validator
from datetime import datetime, date
from typing import Optional, List
from app.models.transaction import TransactionType

class TransactionBase(BaseModel):
    exchange: str
    tx_hash: Optional[str] = None
    timestamp: datetime
    type: TransactionType
    asset_from: Optional[str] = None
    amount_from: Optional[float] = 0.0
    asset_to: Optional[str] = None
    amount_to: Optional[float] = 0.0
    fee_asset: Optional[str] = None
    fee_amount: Optional[float] = 0.0
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

class ManualCostBasisUpdate(BaseModel):
    manual_cost_basis_ils: float
    manual_purchase_date: Optional[date] = None

class Transaction(TransactionBase):
    id: int
    ils_rate_date: Optional[date] = None
    ils_exchange_rate: Optional[float] = 0.0
    cost_basis_ils: Optional[float] = 0.0
    purchase_date: Optional[date] = None
    manual_cost_basis_ils: Optional[float] = 0.0
    manual_purchase_date: Optional[date] = None
    capital_gain_ils: Optional[float] = 0.0
    inflationary_gain_ils: Optional[float] = 0.0
    real_gain_ils: Optional[float] = 0.0
    ordinary_income_ils: Optional[float] = 0.0
    is_taxable_event: int = 0
    is_active: bool = True
    parent_tx_id: Optional[int] = None

    @field_validator(
        'ils_exchange_rate', 'cost_basis_ils', 'capital_gain_ils', 
        'inflationary_gain_ils', 'real_gain_ils', 'ordinary_income_ils', 
        'is_taxable_event', 'manual_cost_basis_ils',
        mode='before'
    )
    @classmethod
    def set_default_zero_extra(cls, v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

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
    form_1391_breached: bool = False
    max_foreign_value_ils: float = 0.0
