from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from app.db.session import Base
from app.models.historical_price import HistoricalPrice
from app.models.transaction import Transaction
from app.models.exchange_key import ExchangeKey
from app.models.ils_rate import ILSRate
from app.models.cpi_rate import CPIRate
from app.models.tax_lot_consumption import TaxLotConsumption
from app.models.daily_valuation import DailyValuation
from app.models.app_setting import AppSetting
...
