from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, extract, distinct, delete
from app.models.transaction import Transaction, TransactionType
from app.models.exchange_key import ExchangeKey as ExchangeKeyModel
from app.models.tax_lot_consumption import TaxLotConsumption
from app.services.boi import boi_service
from app.services.cpi import cpi_service
from app.services.price import price_service
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Set
import asyncio
import logging

logger = logging.getLogger(__name__)
...
    async def calculate_taxes(self, db: AsyncSession, use_wash_sale_rule: bool = False):
        # 0. Clean up previous calculations
        await db.execute(delete(TaxLotConsumption))
        
        # 1. Fetch all transactions
        txs_stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(txs_stmt)
        all_txs = result.scalars().all()
        
        if not all_txs:
            return

        # Phase 1: Pre-Processing & Classification
        await self._run_avalanche_merger(all_txs, db)
        reconciled_ids = await self._run_transfer_reconciliation(all_txs, db)
        
        # Filter active transactions for Phase 2 and 3
        active_txs = [t for t in all_txs if t.is_active]

        # Phase 2: Valuation (Pricing everything to ILS)
        logger.info(f"--- Phase 2: Valuation ({len(active_txs)} active transactions) ---")
        await self._run_valuation(active_txs, db)
        
        # Phase 3: The FIFO Ledger
        logger.info(f"--- Phase 3: FIFO Ledger Calculation ({len(active_txs)} transactions) ---")
        ledger = TaxLedger(db)
        for tx in active_txs:
            await self._process_transaction(tx, ledger, reconciled_ids, db, use_wash_sale_rule=use_wash_sale_rule)

        await db.commit()
...
    async def _run_valuation(self, txs: List[Transaction], db: AsyncSession):
        if not txs: return

        min_date = get_jerusalem_date(txs[0].timestamp)
        max_date = get_jerusalem_date(txs[-1].timestamp)
        await boi_service.prefetch_rates(min_date, max_date, db=db)

        # Prefetch crypto prices
        price_tasks = []
        for tx in txs:
            tx_date = get_jerusalem_date(tx.timestamp)
            if tx.asset_from: price_tasks.append((tx.asset_from, tx_date))
            if tx.asset_to: price_tasks.append((tx.asset_to, tx_date))
            if tx.fee_asset: price_tasks.append((tx.fee_asset, tx_date))
        
        if price_tasks:
            logger.info(f"\u26a1 Prefetching {len(price_tasks)} crypto prices in parallel...")
            await price_service.prefetch_prices(price_tasks, db=db)
            logger.info(f"\u2705 Crypto price prefetching complete.")

        rate_cache: Dict[date, float] = {}
        for tx in txs:
            rate_date = get_jerusalem_date(tx.timestamp)
            if rate_date not in rate_cache:
                rate_cache[rate_date] = await boi_service.get_usd_ils_rate(rate_date, db=db)

            tx.ils_exchange_rate = rate_cache[rate_date]
            tx.ils_rate_date = rate_date

    async def get_ils_value(self, asset: str, amount: float, tx_date: date, usd_ils_rate: float, db: Optional[AsyncSession] = None) -> float:
        if not asset or amount == 0:
            return 0.0
        if asset == 'ILS':
            return amount
            
        rate = usd_ils_rate or 3.65
        
        if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
            return amount * rate
        
        usd_price = await price_service.get_historical_price(asset, tx_date, db=db)
        if usd_price is not None:
            val = amount * usd_price * rate
            logger.info(f"Price: {asset} on {tx_date} is ${usd_price}, ILS value: {val}")
            return val
        
        logger.warning(f"MISSING PRICE: {asset} on {tx_date}")
        return 0.0
...
        # Pre-calculate values
        amt_from = tx.amount_from or 0.0
        amt_to = tx.amount_to or 0.0
        val_from = await self.get_ils_value(tx.asset_from, amt_from, tx_date, rate, db=db)
        val_to = await self.get_ils_value(tx.asset_to, amt_to, tx_date, rate, db=db)
        fee_ils = await self.get_ils_value(tx.fee_asset, tx.fee_amount or 0.0, tx_date, rate, db=db)
...
