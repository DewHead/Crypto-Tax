from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, func
from app.models.transaction import Transaction
from app.models.exchange_key import ExchangeKey
from app.models.daily_valuation import DailyValuation
from app.services.tax_engine import get_jerusalem_date
from app.services.boi import boi_service
from app.services.price import price_service
from datetime import date, timedelta
from typing import Dict, List, Set
import logging

logger = logging.getLogger(__name__)

class ValuationService:
    async def update_daily_valuations(self, db: AsyncSession):
        """
        Recalculates the daily foreign inventory value and persists it to the DailyValuation table.
        This is a background task to avoid dashboard timeouts.
        """
        logger.info("Starting daily valuation update...")
        
        # 1. Identify foreign exchanges
        foreign_exchanges = set()
        keys_stmt = select(ExchangeKey)
        keys_result = await db.execute(keys_stmt)
        for k in keys_result.scalars().all():
            if k.exchange_name.lower() not in ['bitsofgold', 'altshuler']:
                foreign_exchanges.add(k.exchange_name.lower())
        
        if not foreign_exchanges:
            logger.info("No foreign exchanges found. Skipping valuation update.")
            return

        # 2. Fetch all active transactions
        stmt = select(Transaction).where(Transaction.is_active == True).order_by(Transaction.timestamp.asc())
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
        if not all_txs:
            return

        # 3. Build a daily event map
        txs_by_date: Dict[date, List[Transaction]] = {}
        for tx in all_txs:
            d = get_jerusalem_date(tx.timestamp)
            txs_by_date.setdefault(d, []).append(tx)

        all_sorted_dates = sorted(txs_by_date.keys())
        first_date = all_sorted_dates[0]
        end_date = date.today()

        # 4. Clear existing valuations to avoid partial updates
        await db.execute(delete(DailyValuation))
        
        # 5. Daily Loop
        inventory_by_exchange: Dict[str, Dict[str, float]] = {}
        curr = first_date
        
        while curr <= end_date:
            # Update inventory with today's transactions
            if curr in txs_by_date:
                for tx in txs_by_date[curr]:
                    ex = tx.exchange.lower()
                    if ex not in inventory_by_exchange: inventory_by_exchange[ex] = {}
                    if tx.asset_from: inventory_by_exchange[ex][tx.asset_from] = inventory_by_exchange[ex].get(tx.asset_from, 0.0) - (tx.amount_from or 0.0)
                    if tx.asset_to: inventory_by_exchange[ex][tx.asset_to] = inventory_by_exchange[ex].get(tx.asset_to, 0.0) + (tx.amount_to or 0.0)
                    if tx.fee_asset: inventory_by_exchange[ex][tx.fee_asset] = inventory_by_exchange[ex].get(tx.fee_asset, 0.0) - (tx.fee_amount or 0.0)
            
            # Record valuation for each foreign exchange
            usd_ils_rate = None
            
            for ex in foreign_exchanges:
                if ex in inventory_by_exchange:
                    # Filter out tiny dust to speed up pricing
                    assets_to_price = {a: q for a, q in inventory_by_exchange[ex].items() if abs(q) > 1e-8}
                    if not assets_to_price:
                        continue
                    
                    if usd_ils_rate is None:
                        usd_ils_rate = await boi_service.get_usd_ils_rate(curr, db=db)
                    
                    daily_val_ils = 0.0
                    for asset, qty in assets_to_price.items():
                        if asset not in ['USD', 'ILS']:
                            usd_price = await price_service.get_historical_price(asset, curr)
                            if usd_price:
                                daily_val_ils += qty * usd_price * usd_ils_rate
                        elif asset == 'USD':
                            daily_val_ils += qty * usd_ils_rate
                        elif asset == 'ILS':
                            daily_val_ils += qty
                    
                    if daily_val_ils > 0:
                        db.add(DailyValuation(
                            date=curr,
                            exchange=ex,
                            ils_value=daily_val_ils
                        ))
            
            # Commit in chunks to avoid massive memory usage
            if curr.day == 1:
                await db.commit()
                
            curr += timedelta(days=1)
        
        await db.commit()
        logger.info("Daily valuation update completed.")

valuation_service = ValuationService()
