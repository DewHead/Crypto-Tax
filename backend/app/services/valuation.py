from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, func
from app.models.transaction import Transaction
from app.models.exchange_key import ExchangeKey
from app.models.daily_valuation import DailyValuation
from app.services.tax_engine import get_jerusalem_date, is_fiat
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

        # 3. Build a daily event map and identify all assets/dates needed
        txs_by_date: Dict[date, List[Transaction]] = {}
        all_asset_date_pairs = []
        unique_dates = set()

        for tx in all_txs:
            d = get_jerusalem_date(tx.timestamp)
            txs_by_date.setdefault(d, []).append(tx)
            unique_dates.add(d)

        all_sorted_dates = sorted(txs_by_date.keys())
        first_date = all_sorted_dates[0]
        end_date = date.today()
        
        # Fill in missing dates for valuation
        curr_fill = first_date
        while curr_fill <= end_date:
            unique_dates.add(curr_fill)
            curr_fill += timedelta(days=1)
        
        all_valuation_dates = sorted(list(unique_dates))

        # Identify assets to price per date (roughly)
        # This is hard because inventory changes daily, but we can prefetch common ones
        active_assets = set()
        for tx in all_txs:
            if tx.asset_from: active_assets.add(tx.asset_from)
            if tx.asset_to: active_assets.add(tx.asset_to)
            if tx.fee_asset: active_assets.add(tx.fee_asset)

        # Prefetch BOI rates for the whole range
        await boi_service.prefetch_rates(first_date, end_date, db=db)

        # 4. Clear existing valuations
        await db.execute(delete(DailyValuation))
        
        # 5. Daily Loop
        inventory_by_exchange: Dict[str, Dict[str, float]] = {}
        curr_idx = 0
        
        new_valuations = []

        for curr in all_valuation_dates:
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
                        if not is_fiat(asset):
                            usd_price = await price_service.get_historical_price(asset, curr, db=db)
                            if usd_price:
                                daily_val_ils += qty * usd_price * usd_ils_rate
                        elif asset.upper() == 'USD':
                            daily_val_ils += qty * usd_ils_rate
                        elif asset.upper() == 'ILS':
                            daily_val_ils += qty
                        else:
                            usd_price = await price_service.get_historical_price(asset, curr, db=db)
                            if usd_price:
                                daily_val_ils += qty * usd_price * usd_ils_rate
                    
                    if daily_val_ils > 0:
                        new_valuations.append({
                            'date': curr,
                            'exchange': ex,
                            'ils_value': daily_val_ils
                        })
            
            # Commit in chunks to avoid massive memory usage and blockages
            if len(new_valuations) >= 500:
                await db.execute(insert(DailyValuation).values(new_valuations))
                await db.commit()
                new_valuations.clear()
        
        if new_valuations:
            await db.execute(insert(DailyValuation).values(new_valuations))
            await db.commit()
            
        logger.info("Daily valuation update completed.")

valuation_service = ValuationService()
