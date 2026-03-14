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

def get_jerusalem_date(ts: datetime) -> date:
    """Localize UTC timestamp to Asia/Jerusalem before extracting the date."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ZoneInfo("Asia/Jerusalem")).date()

class TaxLedger:
    def __init__(self, db: AsyncSession, dry_run: bool = False):
        self.db = db
        self.dry_run = dry_run
        self.inventory: Dict[str, List[Dict[str, Any]]] = {}
        self.recent_losses: Dict[str, List[Dict[str, Any]]] = {} # asset -> list of {'timestamp', 'loss_ils', 'amount'}

    async def consume_lots(self, asset: str, qty: float, sell_tx: Transaction) -> tuple[float, List[TaxLotConsumption]]:
        logger.info(f"Consuming {qty} {asset} for TX {sell_tx.id} ({sell_tx.timestamp})")
        
        # 1. Handle Manual Cost Basis Override
        if sell_tx.manual_cost_basis_ils is not None:
            # We MUST still consume from inventory to keep balances correct
            qty_to_match = qty
            while qty_to_match > 1e-10 and asset in self.inventory and self.inventory[asset]:
                oldest_buy = self.inventory[asset][0]
                if oldest_buy['amount'] <= qty_to_match:
                    qty_to_match -= oldest_buy['amount']
                    self.inventory[asset].pop(0)
                else:
                    oldest_buy['amount'] -= qty_to_match
                    oldest_buy['cost_basis_ils'] -= (oldest_buy['cost_basis_ils'] / (oldest_buy['amount'] + qty_to_match)) * qty_to_match
                    qty_to_match = 0
            
            # CPI Adjustment for manual entry
            cpi_sell = await cpi_service.get_cpi_index(get_jerusalem_date(sell_tx.timestamp), self.db)
            original_cost = sell_tx.manual_cost_basis_ils
            adjusted_cost = original_cost
            
            if sell_tx.manual_purchase_date:
                cpi_buy = await cpi_service.get_cpi_index(sell_tx.manual_purchase_date, self.db)
                cpi_ratio = max(1.0, cpi_sell / cpi_buy)
                adjusted_cost = original_cost * cpi_ratio
            
            # Record a manual consumption record for the audit trail
            consumption = TaxLotConsumption(
                sell_tx_id=sell_tx.id,
                buy_tx_id=None, # Manual override has no linked buy TX
                amount_consumed=qty,
                ils_value_consumed=original_cost,
                adjusted_cost_basis_ils=adjusted_cost
            )
            if not self.dry_run:
                self.db.add(consumption)
            
            # Reset issue status if it was previously flagged
            if not self.dry_run:
                sell_tx.is_issue = False
            
            return original_cost, [consumption]

        if asset not in self.inventory or not self.inventory[asset]:
            # Missing cost basis - ITA rule: Default to ZERO
            if not self.dry_run:
                sell_tx.is_issue = True
                sell_tx.issue_notes = (sell_tx.issue_notes or "") + f" | Missing cost basis for {asset}. ITA default assumes ZERO."
            return 0.0, []

        total_cost_basis_ils = 0.0
        qty_to_match = qty
        consumptions = []
        
        cpi_sell = await cpi_service.get_cpi_index(get_jerusalem_date(sell_tx.timestamp), self.db)
        
        while qty_to_match > 1e-10 and self.inventory[asset]:
            oldest_buy = self.inventory[asset][0]
            buy_tx_id = oldest_buy['tx_id']
            buy_timestamp = oldest_buy['timestamp']
            
            if oldest_buy['amount'] <= qty_to_match:
                matched_qty = oldest_buy['amount']
                matched_cost = oldest_buy['cost_basis_ils']
                self.inventory[asset].pop(0)
            else:
                matched_qty = qty_to_match
                unit_cost = oldest_buy['cost_basis_ils'] / oldest_buy['amount']
                matched_cost = unit_cost * matched_qty
                oldest_buy['amount'] -= matched_qty
                oldest_buy['cost_basis_ils'] -= matched_cost

            # CPI Adjustment (Madad)
            cpi_buy = await cpi_service.get_cpi_index(get_jerusalem_date(buy_timestamp), self.db)
            cpi_ratio = max(1.0, cpi_sell / cpi_buy)
            
            adjusted_cost_basis_ils = matched_cost * cpi_ratio
            total_cost_basis_ils += matched_cost

            # Record consumption for audit trail
            consumption = TaxLotConsumption(
                sell_tx_id=sell_tx.id,
                buy_tx_id=buy_tx_id,
                amount_consumed=matched_qty,
                ils_value_consumed=matched_cost,
                adjusted_cost_basis_ils=adjusted_cost_basis_ils
            )
            if not self.dry_run:
                self.db.add(consumption)
            consumptions.append(consumption)

            qty_to_match -= matched_qty

        return total_cost_basis_ils, consumptions

    async def add_lot(self, asset: str, amount: float, cost_basis_ils: float, tx: Transaction):
        tx_ts = tx.timestamp
        if tx_ts.tzinfo is None:
            tx_ts = tx_ts.replace(tzinfo=timezone.utc)

        logger.info(f"Adding lot: {amount} {asset} at {cost_basis_ils} ILS from TX {tx.id} ({tx_ts})")
        if asset not in self.inventory:
            self.inventory[asset] = []
        
        # 30-Day Wash Sale Rule (Section 94B): Forward Part
        # If we have recent deferred losses, add them to cost basis
        deferred_loss = 0.0
        if asset in self.recent_losses:
            remaining_buy_qty = amount
            retained_losses = []
            
            for loss_entry in self.recent_losses[asset]:
                loss_ts = loss_entry['timestamp']
                if loss_ts.tzinfo is None:
                    loss_ts = loss_ts.replace(tzinfo=timezone.utc)

                time_diff = (tx_ts - loss_ts).total_seconds()
                # If loss was triggered within 30 days BEFORE this buy
                if 0 < time_diff <= 30 * 86400 and remaining_buy_qty > 0:
                    match_qty = min(remaining_buy_qty, loss_entry['amount'])
                    proportion = match_qty / loss_entry['amount']
                    matched_loss_ils = loss_entry['loss_ils'] * proportion
                    
                    deferred_loss += matched_loss_ils
                    remaining_buy_qty -= match_qty
                    
                    if match_qty < loss_entry['amount']:
                        loss_entry['amount'] -= match_qty
                        loss_entry['loss_ils'] -= matched_loss_ils
                        retained_losses.append(loss_entry)
                else:
                    retained_losses.append(loss_entry)
                    
            self.recent_losses[asset] = retained_losses

        self.inventory[asset].append({
            'tx_id': tx.id,
            'amount': amount,
            'cost_basis_ils': cost_basis_ils + deferred_loss,
            'timestamp': tx_ts
        })

    async def record_loss(self, asset: str, loss_ils: float, amount: float, tx: Transaction):
        tx_ts = tx.timestamp
        if tx_ts.tzinfo is None:
            tx_ts = tx_ts.replace(tzinfo=timezone.utc)

        # 30-Day Wash Sale Rule (Section 94B): Backward Part
        # If we bought replacement assets 30 days BEFORE this loss-triggering sale
        remaining_loss = loss_ils
        remaining_qty = amount
        
        if asset in self.inventory:
            # We look for lots in inventory acquired < 30 days before this sell
            for lot in reversed(self.inventory[asset]):
                if remaining_qty <= 0 or remaining_loss <= 0:
                    break
                    
                lot_ts = lot['timestamp']
                if lot_ts.tzinfo is None:
                    lot_ts = lot_ts.replace(tzinfo=timezone.utc)

                time_diff = (tx_ts - lot_ts).total_seconds()
                if 0 < time_diff <= 30 * 86400: # Bought within 30 days before sale
                    # This lot absorbs the loss
                    absor_qty = min(remaining_qty, lot['amount'])
                    absorb_proportion = absor_qty / amount
                    absorb_loss = loss_ils * absorb_proportion
                    
                    lot['cost_basis_ils'] += absorb_loss
                    remaining_loss -= absorb_loss
                    remaining_qty -= absor_qty
                    
                    logger.info(f"Backward Wash Sale: TX {tx.id} loss {absorb_loss} absorbed by lot from TX {lot['tx_id']}")

        if remaining_loss > 1e-4:
            if asset not in self.recent_losses:
                self.recent_losses[asset] = []
            self.recent_losses[asset].append({
                'timestamp': tx_ts,
                'loss_ils': remaining_loss,
                'amount': remaining_qty,
                'tx_id': tx.id
            })

class TaxEngine:
    def __init__(self):
        pass

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
        await self._run_valuation(active_txs, db)
        
        # Phase 3: The FIFO Ledger
        ledger = TaxLedger(db)
        for tx in active_txs:
            await self._process_transaction(tx, ledger, reconciled_ids, db, use_wash_sale_rule=use_wash_sale_rule)

        await db.commit()

    async def _run_avalanche_merger(self, txs: List[Transaction], db: AsyncSession):
        i = 0
        while i < len(txs):
            curr = txs[i]
            # ITA Rule: Dust conversions and swaps (convert) are taxable events that should be merged if simultaneous.
            if not curr.is_active or curr.type not in [TransactionType.buy, TransactionType.sell, TransactionType.convert, TransactionType.dust]:
                i += 1
                continue
            
            curr_ts = curr.timestamp
            if curr_ts.tzinfo is None:
                curr_ts = curr_ts.replace(tzinfo=timezone.utc)

            j = i + 1
            merged_count = 0
            while j < len(txs):
                nxt = txs[j]
                nxt_ts = nxt.timestamp
                if nxt_ts.tzinfo is None:
                    nxt_ts = nxt_ts.replace(tzinfo=timezone.utc)
                
                time_diff = (nxt_ts - curr_ts).total_seconds()
                if (nxt.is_active and
                    nxt.exchange == curr.exchange and 
                    nxt.type == curr.type and 
                    nxt.asset_from == curr.asset_from and 
                    nxt.asset_to == curr.asset_to and 
                    time_diff <= 5):
                    
                    curr.amount_from = (curr.amount_from or 0.0) + (nxt.amount_from or 0.0)
                    curr.amount_to = (curr.amount_to or 0.0) + (nxt.amount_to or 0.0)
                    curr.fee_amount = (curr.fee_amount or 0.0) + (nxt.fee_amount or 0.0)
                    
                    nxt.is_active = False
                    nxt.parent_tx_id = curr.id
                    merged_count += 1
                    j += 1
                else:
                    break
            
            if merged_count > 0:
                curr.raw_data = (curr.raw_data or "") + f" | Merged {merged_count + 1} trades."
            i = j

    async def _run_transfer_reconciliation(self, txs: List[Transaction], db: AsyncSession) -> Set[int]:
        withdrawals = [t for t in txs if t.is_active and t.type == TransactionType.withdrawal]
        deposits = [t for t in txs if t.is_active and t.type == TransactionType.deposit]
        reconciled_ids: Set[int] = set()

        w_by_asset = {}
        for w in withdrawals: w_by_asset.setdefault(w.asset_from, []).append(w)
        d_by_asset = {}
        for d in deposits: d_by_asset.setdefault(d.asset_to, []).append(d)

        for asset, asset_withdrawals in w_by_asset.items():
            asset_deposits = d_by_asset.get(asset, [])
            d_idx = 0
            d_len = len(asset_deposits)

            for w in asset_withdrawals:
                w_amt = w.amount_from or 0.0
                w_ts = w.timestamp
                if w_ts.tzinfo is None:
                    w_ts = w_ts.replace(tzinfo=timezone.utc)

                # Fast-forward deposit pointer to ignore old deposits
                while d_idx < d_len:
                    d_ts = asset_deposits[d_idx].timestamp
                    if d_ts.tzinfo is None:
                        d_ts = d_ts.replace(tzinfo=timezone.utc)
                    if (d_ts - w_ts).total_seconds() < 0:
                        d_idx += 1
                    else:
                        break

                # Scan deposits inside the 24h window
                curr_d_idx = d_idx
                while curr_d_idx < d_len:
                    d = asset_deposits[curr_d_idx]
                    d_ts = d.timestamp
                    if d_ts.tzinfo is None:
                        d_ts = d_ts.replace(tzinfo=timezone.utc)
                    
                    time_diff = (d_ts - w_ts).total_seconds()

                    if time_diff > 86400: break # Window closed

                    if d.id not in reconciled_ids:
                        d_amt = d.amount_to or 0.0
                        if d_amt <= w_amt and d_amt >= (w_amt * 0.95):
                            w.is_taxable_event = 0
                            d.is_taxable_event = 0
                            w.linked_transaction_id = d.id
                            d.linked_transaction_id = w.id
                            w.category = "Transfer"
                            d.category = "Transfer"
                            reconciled_ids.update([w.id, d.id])

                            fee_amount = w_amt - d_amt
                            if fee_amount > 1e-10:
                                w.fee_amount = (w.fee_amount or 0.0) + fee_amount
                                w.fee_asset = w.asset_from
                                w.issue_notes = (w.issue_notes or "") + f" | Transfer fee applied: {fee_amount} {w.asset_from}"
                            break
                    curr_d_idx += 1
        return reconciled_ids
    async def _run_valuation(self, txs: List[Transaction], db: AsyncSession):
        if not txs: return

        min_date = get_jerusalem_date(txs[0].timestamp)
        max_date = get_jerusalem_date(txs[-1].timestamp)
        await boi_service.prefetch_rates(min_date, max_date, db=db)

        rate_cache: Dict[date, float] = {}

        for tx in txs:
            rate_date = get_jerusalem_date(tx.timestamp)
            if rate_date not in rate_cache:
                rate_cache[rate_date] = await boi_service.get_usd_ils_rate(rate_date, db=db)

            tx.ils_exchange_rate = rate_cache[rate_date]
            tx.ils_rate_date = rate_date
    async def get_ils_value(self, asset: str, amount: float, tx_date: date, usd_ils_rate: float) -> float:
        if not asset or amount == 0:
            return 0.0
        if asset == 'ILS':
            return amount
            
        rate = usd_ils_rate or 3.65
        
        if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
            return amount * rate
        
        usd_price = await price_service.get_historical_price(asset, tx_date)
        if usd_price is not None:
            val = amount * usd_price * rate
            logger.info(f"Price: {asset} on {tx_date} is ${usd_price}, ILS value: {val}")
            return val
        
        logger.warning(f"MISSING PRICE: {asset} on {tx_date}")
        return 0.0

    async def _process_transaction(self, tx: Transaction, ledger: TaxLedger, reconciled_ids: Set[int], db: AsyncSession, use_wash_sale_rule: bool = False):
        # Reset results
        tx.capital_gain_ils = 0.0
        tx.inflationary_gain_ils = 0.0
        tx.real_gain_ils = 0.0
        tx.cost_basis_ils = 0.0
        tx.ordinary_income_ils = 0.0
        tx.is_taxable_event = 0

        rate = tx.ils_exchange_rate or 3.65
        tx_date = get_jerusalem_date(tx.timestamp)

        # 1. Handle Kraken Futures PnL (Special Case)
        if tx.exchange == 'krakenfutures' and tx.type in [TransactionType.fee, TransactionType.earn] and (tx.asset_from in ['USD', 'BTC', 'XBT'] or tx.asset_to in ['USD', 'BTC', 'XBT']):
            tx_amt_to = tx.amount_to or 0.0
            tx_amt_from = tx.amount_from or 0.0
            pnl_amount = tx_amt_to if tx_amt_to > 0 else -tx_amt_from
            tx.capital_gain_ils = pnl_amount * rate
            tx.real_gain_ils = tx.capital_gain_ils # Futures usually have no Madad adjustment
            tx.is_taxable_event = 1
            if tx_amt_to > 0:
                await ledger.add_lot(tx.asset_to, tx_amt_to, tx.capital_gain_ils, tx)
            return

        # Pre-calculate values
        amt_from = tx.amount_from or 0.0
        amt_to = tx.amount_to or 0.0
        val_from = await self.get_ils_value(tx.asset_from, amt_from, tx_date, rate)
        val_to = await self.get_ils_value(tx.asset_to, amt_to, tx_date, rate)
        fee_ils = await self.get_ils_value(tx.fee_asset, tx.fee_amount or 0.0, tx_date, rate)

        # For swaps, we use fiat value if available, otherwise FMV of acquired/disposed asset.
        if tx.asset_from in ['USD', 'ILS']:
            effective_swap_value = val_from
        elif tx.asset_to in ['USD', 'ILS']:
            effective_swap_value = val_to
        else:
            effective_swap_value = val_to if val_to > 0 else val_from

        # Fallback for fee_ils if external price fetch failed
        if fee_ils == 0 and (tx.fee_amount or 0) > 0 and tx.fee_asset:
            if tx.fee_asset == tx.asset_from and amt_from > 0:
                fee_ils = (effective_swap_value / amt_from) * tx.fee_amount
                logger.info(f"Fee fallback: {tx.fee_asset} used asset_from unit price. fee_ils={fee_ils}")
            elif tx.fee_asset == tx.asset_to and amt_to > 0:
                fee_ils = (effective_swap_value / amt_to) * tx.fee_amount
                logger.info(f"Fee fallback: {tx.fee_asset} used asset_to unit price. fee_ils={fee_ils}")

        # 2. Process Disposal (Sell / Withdrawal / Fee)
        is_sell = amt_from > 0 and tx.asset_from
        if is_sell:
            asset = tx.asset_from
            qty = amt_from
            is_fiat = asset in ['USD', 'ILS']

            if tx.id not in reconciled_ids:
                cost_basis, consumptions = await ledger.consume_lots(asset, qty, tx) if not is_fiat else (0.0, [])

                # Valuation of proceeds: If swap, use effective_swap_value
                proceeds = (effective_swap_value if amt_to > 0 else val_from) - fee_ils

                tx.cost_basis_ils = cost_basis

                # ITA Rule: Stablecoins are NOT fiat.
                if not is_fiat and tx.type != TransactionType.withdrawal:
                    tx.is_taxable_event = 1
                    total_nominal_gain = proceeds - cost_basis
                    
                    # Calculate Madad-adjusted gains from consumptions
                    total_inflationary = 0.0
                    for c in consumptions:
                        c.inflationary_gain_ils = c.adjusted_cost_basis_ils - c.ils_value_consumed
                        total_inflationary += c.inflationary_gain_ils
                    
                    tx.inflationary_gain_ils = total_inflationary
                    tx.capital_gain_ils = total_nominal_gain
                    # The math naturally forces any missing cost basis to become 100% real gain
                    tx.real_gain_ils = total_nominal_gain - total_inflationary 

                    # Wash Sale Rule (Section 94B)
                    if use_wash_sale_rule and tx.capital_gain_ils < 0:
                        await ledger.record_loss(asset, abs(tx.capital_gain_ils), qty, tx)
                        # We defer the loss, so we zero out the taxable gains for this TX
                        tx.capital_gain_ils = 0.0
                        tx.real_gain_ils = 0.0
                        tx.inflationary_gain_ils = 0.0
                else:
                    tx.is_taxable_event = 0

        # 3. Process Acquisition (Buy / Deposit / Earn / Airdrop / Fork)
        is_buy = amt_to > 0 and tx.asset_to
        if is_buy:
            asset = tx.asset_to
            qty = amt_to
            is_fiat = asset in ['USD', 'ILS']

            # If it's a reconciled transfer or fiat, skip adding to ledger completely
            if tx.id in reconciled_ids:
                # Don't overwrite if it was already marked taxable by disposal side
                pass 
            elif is_fiat:
                # Don't overwrite if it was already marked taxable by disposal side
                pass
            else:
                if tx.type in [TransactionType.earn, TransactionType.airdrop]:
                    cost_basis = val_to
                    tx.is_taxable_event = 1
                    tx.ordinary_income_ils = cost_basis
                elif tx.type == TransactionType.fork:
                    cost_basis = 0.0
                    # Don't overwrite taxable status
                else:
                    # Swap or Buy
                    cost_basis = effective_swap_value

                cost_basis += fee_ils
                await ledger.add_lot(asset, qty, cost_basis, tx)
                
                if not is_sell and tx.type != TransactionType.deposit:
                    tx.cost_basis_ils = cost_basis


        # 4. Handle Crypto Fee Disposals (CRITICAL ITA)
        f_amt = tx.fee_amount or 0.0
        if f_amt > 0 and tx.fee_asset and tx.fee_asset not in ['USD', 'ILS']:
            fee_asset = tx.fee_asset
            fee_qty = f_amt
            fee_cost_basis, fee_consumptions = await ledger.consume_lots(fee_asset, fee_qty, tx)
            fee_proceeds = fee_ils
            
            fee_nominal_gain = fee_proceeds - fee_cost_basis
            
            # Calculate Madad for fee disposal
            fee_inflationary = 0.0
            for c in fee_consumptions:
                c.inflationary_gain_ils = c.adjusted_cost_basis_ils - c.ils_value_consumed
                fee_inflationary += c.inflationary_gain_ils

            tx.inflationary_gain_ils += fee_inflationary
            tx.capital_gain_ils += fee_nominal_gain
            tx.real_gain_ils += (fee_nominal_gain - fee_inflationary)
            tx.is_taxable_event = 1

    async def get_kpi(self, db: AsyncSession, year: Optional[int] = None, tax_bracket: float = 0.25) -> Dict[str, Any]:
        # Clear session to force fresh data from DB
        db.expire_all()
        
        # We fetch only essential fields to avoid object attachment issues if possible, 
        # but let's stick to the model for now and ensure we have fresh data.
        stmt = select(Transaction).where(Transaction.is_active == True).order_by(Transaction.timestamp.asc())
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
        # 1. Form 1391 Check (Foreign Assets > 2M ILS)
        foreign_exchanges = set()
        keys_stmt = select(ExchangeKeyModel)
        keys_result = await db.execute(keys_stmt)
        for k in keys_result.scalars().all():
            if k.exchange_name.lower() not in ['bitsofgold', 'altshuler']:
                foreign_exchanges.add(k.exchange_name.lower())
        
        max_daily_foreign_value_ils = 0.0
        form_1391_breached = False
        threshold_ils = 2000000.0
        
        inventory_by_exchange: Dict[str, Dict[str, float]] = {}
        txs_by_date: Dict[date, List[Transaction]] = {}
        for tx in all_txs:
            d = get_jerusalem_date(tx.timestamp)
            txs_by_date.setdefault(d, []).append(tx)

        all_sorted_dates = sorted(txs_by_date.keys())
        if all_sorted_dates:
            first_date = all_sorted_dates[0]
            
            # If a specific year is requested, we must check every day of that year
            # starting from either Jan 1st or the first transaction ever.
            if year:
                curr = min(first_date, date(year, 1, 1))
                end = min(date.today(), date(year, 12, 31))
            else:
                curr = first_date
                end = all_sorted_dates[-1]

            while curr <= end:
                if curr in txs_by_date:
                    for tx in txs_by_date[curr]:
                        ex = tx.exchange.lower()
                        if ex not in inventory_by_exchange: inventory_by_exchange[ex] = {}
                        if tx.asset_from: inventory_by_exchange[ex][tx.asset_from] = inventory_by_exchange[ex].get(tx.asset_from, 0.0) - (tx.amount_from or 0.0)
                        if tx.asset_to: inventory_by_exchange[ex][tx.asset_to] = inventory_by_exchange[ex].get(tx.asset_to, 0.0) + (tx.amount_to or 0.0)
                        if tx.fee_asset: inventory_by_exchange[ex][tx.fee_asset] = inventory_by_exchange[ex].get(tx.fee_asset, 0.0) - (tx.fee_amount or 0.0)
                
                # Only check breach for the target year
                if not year or curr.year == year:
                    daily_foreign_val = 0.0
                    # Quick check: does any foreign exchange have non-zero inventory?
                    has_foreign_assets = False
                    for ex in foreign_exchanges:
                        if ex in inventory_by_exchange:
                            if any(qty > 1e-8 for qty in inventory_by_exchange[ex].values()):
                                has_foreign_assets = True
                                break
                    
                    if has_foreign_assets:
                        usd_ils_rate = await boi_service.get_usd_ils_rate(curr, db=db)
                        for ex in foreign_exchanges:
                            if ex in inventory_by_exchange:
                                for asset, qty in inventory_by_exchange[ex].items():
                                    if qty > 1e-8:
                                        if asset not in ['USD', 'ILS']:
                                            usd_price = await price_service.get_historical_price(asset, curr)
                                            if usd_price: daily_foreign_val += qty * usd_price * usd_ils_rate
                                        elif asset == 'USD':
                                            daily_foreign_val += qty * usd_ils_rate
                        
                        if daily_foreign_val > max_daily_foreign_value_ils:
                            max_daily_foreign_value_ils = daily_foreign_val
                            if max_daily_foreign_value_ils > threshold_ils: form_1391_breached = True
                
                curr += timedelta(days=1)

        # 2. Tax KPIs
        accumulated_loss = 0.0
        report_year_real_gain = 0.0
        report_year_ordinary = 0.0
        report_year_trade_count = 0
        report_year_total_nominal_gain = 0.0 
        report_year_inflationary_gain = 0.0
        report_year_capital_losses = 0.0
        report_year_issue_count = 0
        
        txs_by_year: Dict[int, List[Transaction]] = {}
        for tx in all_txs:
            y = get_jerusalem_date(tx.timestamp).year
            txs_by_year.setdefault(y, []).append(tx)
        
        for y in sorted(txs_by_year.keys()):
            y_txs = txs_by_year[y]
            y_real_gain = sum(t.real_gain_ils or 0.0 for t in y_txs if t.is_taxable_event == 1)
            y_nominal_gain = sum(t.capital_gain_ils or 0.0 for t in y_txs if t.is_taxable_event == 1)
            y_inflationary_gain = sum(t.inflationary_gain_ils or 0.0 for t in y_txs if t.is_taxable_event == 1)
            y_ordinary = sum(t.ordinary_income_ils or 0.0 for t in y_txs)
            y_capital_losses = sum(abs(t.real_gain_ils) for t in y_txs if t.is_taxable_event == 1 and (t.real_gain_ils or 0) < 0)

            if year and y < year:
                accumulated_loss += y_real_gain
                if accumulated_loss > 0: accumulated_loss = 0.0 
            elif year is None or y == year:
                report_year_trade_count += len([t for t in y_txs if t.is_taxable_event == 1 and ((t.capital_gain_ils or 0) != 0 or (t.real_gain_ils or 0) != 0)])
                report_year_total_nominal_gain += y_nominal_gain
                report_year_inflationary_gain += y_inflationary_gain
                report_year_capital_losses += y_capital_losses
                report_year_ordinary += y_ordinary
                report_year_issue_count += len([t for t in y_txs if t.is_issue])
                
                net_real_gain = y_real_gain + accumulated_loss
                if net_real_gain < 0:
                    report_year_real_gain = 0.0
                    accumulated_loss = net_real_gain 
                else:
                    report_year_real_gain = net_real_gain
                    accumulated_loss = 0.0
        
        all_tx_count = len([t for t in all_txs if (year is None or get_jerusalem_date(t.timestamp).year == year)])
        
        return {
            'year': year,
            'total_nominal_gain_ils': round(report_year_total_nominal_gain, 2),
            'ordinary_income_ils': round(report_year_ordinary, 2),
            'net_capital_gain_ils': round(report_year_real_gain, 2),
            'inflationary_gain_ils': round(report_year_inflationary_gain, 2),
            'capital_losses_ils': round(report_year_capital_losses, 2),
            'carried_forward_loss_ils': round(abs(min(0, accumulated_loss)), 2),
            'tax_bracket': tax_bracket,
            'estimated_tax_ils': round(max(0, (report_year_real_gain * 0.25) + (report_year_ordinary * tax_bracket)), 2),
            'trade_count': report_year_trade_count,
            'total_transactions': all_tx_count,
            'high_frequency_warning': report_year_trade_count > 100,
            'issue_count': report_year_issue_count,
            'form_1391_breached': form_1391_breached,
            'max_foreign_value_ils': round(max_daily_foreign_value_ils, 2)
        }

    async def get_unrealized_inventory(self, db: AsyncSession) -> List[Dict[str, Any]]:
        # 1. Fetch all active transactions to build current ledger state
        txs_stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(txs_stmt)
        all_txs = result.scalars().all()
        
        if not all_txs:
            return []

        # We don't merge or reconcile here to avoid modifying DB, just build the FIFO state
        active_txs = [t for t in all_txs if t.is_active]
        
        # Phase 3: The FIFO Ledger (Read-only simulation)
        ledger = TaxLedger(db, dry_run=True)
        for tx in active_txs:
            amt_from = tx.amount_from or 0.0
            amt_to = tx.amount_to or 0.0
            
            # Acquisition
            if amt_to > 0 and tx.asset_to and tx.asset_to not in ['USD', 'ILS']:
                cost = tx.cost_basis_ils or 0.0
                await ledger.add_lot(tx.asset_to, amt_to, cost, tx)
            
            # Disposal
            if amt_from > 0 and tx.asset_from and tx.asset_from not in ['USD', 'ILS']:
                await ledger.consume_lots(tx.asset_from, amt_from, tx)

        # 2. Calculate unrealized PnL using current prices
        unrealized = []
        usd_ils_rate = await boi_service.get_usd_ils_rate(date.today() - timedelta(days=1), db=db)
        
        for asset, lots in ledger.inventory.items():
            total_qty = sum(l['amount'] for l in lots)
            if total_qty < 1e-8: continue
            
            total_cost_basis = sum(l['cost_basis_ils'] for l in lots)
            
            cpi_current = await cpi_service.get_cpi_index(date.today(), db)
            total_adjusted_cost_basis = 0.0
            for l in lots:
                cpi_buy = await cpi_service.get_cpi_index(get_jerusalem_date(l['timestamp']), db)
                total_adjusted_cost_basis += l['cost_basis_ils'] * max(1.0, cpi_current / cpi_buy)
            
            current_price_usd = await price_service.get_current_price(asset)
            if current_price_usd:
                current_value_ils = total_qty * current_price_usd * usd_ils_rate
                unrealized_gain_ils = current_value_ils - total_cost_basis
                real_unrealized_gain_ils = current_value_ils - total_adjusted_cost_basis
                
                unrealized.append({
                    'asset': asset,
                    'quantity': round(total_qty, 8),
                    'cost_basis_ils': round(total_cost_basis, 2),
                    'current_price_usd': round(current_price_usd, 4),
                    'current_value_ils': round(current_value_ils, 2),
                    'unrealized_gain_ils': round(unrealized_gain_ils, 2),
                    'real_unrealized_gain_ils': round(real_unrealized_gain_ils, 2),
                    'potential_tax_saving_ils': round(abs(min(0, real_unrealized_gain_ils)) * 0.25, 2)
                })
            else:
                unrealized.append({
                    'asset': asset,
                    'quantity': round(total_qty, 8),
                    'cost_basis_ils': round(total_cost_basis, 2),
                    'current_price_usd': None,
                    'current_value_ils': 0.0,
                    'unrealized_gain_ils': 0.0,
                    'real_unrealized_gain_ils': 0.0,
                    'potential_tax_saving_ils': 0.0
                })
        
        return sorted(unrealized, key=lambda x: x['real_unrealized_gain_ils'])

    async def get_years(self, db: AsyncSession) -> List[int]:
        stmt = select(Transaction.timestamp).where(Transaction.is_active == True)
        result = await db.execute(stmt)
        years = set()
        for row in result.all():
            if row[0]:
                years.add(get_jerusalem_date(row[0]).year)
        return sorted(list(years), reverse=True) if years else [date.today().year]
