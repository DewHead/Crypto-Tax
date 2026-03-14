from typing import List, Dict, Any, Optional, Set
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, extract, distinct, func
from app.models.transaction import Transaction, TransactionType
from app.models.tax_lot_consumption import TaxLotConsumption
from app.services.boi import boi_service
from app.services.price import price_service
from app.services.cpi import cpi_service
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)

def get_jerusalem_date(dt: datetime) -> date:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo("Asia/Jerusalem")).date()

class TaxLedger:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.lots: Dict[str, List[Dict[str, Any]]] = {} # asset -> list of lots
        self.recent_losses: Dict[str, List[Dict[str, Any]]] = {} # asset -> list of losses (for wash sales)

    async def add_lot(self, asset: str, amount: float, ils_value: float, tx: Transaction):
        if asset not in self.lots:
            self.lots[asset] = []
        
        self.lots[asset].append({
            'amount': amount,
            'cost_basis_ils': ils_value,
            'timestamp': tx.timestamp,
            'tx_id': tx.id
        })
        logger.info(f"Lot added: {amount} {asset} @ {ils_value} ILS (TX {tx.id})")

    async def consume_lots(self, asset: str, amount: float, tx: Transaction) -> tuple[float, List[TaxLotConsumption]]:
        if asset not in self.lots or not self.lots[asset]:
            # Missing cost basis - ITA rule: Default to ZERO
            return 0.0, []
        
        remaining_to_consume = amount
        total_cost_basis_ils = 0.0
        consumptions = []

        # FIFO
        while remaining_to_consume > 1e-9 and self.lots[asset]:
            lot = self.lots[asset][0]
            consume_qty = min(remaining_to_consume, lot['amount'])
            
            # Proportion of the lot's ILS cost basis
            ils_cost = (consume_qty / lot['amount']) * lot['cost_basis_ils']
            
            # Israeli Tax Rules: Adjusted Cost Basis (CPI)
            buy_date = get_jerusalem_date(lot['timestamp'])
            sell_date = get_jerusalem_date(tx.timestamp)
            
            buy_index = await cpi_service.get_cpi_index(buy_date, db=self.db)
            sell_index = await cpi_service.get_cpi_index(sell_date, db=self.db)
            
            # Inflation factor. If index dropped (deflation), factor is 1.0 (cost basis doesn't decrease)
            inflation_factor = max(1.0, sell_index / buy_index) if buy_index > 0 else 1.0
            adjusted_cost = ils_cost * inflation_factor

            consumption = TaxLotConsumption(
                sell_tx_id=tx.id,
                buy_tx_id=lot['tx_id'],
                amount_consumed=consume_qty,
                ils_value_consumed=ils_cost,
                adjusted_cost_basis_ils=adjusted_cost
            )
            self.db.add(consumption)
            consumptions.append(consumption)

            total_cost_basis_ils += ils_cost
            lot['amount'] -= consume_qty
            lot['cost_basis_ils'] -= ils_cost
            remaining_to_consume -= consume_qty
            
            if lot['amount'] < 1e-9:
                self.lots[asset].pop(0)

        return total_cost_basis_ils, consumptions

    async def record_loss(self, asset: str, loss_ils: float, amount: float, tx: Transaction):
        """
        Israeli Wash Sale Rule (Section 94B): 
        If an asset is sold at a loss and a replacement is bought within 30 days before/after,
        the loss is not realized but added to the cost basis of the replacement.
        """
        tx_ts = tx.timestamp
        if tx_ts.tzinfo is None: tx_ts = tx_ts.replace(tzinfo=timezone.utc)
        
        remaining_loss = loss_ils
        remaining_qty = amount

        # Check for Forward Wash Sale (already bought replacement)
        if asset in self.lots:
            for lot in reversed(self.lots[asset]):
                lot_ts = lot['timestamp']
                if lot_ts.tzinfo is None: lot_ts = lot_ts.replace(tzinfo=timezone.utc)
                
                time_diff = (tx_ts - lot_ts).total_seconds()
                if 0 < time_diff <= 30 * 86400: # Bought within 30 days before sale
                    # This lot absorbs the loss
                    absorb_qty = min(remaining_qty, lot['amount'])
                    absorb_proportion = absorb_qty / amount
                    absorb_loss = loss_ils * absorb_proportion
                    
                    lot['cost_basis_ils'] += absorb_loss
                    remaining_loss -= absorb_loss
                    remaining_qty -= absorb_qty
                    
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
        self.ledger: Optional[TaxLedger] = None

    async def calculate_taxes(self, db: AsyncSession, use_wash_sale_rule: bool = False):
        logger.info("Starting tax calculation engine (Israeli Rules)...")
        
        # 1. Clear previous results (except manual notes/adjustments if we had any)
        # For now, full reset for simplicity
        await db.execute(delete(TaxLotConsumption))
        await db.execute(select(Transaction).filter(Transaction.is_active == True)) # Warming up
        
        # 2. Load and sort all active transactions
        result = await db.execute(
            select(Transaction)
            .filter(Transaction.is_active == True)
            .order_by(Transaction.timestamp.asc())
        )
        txs = result.scalars().all()
        
        # 3. Pre-process: Reconciliation (Transfers) and Merging (Avalanche/Dust)
        await self._run_avalanche_merger(txs, db)
        reconciled_ids = await self._run_transfer_reconciliation(txs, db)
        
        # 4. Valuation: Fetch USD/ILS rates for all dates
        await self._run_valuation(txs, db)
        
        # 5. Process Ledger
        self.ledger = TaxLedger(db)
        for tx in txs:
            await self._process_transaction(tx, self.ledger, reconciled_ids, db, use_wash_sale_rule=use_wash_sale_rule)
            
        await db.commit()
        logger.info("Tax calculation completed successfully.")

    async def _run_avalanche_merger(self, txs: List[Transaction], db: AsyncSession):
        """
        Binance dust conversions often appear as dozens of simultaneous small trades.
        ITA allows grouping these into a single event if the FMV is small.
        """
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
                    nxt.asset_to == curr.asset_to and
                    time_diff < 1.0):
                    
                    # Merge nxt into curr
                    curr.amount_from = (curr.amount_from or 0.0) + (nxt.amount_from or 0.0)
                    curr.amount_to = (curr.amount_to or 0.0) + (nxt.amount_to or 0.0)
                    curr.fee_amount = (curr.fee_amount or 0.0) + (nxt.fee_amount or 0.0)
                    
                    nxt.is_active = False
                    nxt.parent_tx_id = curr.id
                    merged_count += 1
                j += 1
            if merged_count > 0:
                logger.info(f"Merged {merged_count} dust/convert transactions into TX {curr.id}")
            i += 1

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
        
        if asset == 'USD':
            return amount * usd_ils_rate
        if asset == 'ILS':
            return amount
        
        # Try to get price from local DB/cache first
        price_usd = await price_service.get_historical_price(asset, tx_date)
        if price_usd:
            return amount * price_usd * usd_ils_rate
        
        # Placeholder / Unknown
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

        if tx.type == TransactionType.buy:
            effective_swap_value = val_from if val_from > 0 else val_to
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

            if is_fiat:
                tx.is_taxable_event = 0
                tx.cost_basis_ils = 0.0 
            else:
                if tx.type in [TransactionType.earn, TransactionType.airdrop]:
                    cost_basis = val_to
                    tx.is_taxable_event = 1
                    tx.ordinary_income_ils = cost_basis
                elif tx.type == TransactionType.fork:
                    cost_basis = 0.0
                    tx.is_taxable_event = 0
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
        stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
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
            if not tx.is_active: continue
            # ITA Rule: Fiscal year must follow Jerusalem Time.
            y = get_jerusalem_date(tx.timestamp).year
            if y not in txs_by_year: txs_by_year[y] = []
            txs_by_year[y].append(tx)
        
        for y in sorted(txs_by_year.keys()):
            # ITA Rule: Tax is on Real Gain. 
            # If Real Gain < 0, it's a loss that can offset other capital gains.
            y_nominal_gain = sum(t.capital_gain_ils or 0.0 for t in txs_by_year[y] if t.is_taxable_event)
            y_real_gain = sum(t.real_gain_ils or 0.0 for t in txs_by_year[y] if t.is_taxable_event)
            y_inflationary_gain = sum(t.inflationary_gain_ils or 0.0 for t in txs_by_year[y] if t.is_taxable_event)
            y_ordinary = sum(t.ordinary_income_ils or 0.0 for t in txs_by_year[y])
            
            # ITA Form 1391: Sum of only negative real gains this year
            y_capital_losses = sum(abs(t.real_gain_ils) for t in txs_by_year[y] if t.is_taxable_event and t.real_gain_ils < 0)

            if year and y < year:
                accumulated_loss += y_real_gain
                if accumulated_loss > 0: accumulated_loss = 0.0 
            elif year is None or y == year:
                report_year_trade_count += len([t for t in txs_by_year[y] if t.is_taxable_event and t.capital_gain_ils != 0])
                report_year_total_nominal_gain += y_nominal_gain
                report_year_inflationary_gain += y_inflationary_gain
                report_year_capital_losses += y_capital_losses
                report_year_ordinary += y_ordinary
                report_year_issue_count += len([t for t in txs_by_year[y] if t.is_issue])
                
                net_real_gain = y_real_gain + accumulated_loss
                if net_real_gain < 0:
                    report_year_real_gain = 0.0
                    accumulated_loss = net_real_gain 
                else:
                    report_year_real_gain = net_real_gain
                    accumulated_loss = 0.0
        
        all_tx_count = len([t for t in all_txs if (year is None or get_jerusalem_date(t.timestamp).year == year) and t.is_active])
        
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
            'issue_count': report_year_issue_count
        }

    async def get_years(self, db: AsyncSession) -> List[int]:
        # We fetch distinct years from UTC first as a rough set
        stmt = select(distinct(extract('year', Transaction.timestamp)))
        result = await db.execute(stmt)
        base_years = [int(row[0]) for row in result.all() if row[0] is not None]
        
        # Then we specifically check for edge cases near Jan 1st
        # In reality, grouping by get_jerusalem_date(timestamp).year is the only 100% correct way.
        # Since the number of transactions is manageable, we'll fetch all active timestamps.
        stmt = select(Transaction.timestamp).where(Transaction.is_active == True)
        result = await db.execute(stmt)
        years = set()
        for row in result.all():
            if row[0]:
                years.add(get_jerusalem_date(row[0]).year)
        
        return sorted(list(years), reverse=True) if years else sorted(base_years, reverse=True)
