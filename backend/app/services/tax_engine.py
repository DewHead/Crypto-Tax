from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, extract, distinct, delete
from app.models.transaction import Transaction, TransactionType
from app.models.tax_lot_consumption import TaxLotConsumption
from app.services.boi import boi_service
from app.services.price import price_service
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Set
import asyncio
import logging

logger = logging.getLogger(__name__)

class TaxLedger:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.inventory: Dict[str, List[Dict[str, Any]]] = {}
        self.recent_losses: Dict[str, List[Dict[str, Any]]] = {} # asset -> list of {'timestamp', 'loss_ils', 'amount'}

    async def consume_lots(self, asset: str, qty: float, sell_tx: Transaction) -> float:
        logger.info(f"Consuming {qty} {asset} for TX {sell_tx.id} ({sell_tx.timestamp})")
        if asset not in self.inventory or not self.inventory[asset]:
            sell_tx.is_issue = True
            sell_tx.issue_notes = (sell_tx.issue_notes or "") + f" | Missing cost basis for {round(qty, 8)} {asset}."
            return 0.0

        total_cost_basis_ils = 0.0
        qty_to_match = qty
        
        while qty_to_match > 1e-10 and self.inventory[asset]:
            oldest_buy = self.inventory[asset][0]
            buy_tx_id = oldest_buy['tx_id']
            
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

            total_cost_basis_ils += matched_cost
            qty_to_match -= matched_qty

            # Record consumption for audit trail
            consumption = TaxLotConsumption(
                sell_tx_id=sell_tx.id,
                buy_tx_id=buy_tx_id,
                amount_consumed=matched_qty,
                ils_value_consumed=matched_cost
            )
            self.db.add(consumption)

        return total_cost_basis_ils

    def add_lot(self, asset: str, amount: float, cost_basis_ils: float, tx: Transaction):
        logger.info(f"Adding lot: {amount} {asset} at {cost_basis_ils} ILS from TX {tx.id} ({tx.timestamp})")
        if asset not in self.inventory:
            self.inventory[asset] = []
        
        # 30-Day Wash Sale Rule (Section 94B): If we have recent deferred losses, add them to cost basis
        deferred_loss = 0.0
        if asset in self.recent_losses:
            # Filter losses within 30 days of this buy
            valid_losses = []
            for loss_entry in self.recent_losses[asset]:
                if tx.timestamp - loss_entry['timestamp'] <= timedelta(days=30):
                    # Pro-rate the loss if this buy is smaller than the sell that caused the loss?
                    # ITA usually rolls the whole loss if repurchased.
                    deferred_loss += loss_entry['loss_ils']
                else:
                    pass # expired loss
            # For simplicity, we consume all valid deferred losses and clear them
            self.recent_losses[asset] = [] 

        self.inventory[asset].append({
            'tx_id': tx.id,
            'amount': amount,
            'cost_basis_ils': cost_basis_ils + deferred_loss,
            'timestamp': tx.timestamp
        })

    def record_loss(self, asset: str, loss_ils: float, amount: float, tx: Transaction):
        if asset not in self.recent_losses:
            self.recent_losses[asset] = []
        self.recent_losses[asset].append({
            'timestamp': tx.timestamp,
            'loss_ils': loss_ils,
            'amount': amount,
            'tx_id': tx.id
        })

class TaxEngine:
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
            if not curr.is_active or curr.type not in [TransactionType.buy, TransactionType.sell]:
                i += 1
                continue
            
            j = i + 1
            merged_count = 0
            while j < len(txs):
                nxt = txs[j]
                time_diff = (nxt.timestamp - curr.timestamp).total_seconds()
                if (nxt.is_active and
                    nxt.exchange == curr.exchange and 
                    nxt.type == curr.type and 
                    nxt.asset_from == curr.asset_from and 
                    nxt.asset_to == curr.asset_to and 
                    time_diff <= 5):
                    
                    curr.amount_from += nxt.amount_from
                    curr.amount_to += nxt.amount_to
                    curr.fee_amount += nxt.fee_amount
                    
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
        
        for w in withdrawals:
            for d in deposits:
                if d.id in reconciled_ids:
                    continue
                
                time_diff = (d.timestamp - w.timestamp).total_seconds()
                if 0 <= time_diff <= 86400: # 24 hours
                    if d.asset_to == w.asset_from:
                        # Heuristic: 5% fee variance
                        if d.amount_to <= w.amount_from and d.amount_to >= (w.amount_from * 0.95):
                            w.is_taxable_event = 0
                            d.is_taxable_event = 0
                            w.linked_transaction_id = d.id
                            d.linked_transaction_id = w.id
                            w.category = "Transfer"
                            d.category = "Transfer"
                            
                            reconciled_ids.add(w.id)
                            reconciled_ids.add(d.id)
                            
                            # Log transfer fee if any
                            fee_amount = w.amount_from - d.amount_to
                            if fee_amount > 1e-10:
                                w.issue_notes = (w.issue_notes or "") + f" | Transfer fee: {fee_amount} {w.asset_from}"
                            break
        return reconciled_ids

    async def _run_valuation(self, txs: List[Transaction], db: AsyncSession):
        if not txs: return
        
        min_date = txs[0].timestamp.date()
        max_date = txs[-1].timestamp.date()
        await boi_service.prefetch_rates(min_date, max_date)
        
        rate_cache: Dict[date, float] = {}

        for tx in txs:
            rate_date = tx.timestamp.date()
            if rate_date not in rate_cache:
                rate_cache[rate_date] = await boi_service.get_usd_ils_rate(rate_date)
            
            tx.ils_exchange_rate = rate_cache[rate_date]
            tx.ils_rate_date = rate_date

    async def get_ils_value(self, asset: str, amount: float, tx_date: date, usd_ils_rate: float) -> float:
        if not asset or amount == 0:
            return 0.0
        if asset == 'ILS':
            return amount
        if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
            return amount * usd_ils_rate
        
        usd_price = await price_service.get_historical_price(asset, tx_date)
        if usd_price is not None:
            val = amount * usd_price * usd_ils_rate
            logger.info(f"Price: {asset} on {tx_date} is ${usd_price}, ILS value: {val}")
            return val
        
        logger.warning(f"MISSING PRICE: {asset} on {tx_date}")
        return 0.0

    async def _process_transaction(self, tx: Transaction, ledger: TaxLedger, reconciled_ids: Set[int], db: AsyncSession, use_wash_sale_rule: bool = False):
        # Reset results
        tx.capital_gain_ils = 0.0
        tx.cost_basis_ils = 0.0
        tx.ordinary_income_ils = 0.0
        tx.is_taxable_event = 0

        rate = tx.ils_exchange_rate
        tx_date = tx.timestamp.date()

        # 1. Handle Kraken Futures PnL (Special Case)
        if tx.exchange == 'krakenfutures' and tx.type in [TransactionType.fee, TransactionType.earn] and (tx.asset_from in ['USD', 'BTC', 'XBT'] or tx.asset_to in ['USD', 'BTC', 'XBT']):
            pnl_amount = tx.amount_to if tx.amount_to > 0 else -tx.amount_from
            tx.capital_gain_ils = pnl_amount * rate
            tx.is_taxable_event = 1
            if tx.amount_to > 0:
                ledger.add_lot(tx.asset_to, tx.amount_to, tx.capital_gain_ils, tx)
            return

        # Pre-calculate values
        amt_from = tx.amount_from or 0.0
        amt_to = tx.amount_to or 0.0
        val_from = await self.get_ils_value(tx.asset_from, amt_from, tx_date, rate)
        val_to = await self.get_ils_value(tx.asset_to, amt_to, tx_date, rate)

        # For swaps, we use the higher of the two valuations if one is missing, 
        # but preferably val_to for cost basis of new asset.
        effective_swap_value = val_to if val_to > 0 else val_from

        # 2. Process Disposal (Sell / Withdrawal / Fee)
        is_sell = amt_from > 0 and tx.asset_from
        if is_sell:
            asset = tx.asset_from
            qty = amt_from

            if tx.id not in reconciled_ids:
                cost_basis = await ledger.consume_lots(asset, qty, tx)

                # Valuation of proceeds: If swap, use effective_swap_value
                proceeds = effective_swap_value if amt_to > 0 else val_from

                # Fallback for missing cost basis: assume proceeds (Gain 0)
                if cost_basis == 0 and qty > 0:
                    cost_basis = proceeds
                    logger.warning(f"MISSING COST BASIS FALLBACK: {tx.timestamp} {asset} qty={qty}, proceeds={proceeds}")

                tx.cost_basis_ils = cost_basis

                # ITA Rule: Stablecoins are NOT fiat.
                is_fiat = asset in ['USD', 'ILS']
                if not is_fiat and tx.type != TransactionType.withdrawal:
                    tx.is_taxable_event = 1
                    gain = proceeds - cost_basis

                    # Wash Sale Rule (Section 94B)
                    if use_wash_sale_rule and gain < 0:
                        ledger.record_loss(asset, abs(gain), qty, tx)
                        tx.capital_gain_ils = 0.0 # Deferred
                    else:
                        tx.capital_gain_ils = gain
                else:
                    tx.is_taxable_event = 0

        # 3. Process Acquisition (Buy / Deposit / Earn)
        is_buy = amt_to > 0 and tx.asset_to
        if is_buy:
            asset = tx.asset_to
            qty = amt_to

            if tx.id in reconciled_ids:
                # Transfer: Simplified fallback to market price
                cost_basis = val_to
            elif tx.type == TransactionType.earn:
                cost_basis = val_to
                tx.is_taxable_event = 1
                tx.ordinary_income_ils = cost_basis
                tx.cost_basis_ils = cost_basis
            else:
                # Swap or Buy: Use effective_swap_value
                cost_basis = effective_swap_value

            # Add fee to cost basis
            fee_ils = await self.get_ils_value(tx.fee_asset, tx.fee_amount, tx_date, rate)
            cost_basis += fee_ils

            ledger.add_lot(asset, qty, cost_basis, tx)

            if not is_sell and tx.type != TransactionType.deposit:
                tx.cost_basis_ils = cost_basis


        # 4. Handle Crypto Fee Disposals (CRITICAL ITA)
        if tx.fee_amount > 0 and tx.fee_asset and tx.fee_asset not in ['USD', 'ILS']:
            # This is a separate disposal of the fee asset
            fee_asset = tx.fee_asset
            fee_qty = tx.fee_amount
            fee_cost_basis = await ledger.consume_lots(fee_asset, fee_qty, tx)
            
            # Proceeds of fee disposal is its FMV
            fee_proceeds = await self.get_ils_value(fee_asset, fee_qty, tx_date, rate)
            
            # Fallback for missing cost basis
            if fee_cost_basis == 0 and fee_qty > 0:
                fee_cost_basis = fee_proceeds

            # Capital gain on the fee itself
            fee_gain = fee_proceeds - fee_cost_basis
            tx.capital_gain_ils += fee_gain
            tx.is_taxable_event = 1
            # Note: fee_cost_basis is already added to cost_basis_ils of the main transaction above

    async def get_kpi(self, db: AsyncSession, year: Optional[int] = None, tax_bracket: float = 0.25) -> Dict[str, Any]:
        stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
        accumulated_loss = 0.0
        report_year_gain = 0.0
        report_year_ordinary = 0.0
        report_year_trade_count = 0
        report_year_total_gain = 0.0 
        
        txs_by_year: Dict[int, List[Transaction]] = {}
        for tx in all_txs:
            if not tx.is_active: continue
            y = tx.timestamp.year
            if y not in txs_by_year: txs_by_year[y] = []
            txs_by_year[y].append(tx)
        
        for y in sorted(txs_by_year.keys()):
            y_gain = sum(t.capital_gain_ils or 0.0 for t in txs_by_year[y] if t.is_taxable_event)
            y_ordinary = sum(t.ordinary_income_ils or 0.0 for t in txs_by_year[y])
            
            if year and y < year:
                accumulated_loss += y_gain
                if accumulated_loss > 0: accumulated_loss = 0.0 
            elif year is None or y == year:
                if year == y or year is None:
                    report_year_trade_count += len([t for t in txs_by_year[y] if t.is_taxable_event and t.capital_gain_ils != 0])
                    report_year_total_gain += y_gain
                    report_year_ordinary += y_ordinary
                    
                    net_gain = y_gain + accumulated_loss
                    if net_gain < 0:
                        report_year_gain = 0.0
                        accumulated_loss = net_gain 
                    else:
                        report_year_gain = net_gain
                        accumulated_loss = 0.0
        
        all_tx_count = len([t for t in all_txs if (year is None or t.timestamp.year == year) and t.is_active])
        
        return {
            'year': year,
            'total_gain_ils': round(report_year_total_gain, 2),
            'ordinary_income_ils': round(report_year_ordinary, 2),
            'net_capital_gain_ils': round(report_year_gain, 2),
            'carried_forward_loss_ils': round(abs(min(0, accumulated_loss)), 2),
            'tax_bracket': tax_bracket,
            'estimated_tax_ils': round(max(0, (report_year_gain + report_year_ordinary) * tax_bracket), 2),
            'trade_count': report_year_trade_count,
            'total_transactions': all_tx_count,
            'high_frequency_warning': report_year_trade_count > 100
        }

    async def get_years(self, db: AsyncSession) -> List[int]:
        stmt = select(distinct(extract('year', Transaction.timestamp))).order_by(extract('year', Transaction.timestamp).desc())
        result = await db.execute(stmt)
        return [int(row[0]) for row in result.all() if row[0] is not None]

tax_engine = TaxEngine()
