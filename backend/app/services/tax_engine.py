from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, extract, distinct
from app.models.transaction import Transaction, TransactionType
from app.services.boi import boi_service
from app.services.price import price_service
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Set
import asyncio

class TaxEngine:
    async def calculate_taxes(self, db: AsyncSession):
        # 1. Fetch all transactions and sort by timestamp
        txs_stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(txs_stmt)
        txs = result.scalars().all()
        
        if not txs:
            return

        # 0. Transfer Reconciliation: Match withdrawals and deposits by time window and amount
        # Instead of strictly relying on tx_hash, we use a 24h window and amount heuristic
        withdrawals = [t for t in txs if t.type == TransactionType.withdrawal]
        deposits = [t for t in txs if t.type == TransactionType.deposit]
        
        reconciled_ids: Set[int] = set()
        deposit_to_withdrawal: Dict[int, int] = {}
        withdrawal_to_deposit: Dict[int, int] = {}
        
        for w in withdrawals:
            # Look for a matching deposit within 24h
            for d in deposits:
                if d.id in reconciled_ids:
                    continue
                
                time_diff = (d.timestamp - w.timestamp).total_seconds()
                if 0 <= time_diff <= 86400: # 0 to 24 hours
                    if d.asset_to == w.asset_from:
                        # Deposit amount must be <= withdrawal amount and >= 95% of it
                        if d.amount_to <= w.amount_from and d.amount_to >= (w.amount_from * 0.95):
                            w.is_taxable_event = 0
                            d.is_taxable_event = 0
                            w.linked_transaction_id = d.id
                            d.linked_transaction_id = w.id
                            w.category = "Transfer"
                            d.category = "Transfer"
                            
                            reconciled_ids.add(w.id)
                            reconciled_ids.add(d.id)
                            deposit_to_withdrawal[d.id] = w.id
                            withdrawal_to_deposit[w.id] = d.id
                            break

        # 1.1 Prefetch BOI rates for the entire range of transactions
        min_date = txs[0].timestamp.date()
        max_date = txs[-1].timestamp.date()
        await boi_service.prefetch_rates(min_date, max_date)

        # Phase 3.2: Avalanche Trade Merging (Pre-processing)
        merged_txs = []
        i = 0
        while i < len(txs):
            curr = txs[i]
            # Only merge trades (buy/sell), not transfers or earn
            if curr.id in reconciled_ids or curr.type not in [TransactionType.buy, TransactionType.sell]:
                merged_txs.append(curr)
                i += 1
                continue
            
            group = [curr]
            j = i + 1
            while j < len(txs):
                nxt = txs[j]
                time_diff = (nxt.timestamp - curr.timestamp).total_seconds()
                if (nxt.exchange == curr.exchange and 
                    nxt.type == curr.type and 
                    nxt.asset_from == curr.asset_from and 
                    nxt.asset_to == curr.asset_to and 
                    time_diff <= 5 and
                    nxt.id not in reconciled_ids):
                    group.append(nxt)
                    j += 1
                else:
                    break
            
            if len(group) > 1:
                # Merge into curr (the first in the group)
                # Note: amount fields are updated, original transactions (except curr) are NOT added to merged_txs
                for k in range(1, len(group)):
                    g = group[k]
                    curr.amount_from += g.amount_from
                    curr.amount_to += g.amount_to
                    curr.fee_amount += g.fee_amount
                
                curr.raw_data = (curr.raw_data or "") + f" | Merged {len(group)} trades. IDs: {[t.id for t in group]}"
                i = j
            else:
                i += 1
            merged_txs.append(curr)
        
        txs = merged_txs
        # 2. Group by asset for FIFO matching
        inventory: Dict[str, List[Dict[str, Any]]] = {}
        tx_map = {tx.id: tx for tx in txs}
        
        # Cache for ILS rates and reconciled cost basis
        rate_cache: Dict[date, float] = {}
        reconciled_cost_basis: Dict[int, float] = {} # withdrawal_id -> cost_basis_ils

        async def get_ils_value_v2(asset: str, amount: float, tx_date: date, usd_ils_rate: float) -> Optional[float]:
            if not asset or not amount:
                return 0.0
            if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
                return amount * usd_ils_rate
            if asset == 'ILS':
                return amount
            
            # For other assets, use price_service
            usd_price = await price_service.get_historical_price(asset, tx_date)
            if usd_price is not None:
                return amount * usd_price * usd_ils_rate
            
            return None


        for tx in txs:
            # Reset calculation fields
            tx.capital_gain_ils = 0.0
            tx.cost_basis_ils = 0.0
            tx.ordinary_income_ils = 0.0
            tx.purchase_date = None
            tx.is_taxable_event = 0

            # Fetch ILS rate for the transaction date
            rate_date = tx.timestamp.date()
            if rate_date not in rate_cache:
                rate_cache[rate_date] = await boi_service.get_usd_ils_rate(rate_date)
            
            rate = rate_cache[rate_date]
            tx.ils_rate_date = rate_date
            tx.ils_exchange_rate = rate
            
            # Identify if this is a buy, sell, or both
            is_buy = (tx.asset_to and tx.amount_to > 0)
            is_sell = (tx.asset_from and tx.amount_from > 0)
            
            # Special case: Kraken Futures PnL (realized pnl and funding)
            if tx.exchange == 'krakenfutures' and tx.type in [TransactionType.fee, TransactionType.earn] and not tx.asset_from == 'USD':
                 # If it's not USD, it might be a fee in another asset or actual PnL in BTC
                 pass # Let standard logic handle it if it's an asset disposal

            if tx.exchange == 'krakenfutures' and tx.type in [TransactionType.fee, TransactionType.earn] and (tx.asset_from in ['USD', 'BTC', 'XBT'] or tx.asset_to in ['USD', 'BTC', 'XBT']):
                pnl_amount = tx.amount_to if tx.amount_to > 0 else -tx.amount_from
                # Realized PnL is direct capital gain/loss
                tx.capital_gain_ils = pnl_amount * rate
                tx.is_taxable_event = 1
                tx.cost_basis_ils = 0.0
                # Still need to add to inventory if it's a profit in BTC? 
                # Kraken Futures realized_pnl is usually added to balance.
                if tx.amount_to > 0:
                    asset = tx.asset_to
                    if asset not in inventory: inventory[asset] = []
                    inventory[asset].append({'amount': tx.amount_to, 'cost_basis_ils': tx.capital_gain_ils, 'timestamp': tx.timestamp})
                db.add(tx)
                continue

            # Special case: deposits and withdrawals are not typically taxable sales/buys 
            # unless they are unreconciled (handled below)
            
            # --- PROCESS SELL SIDE FIRST ---
            # Reconciled withdrawals should NOT consume inventory as they are transfers to self
            if is_sell and tx.id not in reconciled_ids:
                asset = tx.asset_from
                qty_to_match = tx.amount_from
                total_cost_basis_ils = 0.0

                
                if asset in inventory and inventory[asset]:
                    # Record purchase date from the earliest lot
                    tx.purchase_date = inventory[asset][0]['timestamp'].date()
                    
                    while qty_to_match > 0 and inventory[asset]:
                        oldest_buy = inventory[asset][0]
                        if oldest_buy['amount'] <= qty_to_match:
                            matched_qty = oldest_buy['amount']
                            total_cost_basis_ils += oldest_buy['cost_basis_ils']
                            inventory[asset].pop(0)
                        else:
                            matched_qty = qty_to_match
                            unit_cost = oldest_buy['cost_basis_ils'] / oldest_buy['amount']
                            matched_cost = unit_cost * matched_qty
                            total_cost_basis_ils += matched_cost
                            oldest_buy['amount'] -= matched_qty
                            oldest_buy['cost_basis_ils'] -= matched_cost
                        
                if qty_to_match > 1e-8:
                    # Short sale detected
                    short_qty = qty_to_match
                    tx.is_issue = True
                    tx.issue_notes = f"Missing cost basis for {round(qty_to_match, 8)} {asset}. Assuming zero cost basis for this amount."
                
                # Proceeds calculation

                if tx.type == TransactionType.withdrawal:
                    # An unreconciled withdrawal is treated as a disposal at market price
                    proceeds_ils = await get_ils_value_v2(asset, tx.amount_from, rate_date, rate)
                else:
                    # Prefer FMV of the asset SOLD as proceeds (standard ITA practice)
                    proceeds_ils = await get_ils_value_v2(tx.asset_from, tx.amount_from, rate_date, rate)
                    
                    if proceeds_ils is None and tx.asset_to:
                         # Fallback to FMV of asset received
                         proceeds_ils = await get_ils_value_v2(tx.asset_to, tx.amount_to, rate_date, rate)
                
                if proceeds_ils is None:
                    proceeds_ils = total_cost_basis_ils if total_cost_basis_ils > 0 else 0.0

                
                # Fee handling
                fee_ils = await get_ils_value_v2(tx.fee_asset, tx.fee_amount, rate_date, rate) or 0.0
                proceeds_ils -= fee_ils
                
                # Taxation logic
                if tx.id in reconciled_ids:
                    tx.is_taxable_event = 0
                    if tx.type == TransactionType.withdrawal:
                        reconciled_cost_basis[tx.id] = total_cost_basis_ils
                elif tx.type == TransactionType.withdrawal:
                    tx.is_taxable_event = 0 # Unreconciled withdrawals are usually not taxable until proven otherwise in ITA
                else:
                    # If it's a swap (has both asset_from and asset_to) or a direct sell
                    # It's taxable unless the asset_from is a fiat currency OR it's a same-asset swap
                    if tx.asset_from in ['USD', 'ILS', 'USDT', 'USDC', 'DAI', 'BUSD'] or tx.asset_from == tx.asset_to:
                        tx.is_taxable_event = 0
                    else:
                        tx.is_taxable_event = 1

                
                tx.cost_basis_ils = total_cost_basis_ils
                tx.capital_gain_ils = proceeds_ils - total_cost_basis_ils if tx.is_taxable_event else 0.0
            
            # --- PROCESS BUY SIDE ---
            # Reconciled deposits should NOT add new inventory as the original lots are still in inventory
            if is_buy and tx.id not in reconciled_ids:
                asset = tx.asset_to

                if tx.type == TransactionType.earn:
                    cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate) or 0.0
                elif tx.type == TransactionType.deposit:
                    if tx.id in reconciled_ids:
                        w_id = deposit_to_withdrawal.get(tx.id)
                        w_cost = reconciled_cost_basis.get(w_id)
                        if w_cost is not None:
                            w_tx = tx_map.get(w_id)
                            if w_tx and w_tx.amount_from > 0:
                                cost_ils = w_cost * (tx.amount_to / w_tx.amount_from)
                            else:
                                cost_ils = w_cost
                        else:
                            cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate) or 0.0
                    else:
                        cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate) or 0.0
                elif tx.asset_from:
                    # It's a swap or buy with fiat
                    # The cost basis is the value of what we gave up (which matches proceeds_ils before fee deduction)
                    # We use the same logic as proceeds calculation (prefer FMV of asset_from)
                    cost_ils = await get_ils_value_v2(tx.asset_from, tx.amount_from, rate_date, rate)
                    if cost_ils is None and asset:
                        cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate)

                else:
                    # Simple buy (shouldn't happen without asset_from, but for safety)
                    cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate)
                    
                if cost_ils is None:
                    # Fallback to market price
                    cost_ils = await get_ils_value_v2(asset, tx.amount_to, rate_date, rate) or 0.0
                
                # Fee handling
                fee_ils = await get_ils_value_v2(tx.fee_asset, tx.fee_amount, rate_date, rate) or 0.0
                cost_ils += fee_ils
                
                if asset not in inventory:
                    inventory[asset] = []
                
                inventory[asset].append({
                    'amount': tx.amount_to,
                    'cost_basis_ils': cost_ils,
                    'timestamp': tx.timestamp
                })

                
                if tx.type == TransactionType.earn:
                    tx.is_taxable_event = 1
                    tx.ordinary_income_ils = cost_ils
                    tx.capital_gain_ils = 0.0
                    tx.cost_basis_ils = cost_ils
                elif not is_sell and tx.type != TransactionType.deposit:
                    tx.is_taxable_event = 0
                    tx.cost_basis_ils = cost_ils
                    tx.capital_gain_ils = 0.0
                elif tx.id in reconciled_ids:
                    tx.is_taxable_event = 0
                    tx.cost_basis_ils = cost_ils
                    tx.capital_gain_ils = 0.0

            
            db.add(tx)
        
        await db.commit()

    async def get_kpi(self, db: AsyncSession, year: Optional[int] = None, tax_bracket: float = 0.25) -> Dict[str, Any]:
        # 1. Fetch all transactions for the range
        stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        if year:
            # We need historical transactions too for loss harvesting, but let's filter carefully
            pass
        
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
        # 2. Loss Harvesting & Fee Aggregation
        accumulated_loss = 0.0
        
        # Group by year
        txs_by_year: Dict[int, List[Transaction]] = {}
        for tx in all_txs:
            y = tx.timestamp.year
            if y not in txs_by_year:
                txs_by_year[y] = []
            txs_by_year[y].append(tx)
        
        sorted_years = sorted(txs_by_year.keys())
        
        report_year_gain = 0.0
        report_year_ordinary = 0.0
        report_year_trade_count = 0
        report_year_total_gain = 0.0 
        report_year_total_fees = 0.0
        
        async def get_fee_ils(tx: Transaction) -> float:
            if not tx.fee_amount: return 0.0
            # We already have exchange rate in tx.ils_exchange_rate if calculate_taxes was run
            rate = tx.ils_exchange_rate or 3.65
            if tx.fee_asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
                return tx.fee_amount * rate
            if tx.fee_asset == 'ILS':
                return tx.fee_amount
            
            usd_price = await price_service.get_historical_price(tx.fee_asset, tx.timestamp.date())
            if usd_price:
                return tx.fee_amount * usd_price * rate
            return 0.0

        for y in sorted_years:
            # Calculate standalone fees (not already accounted in buy/sell capital gains)
            y_fees = 0.0
            for tx in txs_by_year[y]:
                if tx.type not in [TransactionType.buy, TransactionType.sell, TransactionType.dust, TransactionType.convert]:
                    y_fees += await get_fee_ils(tx)
            
            y_taxable_txs = [t for t in txs_by_year[y] if t.is_taxable_event]
            y_gain = sum(t.capital_gain_ils or 0.0 for t in y_taxable_txs)
            
            # y_gain already has sell fees deducted and buy fees in cost basis
            y_net_gain = y_gain - y_fees
            
            y_ordinary = sum(t.ordinary_income_ils or 0.0 for t in txs_by_year[y])
            
            if year and y < year:
                # Historical year: update accumulated loss
                accumulated_loss += y_net_gain
                if accumulated_loss > 0:
                    accumulated_loss = 0.0 
            elif year is None or y == year:
                # Current reporting year
                if year == y or year is None:
                    report_year_trade_count += len([t for t in y_taxable_txs if t.capital_gain_ils != 0])
                    report_year_total_gain += y_gain
                    report_year_total_fees += y_fees
                    report_year_ordinary += y_ordinary
                    
                    # Apply accumulated loss to current year
                    net_gain = y_net_gain + accumulated_loss
                    if net_gain < 0:
                        report_year_gain = 0.0
                        accumulated_loss = net_gain 
                    else:
                        report_year_gain = net_gain
                        accumulated_loss = 0.0
        
        all_tx_count = len([t for t in all_txs if year is None or t.timestamp.year == year])
        is_business = report_year_trade_count > 100 
        
        return {
            'year': year,
            'total_gain_ils': round(report_year_total_gain, 2),
            'total_fees_ils': round(report_year_total_fees, 2),
            'ordinary_income_ils': round(report_year_ordinary, 2),
            'net_capital_gain_ils': round(report_year_gain, 2),
            'carried_forward_loss_ils': round(abs(min(0, accumulated_loss)), 2),
            'tax_bracket': tax_bracket,
            'estimated_tax_ils': round(max(0, (report_year_gain + report_year_ordinary) * tax_bracket), 2),
            'trade_count': report_year_trade_count,
            'total_transactions': all_tx_count,
            'is_business_threshold_crossed': is_business
        }


    async def get_years(self, db: AsyncSession) -> List[int]:
        stmt = select(distinct(extract('year', Transaction.timestamp))).order_by(extract('year', Transaction.timestamp).desc())
        result = await db.execute(stmt)
        return [int(row[0]) for row in result.all() if row[0] is not None]

tax_engine = TaxEngine()
