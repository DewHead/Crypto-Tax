import csv
import os
import re
import zipfile
import shutil
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from app.models.transaction import Transaction, TransactionType
from app.db.session import AsyncSessionLocal
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

class CSVIngestionService:
    async def process_zip(self, zip_path: str, db: Optional[AsyncSession] = None, tx_buffer: List[Transaction] = None, flush_callback = None):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.csv'):
                    with zip_ref.open(file_name) as f:
                        content = f.read().decode('utf-8-sig')
                        lines = content.splitlines()
                        if "UTC_Time" in lines[0]:
                            await self._process_binance_statements(lines, db=db, tx_buffer=tx_buffer, flush_callback=flush_callback)
                        elif "Date(UTC)" in lines[0]:
                            await self._process_binance_trades(lines, db=db, tx_buffer=tx_buffer, flush_callback=flush_callback)

    async def _process_binance_statements(self, lines: List[str], db: Optional[AsyncSession] = None, tx_buffer: List[Transaction] = None, flush_callback = None):
        reader = csv.DictReader(lines)
        
        # Group rows by timestamp (heuristic for trades)
        groups = []
        for row in reader:
            ts_str = row['UTC_Time'].strip()
            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            
            added = False
            if groups:
                last_group = groups[-1]
                last_ts_str = last_group[0]['UTC_Time'].strip()
                last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                if abs((ts - last_ts).total_seconds()) <= 2:
                    last_group.append(row)
                    added = True

            if not added:
                groups.append([row])

        income_ops = [
            'Distribution', 'Savings Interest', 'Simple Earn Flexible Interest', 'Simple Earn Locked Interest',
            'Commission History', 'Commission Rebate', 'Staking Rewards', 'Airdrop Assets', 'Asset - Transfer'
        ]

        async def add_tx(tx):
            if tx_buffer is not None:
                tx_buffer.append(tx)
                if len(tx_buffer) >= 100 and flush_callback:
                    await flush_callback()
            else:
                await self._save_transaction(tx, db=db)

        for group_rows in groups:
            ts_str = group_rows[0]['UTC_Time'].strip()
            timestamp = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            ts = ts_str # for tx_hash
            rows = group_rows

            # Identify components
            deposits = [r for r in rows if r['Operation'].strip() == 'Deposit']
            withdrawals = [r for r in rows if r['Operation'].strip() == 'Withdraw']
            income = [r for r in rows if r['Operation'].strip() in income_ops]
            dust = [r for r in rows if r['Operation'].strip() == 'Small Assets Exchange BNB']
            fees = [r for r in rows if 'Fee' in r['Operation'].strip() or r['Operation'].strip() == 'Transaction Fee']

            # Remaining rows that might be parts of a trade
            trade_rows = [r for r in rows if r not in deposits and r not in withdrawals and r not in income and r not in dust and r not in fees]

            # 1. Process simple deposits
            for r in deposits:
                await add_tx(Transaction(
                    exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_dep",
                    timestamp=timestamp, type=TransactionType.deposit, asset_to=r['Coin'], amount_to=float(r['Change']), source='csv'
                ))

            # 2. Process simple withdrawals
            for r in withdrawals:
                await add_tx(Transaction(
                    exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_wd",
                    timestamp=timestamp, type=TransactionType.withdrawal, asset_from=r['Coin'], amount_from=abs(float(r['Change'])), source='csv'
                ))

            # 3. Process income
            for r in income:
                val = float(r['Change'])
                if val >= 0:
                    await add_tx(Transaction(
                        exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_earn",
                        timestamp=timestamp, type=TransactionType.earn, asset_to=r['Coin'], amount_to=val, source='csv'
                    ))
                else:
                    await add_tx(Transaction(
                        exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_wd",
                        timestamp=timestamp, type=TransactionType.withdrawal, asset_from=r['Coin'], amount_from=abs(val), source='csv'
                    ))

            # 4. Process Dust
            if dust:
                buy_bnb = [r for r in dust if float(r['Change']) > 0]
                sell_coins = [r for r in dust if float(r['Change']) < 0]
                if buy_bnb and sell_coins:
                    asset_to = buy_bnb[0]['Coin']
                    total_amount_to = float(buy_bnb[0]['Change'])
                    total_sold_units = sum(abs(float(s['Change'])) for s in sell_coins)
                    for s in sell_coins:
                        asset_from = s['Coin']
                        amount_from = abs(float(s['Change']))
                        proportion = amount_from / total_sold_units if total_sold_units > 0 else 0
                        await add_tx(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{asset_from}_dust",
                            timestamp=timestamp, type=TransactionType.dust,
                            asset_from=asset_from, amount_from=amount_from,
                            asset_to=asset_to, amount_to=total_amount_to * proportion, source='csv'
                        ))

            # 5. Process Trades / Swaps (Remaining rows + Fees)
            if trade_rows:
                buy_side = [r for r in trade_rows if float(r['Change']) > 0]
                sell_side = [r for r in trade_rows if float(r['Change']) < 0]

                f_asset = None; f_amount = 0.0
                if fees:
                    f_asset = fees[0]['Coin']
                    f_amount = sum(abs(float(f['Change'])) for f in fees)

                if buy_side and sell_side:
                    total_sell_units = sum(abs(float(s['Change'])) for s in sell_side)
                    total_buy_units = sum(abs(float(b['Change'])) for b in buy_side)
                    for s in sell_side:
                        a_from = s['Coin']; q_from = abs(float(s['Change']))
                        a_to = buy_side[0]['Coin'] # Heuristic: assume same asset for all buy rows in a group
                        q_to = total_buy_units * (q_from / total_sell_units) if total_sell_units > 0 else 0
                        
                        # Reconstruct gross amount if fee was deducted from the received asset
                        # Binance statements usually show net change.
                        q_to_gross = q_to
                        if f_asset == a_to:
                            # If we bought BTC and the fee was also in BTC, q_to is likely already net.
                            # We add the proportional fee back to get the gross amount for proper FIFO cost basis.
                            prop_fee = f_amount * (q_from / total_sell_units)
                            q_to_gross += prop_fee

                        await add_tx(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{a_from}_{a_to}_trd_{group_rows.index(s)}",
                            timestamp=timestamp, type=TransactionType.buy if a_from in ['USD', 'EUR', 'USDT'] else TransactionType.sell,
                            asset_from=a_from, amount_from=q_from,
                            asset_to=a_to, amount_to=q_to_gross,
                            fee_asset=f_asset, fee_amount=f_amount * (q_from / total_sell_units) if f_amount and total_sell_units > 0 else 0,
                            source='csv'
                        ))

                else:
                    for r in buy_side:
                        # Reconstruct gross amount if fee was deducted
                        q_to = float(r['Change'])
                        if f_asset == r['Coin']:
                            q_to += f_amount / len(buy_side) # Rough split

                        await add_tx(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_buy_{group_rows.index(r)}",
                            timestamp=timestamp, type=TransactionType.buy, asset_to=r['Coin'], amount_to=q_to,
                            fee_asset=f_asset, fee_amount=f_amount / len(buy_side) if f_amount else 0, source='csv'
                        ))
                    for r in sell_side:
                        await add_tx(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_sell_{group_rows.index(r)}",
                            timestamp=timestamp, type=TransactionType.sell, asset_from=r['Coin'], amount_from=abs(float(r['Change'])),
                            fee_asset=f_asset, fee_amount=f_amount / len(sell_side) if f_amount else 0, source='csv'
                        ))

    async def _process_binance_trades(self, lines: List[str], db: Optional[AsyncSession] = None, tx_buffer: List[Transaction] = None, flush_callback = None):
        reader = csv.DictReader(lines)
        async def add_tx(tx):
            if tx_buffer is not None:
                tx_buffer.append(tx)
                if len(tx_buffer) >= 100 and flush_callback:
                    await flush_callback()
            else:
                await self._save_transaction(tx, db=db)

        for row in reader:
            timestamp = datetime.strptime(row['Date(UTC)'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            side = row['Side'].upper(); pair = row['Pair']
            def parse_val(s):
                match = re.match(r"([0-9\.]+)\s*([A-Z]+)", s)
                if match: return float(match.group(1)), match.group(2)
                try: return float(s), None
                except: return 0.0, None
            exec_amt, base_asset = parse_val(row['Executed'])
            total_amt, quote_asset = parse_val(row['Amount'])
            fee_amt, fee_asset = parse_val(row['Fee'])
            tx_hash = f"csv_trade_{row['Date(UTC)']}_{pair}_{row['Executed']}"
            if side == 'BUY':
                tx = Transaction(
                    exchange='binance', tx_hash=tx_hash, timestamp=timestamp, type=TransactionType.buy,
                    asset_from=quote_asset, amount_from=total_amt, asset_to=base_asset, amount_to=exec_amt,
                    fee_asset=fee_asset, fee_amount=fee_amt, source='csv', is_taxable_event=0
                )
            else:
                tx = Transaction(
                    exchange='binance', tx_hash=tx_hash, timestamp=timestamp, type=TransactionType.sell,
                    asset_from=base_asset, amount_from=exec_amt, asset_to=quote_asset, amount_to=total_amt,
                    fee_asset=fee_asset, fee_amount=fee_amt, source='csv', is_taxable_event=1
                )
            await add_tx(tx)

    async def _save_transaction(self, tx: Transaction, db: Optional[AsyncSession] = None):
        if db:
            await self._save_to_db(tx, db)
        else:
            async with AsyncSessionLocal() as session:
                await self._save_to_db(tx, session)

    async def _save_to_db(self, tx: Transaction, db: AsyncSession, commit: bool = True):
        try:
            if tx.tx_hash:
                stmt = select(Transaction).filter(Transaction.tx_hash == tx.tx_hash, Transaction.exchange == tx.exchange)
                if (await db.execute(stmt)).scalars().first(): return

            t_start = tx.timestamp - timedelta(seconds=1)
            t_end = tx.timestamp + timedelta(seconds=1)
            stmt = select(Transaction).filter(
                Transaction.exchange == tx.exchange, Transaction.asset_from == tx.asset_from, Transaction.amount_from == tx.amount_from,
                Transaction.asset_to == tx.asset_to, Transaction.amount_to == tx.amount_to, Transaction.timestamp.between(t_start, t_end)
            )
            existing = (await db.execute(stmt)).scalars().first()
            if not existing:
                db.add(tx)
                if commit: await db.commit()
            elif existing.source == 'csv' and tx.source == 'api':
                existing.tx_hash = tx.tx_hash; existing.source = 'api'
                if commit: await db.commit()
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Error saving transaction to DB: {e}")
            if commit: await db.rollback()


csv_ingestion_service = CSVIngestionService()
