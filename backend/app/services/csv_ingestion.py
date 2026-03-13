import csv
import zipfile
import io
import re
from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.transaction import Transaction, TransactionType
from app.db.session import AsyncSessionLocal
from sqlalchemy import select
import os

class CSVIngestionService:
    async def process_zip(self, zip_path: str):
        if not os.path.exists(zip_path): return
        with zipfile.ZipFile(zip_path, 'r') as z:
            for filename in z.namelist():
                if filename.endswith('.csv'):
                    with z.open(filename) as f:
                        content = f.read().decode('utf-8-sig')
                        lines = content.splitlines()
                        if not lines: continue
                        header = lines[0]
                        if '"User_ID","UTC_Time","Account","Operation","Coin","Change","Remark"' in header:
                            print(f"Detected Binance Statements CSV: {filename}")
                            await self._process_binance_statements(lines)
                        elif '"Date(UTC)","Pair","Side","Price","Executed","Amount","Fee"' in header:
                            print(f"Detected Binance Trades CSV: {filename}")
                            await self._process_binance_trades(lines)

    async def _process_binance_statements(self, lines: List[str], db: Optional[AsyncSession] = None):
        reader = csv.DictReader(lines)
        # Group by timestamp (with 2s fuzzy window)
        groups: List[List[dict]] = []
        rows_sorted = sorted(list(reader), key=lambda x: x['UTC_Time'])

        for row in rows_sorted:
            ts_str = row['UTC_Time'].strip()
            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')

            added = False
            if groups:
                last_group = groups[-1]
                last_ts_str = last_group[0]['UTC_Time'].strip()
                last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
                if abs((ts - last_ts).total_seconds()) <= 2:
                    last_group.append(row)
                    added = True

            if not added:
                groups.append([row])

        income_ops = [
            'Distribution', 'Savings Interest', 'Simple Earn Flexible Interest', 'Simple Earn Locked Interest',
            'Commission History', 'Commission Rebate', 'Staking Rewards', 'Airdrop Assets'
        ]

        for group_rows in groups:
            ts_str = group_rows[0]['UTC_Time'].strip()
            timestamp = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
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
                await self._save_transaction(Transaction(
                    exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_dep",
                    timestamp=timestamp, type=TransactionType.deposit, asset_to=r['Coin'], amount_to=float(r['Change']), source='csv'
                ), db=db)

            # 2. Process simple withdrawals
            for r in withdrawals:
                await self._save_transaction(Transaction(
                    exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_wd",
                    timestamp=timestamp, type=TransactionType.withdrawal, asset_from=r['Coin'], amount_from=abs(float(r['Change'])), source='csv'
                ), db=db)

            # 3. Process income
            for r in income:
                await self._save_transaction(Transaction(
                    exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_earn",
                    timestamp=timestamp, type=TransactionType.earn, asset_to=r['Coin'], amount_to=float(r['Change']), source='csv'
                ), db=db)

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
                        await self._save_transaction(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{asset_from}_dust",
                            timestamp=timestamp, type=TransactionType.dust,
                            asset_from=asset_from, amount_from=amount_from,
                            asset_to=asset_to, amount_to=total_amount_to * proportion, source='csv'
                        ), db=db)

            # 5. Process Trades / Swaps (Remaining rows + Fees)
            if trade_rows:
                buy_side = [r for r in trade_rows if float(r['Change']) > 0]
                sell_side = [r for r in trade_rows if float(r['Change']) < 0]

                f_asset = None; f_amount = 0.0
                if fees:
                    f_asset = fees[0]['Coin']
                    f_amount = sum(abs(float(f['Change'])) for f in fees)

                if buy_side and sell_side:
                    # Multi-component trade
                    total_sell_units = sum(abs(float(s['Change'])) for s in sell_side)
                    for s in sell_side:
                        a_from = s['Coin']; q_from = abs(float(s['Change']))
                        # Allocate first buy asset received proportionally
                        a_to = buy_side[0]['Coin']
                        q_to = float(buy_side[0]['Change']) * (q_from / total_sell_units) if total_sell_units > 0 else 0
                        
                        await self._save_transaction(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{a_from}_{a_to}_trd",
                            timestamp=timestamp, type=TransactionType.buy,
                            asset_from=a_from, amount_from=q_from, asset_to=a_to, amount_to=q_to,
                            fee_asset=f_asset, fee_amount=f_amount * (q_from / total_sell_units) if f_amount and total_sell_units > 0 else 0,
                            source='csv'
                        ), db=db)

                else:
                    # Single side rows
                    for r in buy_side:
                        await self._save_transaction(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_buy",
                            timestamp=timestamp, type=TransactionType.buy, asset_to=r['Coin'], amount_to=float(r['Change']), source='csv'
                        ), db=db)
                    for r in sell_side:
                        await self._save_transaction(Transaction(
                            exchange='binance', tx_hash=f"csv_stmt_{ts}_{r['Coin']}_sell",
                            timestamp=timestamp, type=TransactionType.sell, asset_from=r['Coin'], amount_from=abs(float(r['Change'])), source='csv'
                        ), db=db)




    async def _process_binance_trades(self, lines: List[str]):
        reader = csv.DictReader(lines)
        for row in reader:
            timestamp = datetime.strptime(row['Date(UTC)'], '%Y-%m-%d %H:%M:%S')
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
            await self._save_transaction(tx)

    async def _save_transaction(self, tx: Transaction, db: Optional[AsyncSession] = None):
        if db:
            await self._save_to_db(tx, db)
        else:
            async with AsyncSessionLocal() as session:
                await self._save_to_db(tx, session)

    async def _save_to_db(self, tx: Transaction, db: AsyncSession):
        try:
            # 1. Exact tx_hash check
            if tx.tx_hash:
                stmt = select(Transaction).filter(Transaction.tx_hash == tx.tx_hash, Transaction.exchange == tx.exchange)
                if (await db.execute(stmt)).scalars().first(): return

            # 2. Fingerprint check
            t_start = tx.timestamp - timedelta(seconds=1)
            t_end = tx.timestamp + timedelta(seconds=1)
            stmt = select(Transaction).filter(
                Transaction.exchange == tx.exchange, Transaction.asset_from == tx.asset_from, Transaction.amount_from == tx.amount_from,
                Transaction.asset_to == tx.asset_to, Transaction.amount_to == tx.amount_to, Transaction.timestamp.between(t_start, t_end)
            )
            existing = (await db.execute(stmt)).scalars().first()
            if not existing:
                db.add(tx); await db.commit()
            elif existing.source == 'csv' and tx.source == 'api':
                existing.tx_hash = tx.tx_hash; existing.source = 'api'; await db.commit()
        except: await db.rollback()


csv_ingestion_service = CSVIngestionService()
