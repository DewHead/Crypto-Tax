import csv
import io
import os
import re
import zipfile
import shutil
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from app.models.transaction import Transaction, TransactionType
from app.db.session import AsyncSessionLocal
from sqlalchemy import select, delete

class CSVIngestionService:
    def __init__(self, temp_dir: str = "temp_csv"):
        self.temp_dir = temp_dir
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    async def process_zip(self, zip_path: str):
        """
        Extracts a ZIP file and processes all CSVs inside.
        Supports Binance Statement (Export) and Trade History.
        """
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.temp_dir)
        
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_path = os.path.join(root, file)
                    if "Statement" in file:
                        await self.process_binance_statement(csv_path)
                    else:
                        await self.process_binance_trades(csv_path)
                    os.remove(csv_path)

    async def process_binance_statement(self, csv_path: str):
        """
        Parses Binance Statement CSV (Account History).
        Columns: UTC_Time, Account, Operation, Asset, Change, Remark
        """
        async with AsyncSessionLocal() as db:
            with open(csv_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                # Binance statements are sorted by time, we might need to group simultaneous operations
                # (e.g. Sell BTC for USDT appears as two rows: -BTC and +USDT)
                rows = list(reader)
                i = 0
                while i < len(rows):
                    row = rows[i]
                    ts_str = row['UTC_Time'].strip()
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    
                    # Look for matching pair (same second, same remark)
                    tx_type = self._map_operation(row['Operation'])
                    
                    # Create a unique hash for idempotency
                    tx_hash = f"binance_stmt_{ts.timestamp()}_{i}_{row['Asset']}"
                    
                    stmt = select(Transaction).filter(Transaction.tx_hash == tx_hash)
                    existing = (await db.execute(stmt)).scalars().first()
                    if not existing:
                        tx = Transaction(
                            exchange='binance',
                            tx_hash=tx_hash,
                            timestamp=ts,
                            type=tx_type,
                            source='csv',
                            category=row['Operation']
                        )
                        
                        change = float(row['Change'].replace(',', ''))
                        if change < 0:
                            tx.asset_from = row['Asset']
                            tx.amount_from = abs(change)
                        else:
                            tx.asset_to = row['Asset']
                            tx.amount_to = change
                            
                        db.add(tx)
                    i += 1
            await db.commit()

    async def process_binance_trades(self, csv_path: str):
        """
        Parses Binance Trade History CSV.
        Columns: Date(UTC), Market, Type, Price, Amount, Total, Fee, Fee Coin
        """
        async with AsyncSessionLocal() as db:
            with open(csv_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    ts_str = row['Date(UTC)'].strip()
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    
                    market = row['Market'] # e.g. BTCUSDT
                    # Extract assets (crude but usually works for Binance)
                    # USDT, BUSD, USDC, BTC, ETH, BNB are common bases
                    base_assets = ['USDT', 'BUSD', 'USDC', 'BTC', 'ETH', 'BNB', 'EUR', 'TRY']
                    asset_from, asset_to = None, None
                    for ba in base_assets:
                        if market.endswith(ba):
                            asset_to_symbol = market[:-len(ba)]
                            asset_from_symbol = ba
                            break
                    else:
                        # Fallback
                        asset_to_symbol = market[:3]
                        asset_from_symbol = market[3:]

                    side = row['Type'].lower() # buy or sell
                    amount = float(row['Amount'].replace(',', ''))
                    total = float(row['Total'].replace(',', ''))
                    fee = float(row['Fee'].replace(',', ''))
                    fee_coin = row['Fee Coin']

                    tx_hash = f"binance_trade_{ts.timestamp()}_{i}_{market}"
                    
                    stmt = select(Transaction).filter(Transaction.tx_hash == tx_hash)
                    existing = (await db.execute(stmt)).scalars().first()
                    if not existing:
                        tx = Transaction(
                            exchange='binance',
                            tx_hash=tx_hash,
                            timestamp=ts,
                            source='csv',
                            category='Trade'
                        )
                        
                        if side == 'buy':
                            tx.type = TransactionType.buy
                            tx.asset_to = asset_to_symbol
                            tx.amount_to = amount
                            tx.asset_from = asset_from_symbol
                            tx.amount_from = total
                        else:
                            tx.type = TransactionType.sell
                            tx.asset_from = asset_to_symbol
                            tx.amount_from = amount
                            tx.asset_to = asset_from_symbol
                            tx.amount_to = total
                        
                        tx.fee_amount = fee
                        tx.fee_asset = fee_coin
                        db.add(tx)
            await db.commit()

    def _map_operation(self, op: str) -> TransactionType:
        op = op.lower()
        if 'buy' in op: return TransactionType.buy
        if 'sell' in op: return TransactionType.sell
        if 'deposit' in op: return TransactionType.deposit
        if 'withdraw' in op: return TransactionType.withdrawal
        if 'fee' in op: return TransactionType.fee
        if 'distribution' in op or 'staking' in op or 'interest' in op: return TransactionType.earn
        return TransactionType.earn # Default for other income-like ops

csv_ingestion_service = CSVIngestionService()
