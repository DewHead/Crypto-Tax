import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.transaction import Transaction, TransactionType
from app.db.session import AsyncSessionLocal
from sqlalchemy import select
from datetime import timedelta
import os

class BloxTaxIngestionService:
    TYPE_MAPPING = {
        'המרה': TransactionType.convert,
        'הפקדה': TransactionType.deposit,
        'משיכה': TransactionType.withdrawal,
        'מתנה': TransactionType.earn,
        'תקבול': TransactionType.earn,
        'סטייקינג': TransactionType.earn,
        'רווח הון': TransactionType.earn,
        'הפסד הון': TransactionType.sell,
        'העברה ידועה': TransactionType.deposit,
        'עמלה': TransactionType.fee,
        'דיבידנד': TransactionType.earn,
        'הוצאה': TransactionType.withdrawal,
    }

    async def process_excel(self, file_path: str, db: Optional[AsyncSession] = None):
        if not os.path.exists(file_path): 
            print(f"File not found: {file_path}")
            return

        try:
            df = pd.read_excel(file_path, header=None)
            if df.empty: return

            # Detection logic:
            # Format 1: Col 1 has Hebrew types like המרה, Col 2 is date, Col 3 is exchange (string)
            # Format 2: Col 1 is 'רווח הון' etc, Col 2 is date, Col 3 is Asset name, Col 4 is amount
            
            # Check a few rows to see what Col 3 looks like
            col3_is_date = False
            try:
                pd.to_datetime(df.iloc[0, 2])
                # If Col 2 is a date, check Col 3
            except: pass

            first_type = str(df.iloc[0, 1]).strip()
            
            if first_type in ['המרה', 'הפקדה', 'משיכה', 'מתנה', 'תקבול'] and len(df.columns) >= 10:
                 # In Format 1, Col 3 is often "Binance", "On Chain", "Income"
                 print(f"Detected BloxTax Format 1: {file_path}")
                 await self._process_format_1(df, db)
            else:
                 print(f"Detected BloxTax Format 2: {file_path}")
                 await self._process_format_2(df, db)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    async def _process_format_1(self, df: pd.DataFrame, db: Optional[AsyncSession] = None):
        for _, row in df.iterrows():
            try:
                raw_type = str(row[1]).strip()
                if raw_type == 'nan': continue
                
                ts_val = row[2]
                if isinstance(ts_val, str):
                    timestamp_str = ts_val.replace('⠀', ' ').strip()
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M')
                    except:
                        timestamp = pd.to_datetime(ts_val)
                else:
                    timestamp = pd.to_datetime(ts_val)

                exchange = str(row[3]).strip().lower()
                amount_from = float(row[4]) if pd.notnull(row[4]) else 0.0
                asset_from = str(row[5]).strip() if pd.notnull(row[5]) else None
                amount_to = float(row[6]) if pd.notnull(row[6]) else 0.0
                asset_to = str(row[7]).strip() if pd.notnull(row[7]) else None
                fee_amount = float(row[8]) if pd.notnull(row[8]) else 0.0
                fee_asset = str(row[9]).strip() if pd.notnull(row[9]) else None
                remark = str(row[10]).strip() if pd.notnull(row[10]) else ""

                tx_hash = f"bloxtax_f1_{row[0]}_{timestamp.isoformat()}"
                if "txid:" in remark.lower():
                    tx_hash = remark.lower().split("txid:")[1].split()[0].strip()
                elif "id:" in remark.lower():
                    tx_hash = remark.lower().split("id:")[1].strip()

                tx_type = self.TYPE_MAPPING.get(raw_type, TransactionType.convert)
                is_taxable = 0
                category = raw_type
                
                if raw_type == 'הפקדה':
                    tx_type = TransactionType.deposit
                    asset_to = asset_from; amount_to = amount_from
                    asset_from = None; amount_from = 0.0
                elif raw_type == 'משיכה':
                    tx_type = TransactionType.withdrawal
                elif raw_type == 'המרה':
                    if amount_from > 0 and amount_to > 0:
                        tx_type = TransactionType.sell if asset_from not in ['USD', 'EUR', 'ILS'] else TransactionType.buy
                        is_taxable = 1 if tx_type == TransactionType.sell else 0
                    elif amount_from > 0:
                        tx_type = TransactionType.sell; is_taxable = 1
                    else:
                        tx_type = TransactionType.earn
                elif raw_type in ['תקבול', 'מתנה', 'סטייקינג', 'רווח הון', 'דיבידנד']:
                    tx_type = TransactionType.earn
                    if amount_from > 0 and amount_to == 0:
                        asset_to = asset_from; amount_to = amount_from
                        asset_from = None; amount_from = 0.0

                tx = Transaction(
                    exchange=exchange, tx_hash=tx_hash, timestamp=timestamp, type=tx_type,
                    asset_from=asset_from, amount_from=amount_from,
                    asset_to=asset_to, amount_to=amount_to,
                    fee_asset=fee_asset, fee_amount=fee_amount,
                    source='csv', is_taxable_event=is_taxable, category=category,
                    raw_data=str(row.to_dict())
                )
                await self._save_transaction(tx, db)
            except Exception: pass

    async def _process_format_2(self, df: pd.DataFrame, db: Optional[AsyncSession] = None):
        for _, row in df.iterrows():
            try:
                account = str(row[0]).strip().lower()
                raw_type = str(row[1]).strip()
                if raw_type == 'nan': continue
                
                timestamp = pd.to_datetime(row[2])
                asset = str(row[3]).strip()
                amount = float(row[4])
                
                tx_hash = f"bloxtax_f2_{timestamp.isoformat()}_{asset}_{amount}"
                tx_type = self.TYPE_MAPPING.get(raw_type, TransactionType.earn)
                is_taxable = 0
                
                asset_from = None; amount_from = 0.0
                asset_to = asset; amount_to = amount
                
                if tx_type == TransactionType.withdrawal:
                    asset_from = asset; amount_from = amount
                    asset_to = None; amount_to = 0.0
                elif tx_type == TransactionType.sell:
                    asset_from = asset; amount_from = amount
                    asset_to = None; amount_to = 0.0
                    is_taxable = 1
                elif raw_type == 'הפסד הון':
                    # Loss event, often one-sided in BloxTax report
                    asset_from = asset; amount_from = amount
                    asset_to = None; amount_to = 0.0
                    is_taxable = 1

                tx = Transaction(
                    exchange=account, tx_hash=tx_hash, timestamp=timestamp, type=tx_type,
                    asset_from=asset_from, amount_from=amount_from,
                    asset_to=asset_to, amount_to=amount_to,
                    source='csv', is_taxable_event=is_taxable, category=raw_type,
                    raw_data=str(row.to_dict())
                )
                await self._save_transaction(tx, db)
            except Exception: pass

    async def _save_transaction(self, tx: Transaction, db: Optional[AsyncSession] = None):
        if db: await self._save_to_db(tx, db)
        else:
            async with AsyncSessionLocal() as session:
                await self._save_to_db(tx, session)

    async def _save_to_db(self, tx: Transaction, db: AsyncSession):
        try:
            t_start = tx.timestamp - timedelta(seconds=1)
            t_end = tx.timestamp + timedelta(seconds=1)
            stmt = select(Transaction).filter(
                Transaction.exchange == tx.exchange, 
                Transaction.asset_from == tx.asset_from, Transaction.amount_from == tx.amount_from,
                Transaction.asset_to == tx.asset_to, Transaction.amount_to == tx.amount_to, 
                Transaction.timestamp.between(t_start, t_end)
            )
            existing = (await db.execute(stmt)).scalars().first()
            if not existing:
                db.add(tx); await db.commit()
        except Exception: await db.rollback()

bloxtax_ingestion_service = BloxTaxIngestionService()
