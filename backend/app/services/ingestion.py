import ccxt.async_support as ccxt
from app.core.config import settings
from app.models.transaction import Transaction, TransactionType
from app.models.exchange_key import ExchangeKey
from app.db.session import AsyncSessionLocal
from sqlalchemy import select
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import asyncio

class IngestionService:
    async def sync_env_keys(self):
        async with AsyncSessionLocal() as db:
            exchanges = {
                'binance': (settings.BINANCE_API_KEY, settings.BINANCE_API_SECRET),
                'kraken': (settings.KRAKEN_API_KEY, settings.KRAKEN_API_SECRET),
                'krakenfutures': (settings.KRAKENFUTURES_API_KEY, settings.KRAKENFUTURES_API_SECRET),
            }
            for name, (key, secret) in exchanges.items():
                if key and not key.startswith("your_") and secret and not secret.startswith("your_"):
                    stmt = select(ExchangeKey).filter(ExchangeKey.exchange_name == name)
                    result = await db.execute(stmt)
                    if not result.scalars().first():
                        print(f"Seeding {name} API key from environment...")
                        new_key = ExchangeKey(exchange_name=name, api_key=key, api_secret=secret)
                        db.add(new_key)
            await db.commit()

    async def reset_sync_status(self):
        """
        Resets all is_syncing flags to 0. Called on startup.
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(ExchangeKey))
            keys = result.scalars().all()
            for key in keys:
                key.is_syncing = 0
            await db.commit()
            print("Reset all sync statuses.")

    async def sync_all(self):
        await self.sync_env_keys()
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(ExchangeKey))
            keys = result.scalars().all()
            for key in keys: key.is_syncing = 1
            await db.commit()
        tasks = []
        for key in keys: tasks.append(self.sync_one(key.id))
        try:
            if tasks: await asyncio.gather(*tasks)
        finally:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(ExchangeKey))
                keys = result.scalars().all()
                for key in keys:
                    key.is_syncing = 0
                    key.last_sync_at = datetime.now(timezone.utc)
                await db.commit()

    async def sync_one(self, key_id: int):
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(ExchangeKey).filter(ExchangeKey.id == key_id))
            key = result.scalars().first()
            if not key: return
            if not key.api_key or not key.api_secret:
                print(f"Skipping sync for {key.exchange_name} as API keys are missing.")
                return
            key.is_syncing = 1
            await db.commit()
            try:
                exchange_class = getattr(ccxt, key.exchange_name.lower())
                exchange_instance = exchange_class({
                    'apiKey': key.api_key,
                    'secret': key.api_secret,
                    'enableRateLimit': True,
                })
                await self.sync_exchange(key.exchange_name, exchange_instance)
                await exchange_instance.close()
            except Exception as e:
                print(f"Error syncing {key.exchange_name}: {e}")
            finally:
                async with AsyncSessionLocal() as db:
                    result = await db.execute(select(ExchangeKey).filter(ExchangeKey.id == key_id))
                    key = result.scalars().first()
                    if key:
                        key.is_syncing = 0
                        key.last_sync_at = datetime.now(timezone.utc)
                        await db.commit()

    async def sync_exchange(self, name: str, exchange: ccxt.Exchange):
        try:
            await exchange.load_markets()
            assets = set()
            
            # 1. Deposits & Withdrawals (Discovery)
            if exchange.has['fetchDeposits']:
                try:
                    deposits = await exchange.fetch_deposits()
                    for d in deposits:
                        assets.add(d['currency'])
                        if d['status'].lower() in ['ok', 'success', 'completed']:
                            tx = Transaction(
                                exchange=name, tx_hash=d['txid'] or d['id'],
                                timestamp=datetime.fromtimestamp(d['timestamp'] / 1000.0),
                                type=TransactionType.deposit, asset_from=None, amount_from=0.0,
                                asset_to=d['currency'], amount_to=d['amount'],
                                fee_asset=d['fee']['currency'] if d.get('fee') else None,
                                fee_amount=d['fee']['cost'] if d.get('fee') else 0.0,
                                is_taxable_event=0, source='api'
                            )
                            await self._save_transaction(tx)
                except: pass

            if exchange.has['fetchWithdrawals']:
                try:
                    withdrawals = await exchange.fetch_withdrawals()
                    for w in withdrawals:
                        assets.add(w['currency'])
                        if w['status'].lower() in ['ok', 'success', 'completed']:
                            tx = Transaction(
                                exchange=name, tx_hash=w['txid'] or w['id'],
                                timestamp=datetime.fromtimestamp(w['timestamp'] / 1000.0),
                                type=TransactionType.withdrawal, asset_from=w['currency'], amount_from=w['amount'],
                                asset_to=None, amount_to=0.0,
                                fee_asset=w['fee']['currency'] if w.get('fee') else None,
                                fee_amount=w['fee']['cost'] if w.get('fee') else 0.0,
                                is_taxable_event=0, source='api'
                            )
                            await self._save_transaction(tx)
                except: pass

            # 2. Special Histories (Discovery)
            if name.lower() == 'binance':
                found_assets = await self._sync_binance_special(exchange, assets)
                assets.update(found_assets)

            # 3. Current Balance (Discovery)
            try:
                balance = await exchange.fetch_balance()
                for asset, total in balance['total'].items():
                    if total > 0: assets.add(asset)
            except: pass

            # 4. My Trades
            if exchange.has['fetchMyTrades']:
                try:
                    since = int(datetime(2017, 1, 1).timestamp() * 1000)
                    if name.lower().startswith('kraken'):
                        print(f"Fetching all trades for {name} with pagination...")
                        trades = await self._fetch_all_my_trades(exchange, symbol=None, since=since)
                        print(f"Found {len(trades)} total trades for {name}")
                        for t in trades: await self._process_trade(name, exchange, t)
                        print(f"Syncing ledger for {name}...")
                        if name.lower() == 'krakenfutures':
                            await self._sync_krakenfutures_ledger(exchange, since=since)
                        else:
                            await self._sync_kraken_ledger(exchange, name=name, since=since)
                    else:
                        # Narrow symbol search but ensure common quotes are included
                        discovery_assets = assets.copy()
                        discovery_assets.update(['USDT', 'BTC', 'ETH', 'BNB', 'BUSD'])
                        symbols_to_check = []
                        for s in exchange.markets.keys():
                            m = exchange.market(s)
                            # Check symbols where base is an asset we know about
                            if m['base'] in discovery_assets:
                                symbols_to_check.append(s)
                        
                        print(f"Checking {len(symbols_to_check)} relevant symbols for trades on {name} in parallel...")
                        semaphore = asyncio.Semaphore(10) 
                        async def fetch_trades_safe(symbol):
                            async with semaphore:
                                try: return await self._fetch_all_my_trades(exchange, symbol, since=since)
                                except: return []
                        tasks = [fetch_trades_safe(s) for s in symbols_to_check]
                        results = await asyncio.gather(*tasks)
                        total_found = 0
                        for trades in results:
                            if trades:
                                total_found += len(trades)
                                for t in trades: await self._process_trade(name, exchange, t)
                        print(f"Finished relevant symbol check for {name}. Total trades found: {total_found}")
                except Exception as e:
                    print(f"Error fetching trades for {name}: {e}")
        except Exception as e:
            print(f"Critical error syncing {name}: {e}")

    async def _fetch_all_my_trades(self, exchange: ccxt.Exchange, symbol: str = None, since: int = None):
        all_trades = []
        if exchange.id == 'kraken':
            offset = 0
            while True:
                trades = await exchange.fetch_my_trades(symbol, since=since, params={'ofs': offset})
                if not trades: break
                all_trades.extend(trades)
                offset += len(trades)
                if len(trades) < 50: break
        else:
            current_since = since
            while True:
                trades = await exchange.fetch_my_trades(symbol, since=current_since)
                if not trades: break
                all_trades.extend(trades)
                new_since = trades[-1]['timestamp'] + 1
                if current_since is not None and new_since <= current_since: break
                current_since = new_since
                if len(trades) < 5: break
        return all_trades

    async def _sync_kraken_ledger(self, exchange: ccxt.Exchange, name: str = 'kraken', since: int = None):
        try:
            print(f"Syncing {name} ledger since {since}...")
            all_ledger = []
            offset = 0
            while True:
                ledger = await exchange.fetch_ledger(since=since, params={'ofs': offset})
                if not ledger: break
                all_ledger.extend(ledger)
                offset += len(ledger)
                if len(ledger) < 50: break

            trades_by_refid = {}
            for entry in all_ledger:
                etype = entry['type']
                refid = entry['referenceId']
                if etype == 'trade':
                    if name.lower() == 'kraken': continue # Kraken trades are fetched via fetch_my_trades
                    if refid not in trades_by_refid: trades_by_refid[refid] = []
                    trades_by_refid[refid].append(entry)
                elif etype in ['deposit', 'withdrawal', 'transfer', 'receive', 'spend', 'settle']:
                    tx = Transaction(
                        exchange=name, tx_hash=entry['id'],
                        timestamp=datetime.fromtimestamp(entry['timestamp'] / 1000.0),
                        type=TransactionType.deposit if (etype in ['deposit', 'receive', 'transfer'] or (etype == 'settle' and entry['amount'] > 0)) and entry['amount'] > 0 else TransactionType.withdrawal,
                        asset_from=entry['currency'] if entry['amount'] < 0 else None,
                        amount_from=abs(entry['amount']) if entry['amount'] < 0 else 0.0,
                        asset_to=entry['currency'] if entry['amount'] > 0 else None,
                        amount_to=abs(entry['amount']) if entry['amount'] > 0 else 0.0,
                        fee_asset=entry['fee']['currency'] if entry.get('fee') else None,
                        fee_amount=entry['fee']['cost'] if entry.get('fee') else 0.0,
                        is_taxable_event=0, source='api'
                    )
                    await self._save_transaction(tx)
                elif etype in ['staking', 'earn', 'reward']:
                    tx = Transaction(
                        exchange=name, tx_hash=entry['id'],
                        timestamp=datetime.fromtimestamp(entry['timestamp'] / 1000.0),
                        type=TransactionType.earn, asset_from=None, amount_from=0.0,
                        asset_to=entry['currency'], amount_to=abs(entry['amount']),
                        fee_asset=entry['fee']['currency'] if entry.get('fee') else None,
                        fee_amount=entry['fee']['cost'] if entry.get('fee') else 0.0,
                        is_taxable_event=0, source='api',
                        category=etype.capitalize()
                    )
                    await self._save_transaction(tx)
                elif etype in ['margin', 'rollover']:
                    # Realized Margin P&L in Kraken ledger is usually 'margin' type with positive/negative amount
                    is_pnl = (etype == 'margin')
                    tx = Transaction(
                        exchange=name, tx_hash=entry['id'],
                        timestamp=datetime.fromtimestamp(entry['timestamp'] / 1000.0),
                        type=TransactionType.sell if entry['amount'] < 0 else TransactionType.buy,
                        asset_from=entry['currency'] if entry['amount'] < 0 else None,
                        amount_from=abs(entry['amount']) if entry['amount'] < 0 else 0.0,
                        asset_to=entry['currency'] if entry['amount'] > 0 else None,
                        amount_to=abs(entry['amount']) if entry['amount'] > 0 else 0.0,
                        fee_asset=entry['currency'] if etype == 'rollover' else None,
                        fee_amount=abs(entry['amount']) if etype == 'rollover' else 0.0,
                        is_taxable_event=1, source='api'
                    )
                    await self._save_transaction(tx)

            for refid, entries in trades_by_refid.items():
                if len(entries) >= 2:
                    out_entry = next((e for e in entries if e['amount'] < 0), None)
                    in_entry = next((e for e in entries if e['amount'] > 0), None)
                    if out_entry and in_entry:
                        tx = Transaction(
                            exchange=name, tx_hash=refid,
                            timestamp=datetime.fromtimestamp(in_entry['timestamp'] / 1000.0),
                            type=TransactionType.sell if out_entry['currency'] not in ['USD', 'EUR', 'ILS'] else TransactionType.buy,
                            asset_from=out_entry['currency'], amount_from=abs(out_entry['amount']),
                            asset_to=in_entry['currency'], amount_to=abs(in_entry['amount']),
                            fee_asset=in_entry['fee']['currency'] if in_entry.get('fee') else None,
                            fee_amount=in_entry['fee']['cost'] if in_entry.get('fee') else 0.0,
                            is_taxable_event=1 if out_entry['currency'] not in ['USD', 'EUR', 'ILS'] else 0,
                            source='api'
                        )
                        await self._save_transaction(tx)
        except Exception as e:
            print(f"Error syncing {name} ledger: {e}")

    async def _sync_krakenfutures_ledger(self, exchange: ccxt.Exchange, since: int = None):
        try:
            print(f"Syncing krakenfutures ledger...")
            # Kraken Futures uses history_get_account_log
            res = await exchange.history_get_account_log()
            logs = res.get('logs', [])
            
            for l in logs:
                # date format: 2025-09-30T08:06:52.772Z
                # Handle cases where micro/milliseconds might be missing or different
                date_str = l['date']
                try:
                    if '.' in date_str:
                        ts = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                    else:
                        ts = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except Exception:
                    # Fallback to general isoformat parsing if possible, or skip
                    continue

                if since and ts.timestamp() * 1000 < since:
                    continue
                
                info = l.get('info', '')
                asset = l['asset'].upper()
                if asset == 'XBT': asset = 'BTC'
                
                pnl = float(l.get('realized_pnl') or 0)
                funding = float(l.get('realized_funding') or 0)
                amount = pnl + funding
                fee = float(l.get('fee') or 0) + float(l.get('liquidation_fee') or 0)
                
                if info in ['futures trade', 'futures liquidation', 'funding rate change']:
                    # If it's a 'futures trade', it might be a position open/close which CCXT fetchMyTrades also catches.
                    # We only want the REALIZED P&L from the ledger.
                    is_taxable = (info != 'futures trade') # Only funding and liquidations are P&L in ledger
                    
                    if amount != 0 or fee != 0:
                        tx = Transaction(
                            exchange='krakenfutures', tx_hash=l['booking_uid'],
                            timestamp=ts,
                            type=TransactionType.earn if amount > 0 else (TransactionType.fee if amount < 0 else TransactionType.fee),
                            asset_from=asset if amount < 0 else None,
                            amount_from=abs(amount) if amount < 0 else 0.0,
                            asset_to=asset if amount > 0 else None,
                            amount_to=abs(amount) if amount > 0 else 0.0,
                            fee_asset=asset, fee_amount=fee,
                            is_taxable_event=1 if is_taxable else 0, source='api',
                            raw_data=str(l)
                        )
                        await self._save_transaction(tx)
                elif info == 'cross-exchange transfer':
                    old_bal = float(l.get('old_balance') or 0)
                    new_bal = float(l.get('new_balance') or 0)
                    diff = new_bal - old_bal
                    if diff != 0:
                        tx = Transaction(
                            exchange='krakenfutures', tx_hash=l['booking_uid'],
                            timestamp=ts,
                            type=TransactionType.deposit if diff > 0 else TransactionType.withdrawal,
                            asset_from=asset if diff < 0 else None,
                            amount_from=abs(diff) if diff < 0 else 0.0,
                            asset_to=asset if diff > 0 else None,
                            amount_to=abs(diff) if diff > 0 else 0.0,
                            fee_asset=asset, fee_amount=fee,
                            is_taxable_event=0, source='api',
                            raw_data=str(l)
                        )
                        await self._save_transaction(tx)
        except Exception as e:
            print(f"Error syncing krakenfutures ledger: {e}")

    async def _process_trade(self, name: str, exchange: ccxt.Exchange, t: Dict[str, Any]):
        try:
            if name.lower().startswith('kraken'):
                # Check for margin on Kraken Spot or any trade on Kraken Futures
                leverage = int(t['info'].get('leverage', 0))
                if leverage > 0 or name.lower() == 'krakenfutures':
                    return # Skip margin/futures trades, they are handled via ledger P&L entries
            
            market = exchange.market(t['symbol'])
            base = market['base']; quote = market['quote']; side = t['side'].lower()
            if side == 'buy':
                tx = Transaction(
                    exchange=name, tx_hash=t['id'], timestamp=datetime.fromtimestamp(t['timestamp'] / 1000.0),
                    type=TransactionType.buy, asset_from=quote, amount_from=t['cost'],
                    asset_to=base, amount_to=t['amount'],
                    fee_asset=t['fee']['currency'] if t.get('fee') else None,
                    fee_amount=t['fee']['cost'] if t.get('fee') else 0.0,
                    is_taxable_event=0, source='api'
                )
            else: # sell
                tx = Transaction(
                    exchange=name, tx_hash=t['id'], timestamp=datetime.fromtimestamp(t['timestamp'] / 1000.0),
                    type=TransactionType.sell, asset_from=base, amount_from=t['amount'],
                    asset_to=quote, amount_to=t['cost'],
                    fee_asset=t['fee']['currency'] if t.get('fee') else None,
                    fee_amount=t['fee']['cost'] if t.get('fee') else 0.0,
                    is_taxable_event=1, source='api'
                )
            await self._save_transaction(tx)
        except: pass

    async def _sync_binance_special(self, exchange: ccxt.Exchange, assets: set):
        discovered_assets = set()
        end_time = int(datetime.now().timestamp() * 1000)
        chunk_ms = 30 * 24 * 60 * 60 * 1000
        wall_time = int(datetime(2017, 1, 1).timestamp() * 1000)
        
        # 1. Convert
        try:
            print("Syncing Binance Convert history...")
            current_end = end_time
            while current_end > wall_time:
                current_start = max(current_end - chunk_ms, wall_time)
                response = await exchange.sapi_get_convert_tradeflow({'startTime': current_start, 'endTime': current_end, 'limit': 1000})
                records = response.get('list', [])
                for c in records:
                    discovered_assets.add(c['fromAsset']); discovered_assets.add(c['toAsset'])
                    tx = Transaction(
                        exchange='binance', tx_hash=str(c['orderId']), timestamp=datetime.fromtimestamp(float(c['createTime']) / 1000.0),
                        type=TransactionType.convert, asset_from=c['fromAsset'], amount_from=float(c['fromAmount']),
                        asset_to=c['toAsset'], amount_to=float(c['toAmount']), fee_asset=None, fee_amount=0.0,
                        is_taxable_event=1, source='api', category='Convert'
                    )
                    await self._save_transaction(tx)
                current_end = current_start
        except: pass

        # 2. Dust
        try:
            print("Syncing Binance Dust log...")
            current_end = end_time
            while current_end > wall_time:
                current_start = max(current_end - chunk_ms, wall_time)
                response = await exchange.sapi_get_asset_dribblet({'startTime': current_start, 'endTime': current_end})
                userAssetDribblets = response.get('userAssetDribblets', [])
                for d in userAssetDribblets:
                    for detail in d['userAssetDribbletDetails']:
                        discovered_assets.add(detail['fromAsset']); discovered_assets.add('BNB')
                        tx = Transaction(
                            exchange='binance', tx_hash=str(detail['transId']), timestamp=datetime.fromtimestamp(float(d['operateTime']) / 1000.0),
                            type=TransactionType.dust, asset_from=detail['fromAsset'], amount_from=float(detail['amount']),
                            asset_to='BNB', amount_to=float(detail.get('transferAmount', detail.get('amount', 0))),
                            fee_asset='BNB', fee_amount=float(detail.get('serviceChargeAmount', 0)),
                            is_taxable_event=1, source='api', category='Dust'
                        )
                        await self._save_transaction(tx)
                current_end = current_start
        except: pass

        # 3. Simple Earn
        try:
            print("Syncing Binance Simple Earn history...")
            current_end = end_time
            while current_end > wall_time:
                current_start = max(current_end - chunk_ms, wall_time)
                for earn_type in ['Flexible', 'Locked']:
                    page = 1
                    while True:
                        method = f"sapiGetSimpleEarn{earn_type}HistoryRewardsRecord"
                        try:
                            response = await getattr(exchange, method)({'startTime': current_start, 'endTime': current_end, 'current': page, 'size': 100})
                            records = response.get('rows', [])
                            if not records: break
                            for r in records:
                                discovered_assets.add(r['asset'])
                                tx = Transaction(
                                    exchange='binance', tx_hash=f"earn_{r['time']}_{r['asset']}", timestamp=datetime.fromtimestamp(float(r['time']) / 1000.0),
                                    type=TransactionType.earn, asset_from=None, amount_from=0.0,
                                    asset_to=r['asset'], amount_to=float(r['amount']), fee_asset=None, fee_amount=0.0,
                                    is_taxable_event=0, source='api', category='Earn'
                                )
                                await self._save_transaction(tx)
                            if len(records) < 100: break
                            page += 1
                        except: break
                current_end = current_start
        except: pass

        # 4. Dividends (Generic Rewards)
        try:
             print("Syncing Binance Dividend history...")
             current_end = end_time
             while current_end > wall_time:
                 current_start = max(current_end - (90 * 24 * 60 * 60 * 1000), wall_time)
                 # Use smaller chunks for dividends to avoid missing data
                 response = await exchange.sapi_get_asset_assetdividend({'startTime': current_start, 'endTime': current_end, 'limit': 500})
                 rows = response.get('rows', [])
                 if rows:
                     for r in rows:
                         discovered_assets.add(r['asset'])
                         tx = Transaction(
                            exchange='binance', tx_hash=f"div_{r['divTime']}_{r['asset']}", timestamp=datetime.fromtimestamp(float(r['divTime']) / 1000.0),
                            type=TransactionType.earn, asset_from=None, amount_from=0.0,
                            asset_to=r['asset'], amount_to=float(r['amount']), fee_asset=None, fee_amount=0.0,
                            is_taxable_event=0, source='api', category='Earn'
                        )
                         await self._save_transaction(tx)
                 current_end = current_start
        except: pass

        # 5. Fiat
        try:
            print("Syncing Binance Fiat history...")
            for transaction_type in [0, 1]:
                current_end = end_time
                while current_end > wall_time:
                    current_start = max(current_end - (90 * 24 * 60 * 60 * 1000), wall_time)
                    response = await exchange.sapi_get_fiat_orders({'transactionType': transaction_type, 'beginTime': current_start, 'endTime': current_end})
                    data = response.get('data', [])
                    if data:
                        for f in data:
                            if f['status'].lower() == 'completed':
                                discovered_assets.add(f['fiatCurrency']); discovered_assets.add(f['cryptoCurrency'])
                                tx = Transaction(
                                    exchange='binance', tx_hash=str(f['orderNo']), timestamp=datetime.fromtimestamp(float(f['createTime']) / 1000.0),
                                    type=TransactionType.buy if transaction_type == 0 else TransactionType.sell,
                                    asset_from=f['fiatCurrency'] if transaction_type == 0 else f['cryptoCurrency'],
                                    amount_from=float(f['amount']) if transaction_type == 0 else float(f['obtainAmount']),
                                    asset_to=f['cryptoCurrency'] if transaction_type == 0 else f['fiatCurrency'],
                                    amount_to=float(f['obtainAmount']) if transaction_type == 0 else float(f['amount']),
                                    fee_asset=f['fiatCurrency'], fee_amount=float(f['totalFee']),
                                    is_taxable_event=1 if transaction_type == 1 else 0, source='api'
                                )
                                await self._save_transaction(tx)
                    current_end = current_start
        except: pass
        return discovered_assets

    async def _save_transaction(self, tx: Transaction):
        async with AsyncSessionLocal() as db:
            try:
                if tx.tx_hash and not tx.tx_hash.startswith('csv_'):
                    stmt = select(Transaction).filter(Transaction.tx_hash == tx.tx_hash, Transaction.exchange == tx.exchange)
                    result = await db.execute(stmt)
                    if result.scalars().first(): return
                t_start = tx.timestamp - timedelta(seconds=1)
                t_end = tx.timestamp + timedelta(seconds=1)
                stmt = select(Transaction).filter(
                    Transaction.exchange == tx.exchange, Transaction.asset_from == tx.asset_from, Transaction.amount_from == tx.amount_from,
                    Transaction.asset_to == tx.asset_to, Transaction.amount_to == tx.amount_to, Transaction.timestamp.between(t_start, t_end)
                )
                result = await db.execute(stmt)
                if not result.scalars().first():
                    db.add(tx); await db.commit()
                elif tx.source == 'api' and result.scalars().first().source == 'csv':
                    existing = result.scalars().first()
                    existing.tx_hash = tx.tx_hash; existing.source = 'api'; existing.raw_data = tx.raw_data
                    await db.commit()
            except: await db.rollback()

ingestion_service = IngestionService()
