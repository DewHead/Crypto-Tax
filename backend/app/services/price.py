import ccxt.async_support as ccxt
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List, Set, Tuple
import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.historical_price import HistoricalPrice

logger = logging.getLogger(__name__)

class PriceService:
    def __init__(self):
        self.cache: Dict[str, Dict[date, float]] = {}
        self.negative_cache: Dict[str, Set[date]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._binance = None
        self._kraken = None
        self._markets_loaded = False

    async def _get_exchange(self, name: str):
        current_loop = asyncio.get_running_loop()
        attr = f"_{name}"
        ex = getattr(self, attr)
        
        if ex and getattr(ex, 'asyncio_loop', None) != current_loop:
            try: await ex.close()
            except: pass
            ex = None
        
        if ex is None:
            if name == 'binance':
                ex = ccxt.binance({'enableRateLimit': True})
            else:
                ex = ccxt.kraken({'enableRateLimit': True})
            ex.asyncio_loop = current_loop
            setattr(self, attr, ex)
            
            # Pre-load markets to avoid redundant 404 requests
            logger.info(f"Loading {name} markets...")
            await ex.load_markets()
            
        return ex

    async def get_binance(self): return await self._get_exchange('binance')
    async def get_kraken(self): return await self._get_exchange('kraken')

    def normalize_symbol(self, asset: str) -> str:
        if not asset: return asset
        if asset.endswith('.F'): asset = asset[:-2]
        mapping = {
            'XXBT': 'BTC', 'XETH': 'ETH', 'XXRP': 'XRP', 'XLTC': 'LTC',
            'XDG': 'DOGE', 'ZUSD': 'USD', 'ZEUR': 'EUR', 'ZGBP': 'GBP',
            'ZJPY': 'JPY', 'XXLM': 'XLM', 'XETC': 'ETC', 'XREP': 'REP',
            'XXMR': 'XMR', 'XZEC': 'ZEC', 'BCHA': 'BCH', 'BCHABC': 'BCH',
        }
        return mapping.get(asset, asset)

    def _get_lock_key(self, asset: str, target_date: date) -> str:
        return f"{asset}_{target_date.isoformat()}"

    async def get_historical_price(self, asset: str, target_date: date, db: Optional[AsyncSession] = None, visited: Optional[Set[str]] = None) -> Optional[float]:
        if visited is None: visited = set()
        orig_asset = asset
        asset = self.normalize_symbol(asset)
        if not asset or asset in visited: return None
        visited.add(asset)

        symbol_map = {'BCC': 'BCH', 'IOTA': 'MIOTA', 'XBT': 'BTC', 'BCHABC': 'BCH'}
        mapped_asset = symbol_map.get(asset, asset)
        if mapped_asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'ZUSD']: return 1.0
        if mapped_asset == 'ILS': return None

        # 1. Check Memory Cache
        if mapped_asset in self.cache and target_date in self.cache[mapped_asset]:
            return self.cache[mapped_asset][target_date]
        
        if mapped_asset in self.negative_cache and target_date in self.negative_cache[mapped_asset]:
            return None

        # 2. Check Database Cache
        if db:
            try:
                stmt = select(HistoricalPrice).where(
                    HistoricalPrice.asset == mapped_asset,
                    HistoricalPrice.date == target_date
                )
                result = await db.execute(stmt)
                db_price = result.scalar_one_or_none()
                if db_price:
                    if mapped_asset not in self.cache: self.cache[mapped_asset] = {}
                    self.cache[mapped_asset][target_date] = db_price.price_usd
                    return db_price.price_usd
            except Exception as e:
                logger.warning(f"Error reading from historical_prices table: {e}")

        # 3. Fetch from Network (using Range Fetching for efficiency)
        lock_key = self._get_lock_key(mapped_asset, target_date)
        if lock_key not in self._locks: self._locks[lock_key] = asyncio.Lock()
        
        async with self._locks[lock_key]:
            # Double check cache
            if mapped_asset in self.cache and target_date in self.cache[mapped_asset]:
                return self.cache[mapped_asset][target_date]

            binance = await self.get_binance()
            kraken = await self.get_kraken()

            search_symbols = [mapped_asset, asset, orig_asset]
            if 'BCH' in search_symbols: search_symbols.append('BCC')
            if 'BTC' in search_symbols: search_symbols.append('XBT')

            price = None
            for symbol in filter(None, list(dict.fromkeys(search_symbols))):
                # 1. USD/USDT pairs - Check markets first!
                for base in ['USDT', 'USD', 'ZUSD', 'USDC']:
                    pair = f"{symbol}/{base}"
                    for ex in [binance, kraken]:
                        if pair in ex.markets:
                            price = await self._fetch_range_and_cache(ex, pair, target_date, mapped_asset, db)
                            if price: break
                    if price: break
                if price: break
                
                # 2. BTC pair
                if symbol not in ['BTC', 'XBT']:
                    btc_price = await self.get_historical_price('BTC', target_date, db, visited.copy())
                    if btc_price:
                        pair = f"{symbol}/BTC"
                        for ex in [binance, kraken]:
                            if pair in ex.markets:
                                pair_price = await self._fetch_range_and_cache(ex, pair, target_date, mapped_asset, db)
                                if pair_price:
                                    price = pair_price * btc_price
                                    break
                        if price: break
            
            if price: return price
            
            # Negative Cache
            if mapped_asset not in self.negative_cache: self.negative_cache[mapped_asset] = set()
            self.negative_cache[mapped_asset].add(target_date)
            return None

    async def _fetch_range_and_cache(self, ex, symbol: str, target_date: date, mapped_asset: str, db: Optional[AsyncSession]) -> Optional[float]:
        """Fetch up to 1000 days of data and populate cache/DB."""
        try:
            dt = datetime.combine(target_date, datetime.min.time())
            ts = int(dt.timestamp() * 1000)
            
            # Fetch 1000 candles (days) starting from target_date
            logger.info(f"\U0001f680 [RANGE FETCH] {symbol} on {ex.id}: Fetching 1000 days starting from {target_date}...")
            
            # 10 second timeout per request to avoid hanging
            ohlcv = await asyncio.wait_for(
                ex.fetch_ohlcv(symbol, timeframe='1d', since=ts, limit=1000),
                timeout=10.0
            )
            
            if not ohlcv:
                logger.warning(f"\u26a0\ufe0f No data found for {symbol} on {ex.id} starting {target_date}")
                return None

            target_price = None
            new_prices_for_db = []

            for entry in ohlcv:
                entry_dt = datetime.fromtimestamp(entry[0] / 1000.0).date()
                p = entry[4]
                
                # Update memory cache
                if mapped_asset not in self.cache: self.cache[mapped_asset] = {}
                self.cache[mapped_asset][entry_dt] = p
                
                if entry_dt == target_date:
                    target_price = p
                
                # Prepare for DB cache
                if db:
                    new_prices_for_db.append(HistoricalPrice(asset=mapped_asset, date=entry_dt, price_usd=p))

            if new_prices_for_db:
                logger.info(f"\u2705 [RANGE FETCH] {symbol}: Cached {len(new_prices_for_db)} days in memory.")
                if db:
                    # Use merge to avoid duplicates if some days are already there
                    for p_obj in new_prices_for_db:
                        await db.merge(p_obj)
                    logger.info(f"\ud83d\udcbe [RANGE FETCH] {symbol}: Merged {len(new_prices_for_db)} days into DB.")
            
            return target_price
        except Exception as e:
            logger.warning(f"Error fetching range for {symbol} on {ex.id}: {e}")
            return None

    async def prefetch_prices(self, asset_date_pairs: List[tuple], db: AsyncSession):
        """Fetch multiple prices in parallel and cache them."""
        # Warm up markets first
        await self.get_binance()
        await self.get_kraken()

        # Group by asset to maximize range fetching
        to_fetch: Dict[str, List[date]] = {}
        seen = set()
        for asset, dt in asset_date_pairs:
            if not asset: continue
            asset = self.normalize_symbol(asset)
            if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'ZUSD', 'ILS']: continue
            if (asset, dt) in seen: continue
            seen.add((asset, dt))
            if asset in self.cache and dt in self.cache[asset]: continue
            to_fetch.setdefault(asset, []).append(dt)
        
        if not to_fetch: return

        logger.info(f"Prefetching prices for {len(to_fetch)} assets...")
        
        # Increased parallelism: 50 assets at a time
        sem = asyncio.Semaphore(50)
        async def fetch_for_asset(asset, dates):
            async with sem:
                sorted_dates = sorted(dates)
                while sorted_dates:
                    d = sorted_dates[0]
                    # This call will populate the cache for 1000 days
                    await self.get_historical_price(asset, d, db=db)
                    
                    # Filter out any dates that were just covered (cache or negative cache)
                    asset_cache = self.cache.get(asset, {})
                    asset_neg_cache = self.negative_cache.get(asset, set())
                    
                    sorted_dates = [sd for sd in sorted_dates if sd not in asset_cache and sd not in asset_neg_cache]

        tasks = [fetch_for_asset(asset, dates) for asset, dates in to_fetch.items()]
        if tasks:
            await asyncio.gather(*tasks)
            await db.commit()

    async def get_current_price(self, asset: str, db: Optional[AsyncSession] = None) -> Optional[float]:
        return await self.get_historical_price(asset, date.today() - timedelta(days=1), db=db)

    async def close(self):
        if self._binance: await self._binance.close(); self._binance = None
        if self._kraken: await self._kraken.close(); self._kraken = None

price_service = PriceService()
