import ccxt.async_support as ccxt
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List, Set
import asyncio
import logging

logger = logging.getLogger(__name__)

class PriceService:
    def __init__(self):
        self.cache: Dict[str, Dict[date, float]] = {}
        self._binance = None
        self._kraken = None

    async def get_binance(self):
        current_loop = asyncio.get_running_loop()
        if self._binance and getattr(self._binance, 'asyncio_loop', None) != current_loop:
            try: await self._binance.close()
            except: pass
            self._binance = None
        
        if self._binance is None:
            self._binance = ccxt.binance({'enableRateLimit': True})
            self._binance.asyncio_loop = current_loop
        return self._binance

    async def get_kraken(self):
        current_loop = asyncio.get_running_loop()
        if self._kraken and getattr(self._kraken, 'asyncio_loop', None) != current_loop:
            try: await self._kraken.close()
            except: pass
            self._kraken = None
        
        if self._kraken is None:
            self._kraken = ccxt.kraken({'enableRateLimit': True})
            self._kraken.asyncio_loop = current_loop
        return self._kraken

    def normalize_symbol(self, asset: str) -> str:
        if asset.endswith('.F'): asset = asset[:-2]
        mapping = {
            'XXBT': 'BTC', 'XETH': 'ETH', 'XXRP': 'XRP', 'XLTC': 'LTC',
            'XDG': 'DOGE', 'ZUSD': 'USD', 'ZEUR': 'EUR', 'ZGBP': 'GBP',
            'ZJPY': 'JPY', 'XXLM': 'XLM', 'XETC': 'ETC', 'XREP': 'REP',
            'XXMR': 'XMR', 'XZEC': 'ZEC', 'BCHA': 'BCH',
        }
        return mapping.get(asset, asset)

    async def get_historical_price(self, asset: str, target_date: date, visited: Optional[Set[str]] = None) -> Optional[float]:
        if visited is None: visited = set()
        orig_asset = asset
        asset = self.normalize_symbol(asset)
        if asset in visited: return None
        visited.add(asset)

        symbol_map = {
            'BCC': 'BCH',
            'IOTA': 'MIOTA',
            'XBT': 'BTC',
        }
        mapped_asset = symbol_map.get(asset, asset)
        if mapped_asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'ZUSD']: return 1.0
        if mapped_asset == 'ILS': return None
        if mapped_asset in self.cache and target_date in self.cache[mapped_asset]:
            return self.cache[mapped_asset][target_date]
        
        dt = datetime.combine(target_date, datetime.min.time())
        ts = int(dt.timestamp() * 1000)
        
        async def fetch_from_exchange(exchange, sym: str):
            try:
                ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1d', since=ts, limit=1)
                if ohlcv and len(ohlcv) > 0:
                    p = ohlcv[0][4]
                    logger.info(f"Fetched price for {sym} on {target_date} from {exchange.id}: {p}")
                    return p
            except: pass
            return None

        binance = await self.get_binance()
        kraken = await self.get_kraken()

        # Try multiple variations and exchanges
        search_symbols = [mapped_asset, asset, orig_asset]
        if 'BCH' in search_symbols: search_symbols.append('BCC')
        if 'BTC' in search_symbols: search_symbols.append('XBT')

        for symbol in filter(None, list(dict.fromkeys(search_symbols))):
            # 1. USD/USDT pairs
            for base in ['USDT', 'USD', 'ZUSD', 'USDC']:
                pair = f"{symbol}/{base}"
                for ex in [binance, kraken]:
                    price = await fetch_from_exchange(ex, pair)
                    if price:
                        if mapped_asset not in self.cache: self.cache[mapped_asset] = {}
                        self.cache[mapped_asset][target_date] = price
                        return price
            
            # 2. BTC pair
            if symbol not in ['BTC', 'XBT']:
                btc_price = await self.get_historical_price('BTC', target_date, visited.copy())
                if btc_price:
                    pair = f"{symbol}/BTC"
                    for ex in [binance, kraken]:
                        pair_price = await fetch_from_exchange(ex, pair)
                        if pair_price:
                            price = pair_price * btc_price
                            if mapped_asset not in self.cache: self.cache[mapped_asset] = {}
                            self.cache[mapped_asset][target_date] = price
                            return price
        return None

    async def get_current_price(self, asset: str) -> Optional[float]:
        """Fetch the most recent price for an asset."""
        # Using yesterday's price as a proxy for 'current' to ensure we have a closed candle
        return await self.get_historical_price(asset, date.today() - timedelta(days=1))

    async def close(self):
        if self._binance: await self._binance.close(); self._binance = None
        if self._kraken: await self._kraken.close(); self._kraken = None

price_service = PriceService()
