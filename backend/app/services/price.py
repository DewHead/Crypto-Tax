import ccxt.async_support as ccxt
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List
import asyncio
import logging
from sqlalchemy import select, insert, and_
from app.db.session import AsyncSessionLocal
# We might want a table for crypto prices too, but let's start with a memory cache
# or just use the DB if we want to persist them.
# For now, let's keep it simple with memory cache and a small DB table if needed.

logger = logging.getLogger(__name__)

class PriceService:
    def __init__(self):
        self.cache: Dict[str, Dict[date, float]] = {}
        self._exchange = None

    async def get_exchange(self):
        """
        Returns the ccxt exchange instance, re-initializing if the event loop has changed.
        """
        current_loop = asyncio.get_running_loop()
        
        # If exchange exists but was created in a different loop (common in tests)
        if self._exchange:
            # Check if the exchange's loop is still running
            # ccxt stores the loop in self.asyncio_loop
            if getattr(self._exchange, 'asyncio_loop', None) != current_loop:
                try:
                    await self._exchange.close()
                except:
                    pass
                self._exchange = None
        
        if self._exchange is None:
            self._exchange = ccxt.binance({
                'enableRateLimit': True,
            })
            # Ensure ccxt knows about the current loop
            self._exchange.asyncio_loop = current_loop
            
        return self._exchange

    def normalize_symbol(self, asset: str) -> str:
        """
        Normalizes Kraken-specific symbols to standard ones.
        """
        # Remove Kraken futures suffix if present
        if asset.endswith('.F'):
            asset = asset[:-2]

        mapping = {
            'XXBT': 'BTC',
            'XETH': 'ETH',
            'XXRP': 'XRP',
            'XLTC': 'LTC',
            'XDG': 'DOGE',
            'ZUSD': 'USD',
            'ZEUR': 'EUR',
            'ZGBP': 'GBP',
            'ZJPY': 'JPY',
            'XXLM': 'XLM',
            'XETC': 'ETC',
            'XREP': 'REP',
            'XXMR': 'XMR',
            'XZEC': 'ZEC',
            'BCHA': 'BCH',
        }
        
        return mapping.get(asset, asset)

    async def get_historical_price(self, asset: str, target_date: date) -> Optional[float]:
        """
        Fetches historical price in USD for a given asset and date.
        """
        asset = self.normalize_symbol(asset)
        
        # Mapping for symbols that changed or have better liquidity elsewhere
        symbol_map = {
            'BCC': 'BCH',
            'IOTA': 'MIOTA',
        }
        asset = symbol_map.get(asset, asset)
        
        if asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI', 'ZUSD']:
            return 1.0
        
        if asset == 'ILS':
            return None
        
        # Check cache
        if asset in self.cache and target_date in self.cache[asset]:
            return self.cache[asset][target_date]
        
        # Convert date to timestamp (start of day)
        dt = datetime.combine(target_date, datetime.min.time())
        ts = int(dt.timestamp() * 1000)
        exchange = await self.get_exchange()

        async def fetch_price(sym: str):
            try:
                ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1d', since=ts, limit=1)
                if ohlcv and len(ohlcv) > 0:
                    return ohlcv[0][4]
            except:
                pass
            return None

        # 1. Try USDT pair
        price = await fetch_price(f"{asset}/USDT")
        if price:
            if asset not in self.cache: self.cache[asset] = {}
            self.cache[asset][target_date] = price
            return price
        
        # 2. Try BTC pair and multiply by BTC price
        if asset != 'BTC':
            btc_price = await self.get_historical_price('BTC', target_date)
            if btc_price:
                pair_price = await fetch_price(f"{asset}/BTC")
                if pair_price:
                    price = pair_price * btc_price
                    if asset not in self.cache: self.cache[asset] = {}
                    self.cache[asset][target_date] = price
                    return price
        
        # 3. Try ETH pair and multiply by ETH price
        if asset != 'ETH':
            eth_price = await self.get_historical_price('ETH', target_date)
            if eth_price:
                pair_price = await fetch_price(f"{asset}/ETH")
                if pair_price:
                    price = pair_price * eth_price
                    if asset not in self.cache: self.cache[asset] = {}
                    self.cache[asset][target_date] = price
                    return price
            
        return None


    async def close(self):
        if self._exchange:
            await self._exchange.close()
            self._exchange = None

price_service = PriceService()
