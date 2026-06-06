"""Live Data Broadcaster - Pulls data from Stockbit and broadcasts via WebSocket."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
import pytz
import json

from ..config import TIER1_SET, TIER2_SET, get_settings
from ..redis_client import get_redis, STOCKBIT_TOKEN_PREFIX
from ..ingestion.stockbit_client import StockbitClient, StockbitAPIError
from ..database import fetchall
from ..websocket.hub import broadcast_prices

WIB = pytz.timezone('Asia/Jakarta')


class LiveDataBroadcaster:
    """
    Broadcasts live price updates from Stockbit to WebSocket clients.
    Runs 5-minute cycles for Tier 1-2 stocks during market hours.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.running = False
        self._last_prices: Dict[str, Dict] = {}
        self._broadcast_task = None
    
    async def _get_active_token(self) -> Optional[str]:
        """Get first available active token from any session."""
        try:
            redis = get_redis()
            keys = await redis.keys(f"{STOCKBIT_TOKEN_PREFIX}*")
            if keys:
                token = await redis.get(keys[0])
                return token
        except Exception as e:
            print(f"[Broadcaster] Error getting token: {e}")
        return None
    
    async def _fetch_latest_prices(self, symbols: List[str], token: str) -> Dict[str, Dict]:
        """
        Fetch latest prices from Stockbit intraday API.
        Returns dict of symbol -> price data.
        """
        prices = {}
        
        async with StockbitClient(token) as client:
            for symbol in symbols:
                try:
                    # Get last 30 minutes of intraday data
                    now = datetime.now(WIB)
                    to_ts = int(now.timestamp())
                    from_ts = to_ts - (30 * 60)  # 30 minutes back
                    
                    data = await client.get_intraday(symbol, from_ts, to_ts)
                    
                    if data and len(data) > 0:
                        # Get the latest candle
                        latest = data[-1]
                        
                        # Calculate change from previous candle
                        prev_close = data[0]['close'] if len(data) > 1 else latest['close']
                        change = latest['close'] - prev_close
                        change_pct = (change / prev_close * 100) if prev_close > 0 else 0
                        
                        prices[symbol] = {
                            'price': float(latest['close']),
                            'open': float(latest['open']),
                            'high': float(latest['high']),
                            'low': float(latest['low']),
                            'volume': int(latest.get('volume', 0)),
                            'change': round(change, 2),
                            'change_pct': round(change_pct, 2),
                            'timestamp': int(latest.get('unix_timestamp', to_ts)),
                            'source': 'stockbit'
                        }
                    else:
                        # Fallback to database if no intraday data
                        prices[symbol] = await self._get_last_db_price(symbol)
                        
                except StockbitAPIError as e:
                    print(f"[Broadcaster] Stockbit error for {symbol}: {e}")
                    # Fallback to database
                    prices[symbol] = await self._get_last_db_price(symbol)
                    
                except Exception as e:
                    print(f"[Broadcaster] Error for {symbol}: {e}")
                    prices[symbol] = await self._get_last_db_price(symbol)
                
                # Small delay between requests
                await asyncio.sleep(0.1)
        
        return prices
    
    async def _get_last_db_price(self, symbol: str) -> Dict:
        """Get last known price from database as fallback."""
        try:
            rows = await fetchall("""
                SELECT close, change, percentage, volume, timestamp
                FROM stock_prices_daily
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            """, symbol)
            
            if rows:
                row = rows[0]
                return {
                    'price': float(row['close']),
                    'open': float(row['close']),  # Approximate
                    'high': float(row['close']),
                    'low': float(row['close']),
                    'volume': int(row.get('volume', 0)),
                    'change': float(row.get('change', 0) or 0),
                    'change_pct': float(row.get('percentage', 0) or 0),
                    'timestamp': int(datetime.now(WIB).timestamp()),
                    'source': 'database'
                }
        except Exception as e:
            print(f"[Broadcaster] DB fallback error for {symbol}: {e}")
        
        # Ultimate fallback
        return {
            'price': 0,
            'open': 0,
            'high': 0,
            'low': 0,
            'volume': 0,
            'change': 0,
            'change_pct': 0,
            'timestamp': int(datetime.now(WIB).timestamp()),
            'source': 'unavailable'
        }
    
    async def _broadcast_cycle(self):
        """Single broadcast cycle - fetch and broadcast."""
        token = await self._get_active_token()
        if not token:
            print("[Broadcaster] No active token, skipping cycle")
            return
        
        # Get symbols to broadcast (Tier 1-2 only)
        symbols = list(TIER1_SET | TIER2_SET)
        
        # Fetch latest prices
        prices = await self._fetch_latest_prices(symbols, token)
        
        # Cache in Redis
        try:
            redis = get_redis()
            await redis.setex(
                "live_prices:cache",
                300,  # 5 minute TTL
                json.dumps({
                    'prices': prices,
                    'timestamp': datetime.now(WIB).isoformat()
                })
            )
        except Exception as e:
            print(f"[Broadcaster] Cache error: {e}")
        
        # Broadcast to WebSocket clients
        if prices:
            await broadcast_prices(prices)
            print(f"[Broadcaster] Broadcasted {len(prices)} prices")
    
    def is_market_open(self) -> bool:
        """Check if BEI market is currently open."""
        now = datetime.now(WIB)
        current_time = now.time()
        weekday = now.weekday()
        
        # Weekend
        if weekday >= 5:
            return False
        
        # Market hours: 09:00 - 11:30, 13:30 - 16:00
        if current_time < __import__('datetime').time(9, 0) or current_time >= __import__('datetime').time(16, 0):
            return False
        
        # Lunch break
        if __import__('datetime').time(11, 30) <= current_time < __import__('datetime').time(13, 30):
            return False
        
        return True
    
    async def start(self):
        """Start the broadcaster loop."""
        self.running = True
        print("[Broadcaster] Starting live data broadcaster")
        
        while self.running:
            if self.is_market_open():
                try:
                    await self._broadcast_cycle()
                except Exception as e:
                    print(f"[Broadcaster] Cycle error: {e}")
            else:
                print("[Broadcaster] Market closed, skipping cycle")
            
            # Wait 5 minutes
            await asyncio.sleep(300)
        
        print("[Broadcaster] Stopped")
    
    def stop(self):
        """Stop the broadcaster."""
        self.running = False


# Singleton instance
live_broadcaster = LiveDataBroadcaster()
