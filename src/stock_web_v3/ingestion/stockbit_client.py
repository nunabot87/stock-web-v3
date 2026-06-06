"""Stockbit API Client - Data ingestion from Stockbit APIs."""

import asyncio
import aiohttp
import json
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import pytz

from ..config import get_settings
from ..database import execute, fetchone, fetchall

WIB = pytz.timezone('Asia/Jakarta')


class StockbitAPIError(Exception):
    """Stockbit API error."""
    pass


class StockbitClient:
    """
    Stockbit API client for data ingestion.
    Endpoints:
    - /chartbit/{symbol}/price/intraday - 1-min OHLCV
    - /chartbit/{symbol}/price/daily - Daily OHLCV
    - /search/stocks - Stock lookup
    - /emitten/{sym}/profile - Emitten profile
    - /corpaction - Corporate actions
    - /orderbook/{symbol} - Order book (if available)
    """
    
    def __init__(self, token: Optional[str] = None):
        self.settings = get_settings()
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = self.settings.stockbit_base_url
        self.chartbit_url = self.settings.stockbit_chartbit_url
        self._rate_limit_lock = asyncio.Lock()
        self._last_request_time = datetime.now()
    
    async def __aenter__(self):
        """Async context manager entry."""
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
            self.session = None
    
    def _get_headers(self) -> Dict[str, str]:
        """Build request headers with auth token."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://stockbit.com",
            "Referer": "https://stockbit.com/",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers
    
    async def _rate_limit(self):
        """Simple rate limiting - 10 requests per second max."""
        async with self._rate_limit_lock:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            if elapsed < 0.1:  # 100ms between requests
                await asyncio.sleep(0.1 - elapsed)
            self._last_request_time = datetime.now()
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request with rate limiting."""
        await self._rate_limit()
        
        if not self.session:
            raise StockbitAPIError("Client not initialized")
        
        headers = self._get_headers()
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        
        try:
            async with self.session.request(
                method, url, headers=headers, **kwargs
            ) as response:
                
                # Handle 401 - Token expired
                if response.status == 401:
                    raise StockbitAPIError("Token expired or unauthorized")
                
                # Handle 429 - Rate limited
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    raise StockbitAPIError(f"Rate limited. Retry after {retry_after}s")
                
                # Handle other errors
                if response.status >= 400:
                    text = await response.text()
                    raise StockbitAPIError(f"HTTP {response.status}: {text[:200]}")
                
                return await response.json()
                
        except aiohttp.ClientError as e:
            raise StockbitAPIError(f"Request failed: {str(e)}")
    
    # ============ Chartbit Data Endpoints ============
    
    async def get_intraday(self, symbol: str, from_ts: Optional[int] = None, 
                           to_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get intraday (1-minute) OHLCV data.
        Note: from/to are in REVERSE chronological order for Chartbit API.
        """
        url = f"{self.chartbit_url}/{symbol}/price/intraday"
        
        params = {}
        if from_ts and to_ts:
            # Chartbit uses REVERSE chronological (from > to in time)
            params["from"] = to_ts
            params["to"] = from_ts
        
        data = await self._request("GET", url, params=params)
        
        # Parse response
        if isinstance(data, dict) and "data" in data:
            inner = data["data"]
            if isinstance(inner, dict) and "chartbit" in inner:
                return inner["chartbit"]
            return inner if isinstance(inner, list) else []
        return data if isinstance(data, list) else []
    
    async def get_daily(self, symbol: str, from_date: Optional[str] = None,
                        to_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get daily OHLCV data.
        Returns daily candles with OHLCV.
        """
        url = f"{self.chartbit_url}/{symbol}/price/daily"
        
        # If no date range, get last 90 days
        if not to_date:
            to_date = datetime.now(WIB).strftime("%Y-%m-%d")
        if not from_date:
            from_dt = datetime.now(WIB) - timedelta(days=90)
            from_date = from_dt.strftime("%Y-%m-%d")
        
        # Convert to timestamps
        to_ts = int(datetime.strptime(to_date, "%Y-%m-%d").timestamp())
        from_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
        
        # Chartbit uses REVERSE chronological (from > to in time)
        params = {"from": to_ts, "to": from_ts}  # Reversed!
        
        data = await self._request("GET", url, params=params)
        
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data if isinstance(data, list) else []
    
    async def get_ihsg_daily(self, from_date: Optional[str] = None,
                              to_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get IHSG daily data."""
        return await self.get_daily("IHSG", from_date, to_date)
    
    # ============ Emitten Profile ============
    
    async def get_emitten_profile(self, symbol: str) -> Dict[str, Any]:
        """Get emitten (company) profile."""
        url = f"{self.base_url}/emitten/{symbol}/profile"
        return await self._request("GET", url)
    
    # ============ Corporate Actions ============
    
    async def get_corp_actions(self, symbol: Optional[str] = None,
                                start_date: Optional[str] = None,
                                end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get corporate actions."""
        url = f"{self.base_url}/corpaction"
        
        params = {}
        if symbol:
            params["symbol"] = symbol
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        data = await self._request("GET", url, params=params)
        return data.get("data", []) if isinstance(data, dict) else data
    
    # ============ Market Movers (Watchlist) ============
    
    async def get_market_mover(self, mover_type: str = "MOVER_TYPE_TOP_VOLUME") -> List[Dict[str, Any]]:
        """
        Get market mover data from Stockbit.
        mover_type: MOVER_TYPE_TOP_VOLUME | MOVER_TYPE_NET_FOREIGN_BUY
        Returns list of stocks with their metrics.
        """
        url = "https://exodus.stockbit.com/order-trade/market-mover"
        
        # Build query string manually because aiohttp params doesn't handle repeated keys well
        board_types = [
            "FILTER_STOCKS_TYPE_MAIN_BOARD",
            "FILTER_STOCKS_TYPE_DEVELOPMENT_BOARD",
            "FILTER_STOCKS_TYPE_ACCELERATION_BOARD",
            "FILTER_STOCKS_TYPE_NEW_ECONOMY_BOARD",
            "FILTER_STOCKS_TYPE_SPECIAL_MONITORING_BOARD"
        ]
        
        query_parts = [f"mover_type={mover_type}"]
        for board in board_types:
            query_parts.append(f"filter_stocks={board}")
        
        full_url = f"{url}?{'&'.join(query_parts)}"
        
        data = await self._request("GET", full_url)
        
        # Response format: {"message": "...", "data": {"mover_list": [...]}}
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], dict):
                return data["data"].get("mover_list", [])
            if "result" in data:
                return data["result"]
            if "stocks" in data:
                return data["stocks"]
            if "mover_list" in data:
                return data["mover_list"]
        return data if isinstance(data, list) else []


async def validate_token(token: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Validate Stockbit JWT token by making a test request.
    Returns (is_valid, user_info).
    """
    try:
        async with StockbitClient(token) as client:
            # Try to fetch IHSG data as validation
            data = await client.get_daily("IHSG")
            
            # If we got data, token is valid
            # Extract basic user info from token payload if possible
            user_info = None
            try:
                import base64
                payload = token.split(".")[1]
                # Add padding if needed
                payload += "=" * (4 - len(payload) % 4)
                decoded = json.loads(base64.b64decode(payload))
                user_info = {
                    "id": decoded.get("sub"),
                    "email": decoded.get("email"),
                    "is_premium": decoded.get("premium", False)
                }
            except Exception:
                pass
            
            return True, user_info
            
    except StockbitAPIError as e:
        if "expired" in str(e).lower() or "unauthorized" in str(e).lower():
            return False, None
        # Other errors might be temporary
        return False, None


class DataIngestionWorker:
    """
    Worker for ingesting data from Stockbit and storing to database.
    Handles both intraday and daily sync.
    """
    
    def __init__(self):
        self.settings = get_settings()
    
    async def store_intraday(self, symbol: str, data: List[Dict], token: str) -> int:
        """
        Store intraday (1-minute) data to TimescaleDB.
        Returns count of records inserted.
        """
        if not data:
            return 0
        
        inserted = 0
        
        for candle in data:
            try:
                # Chartbit v2 format: {unix_timestamp, close, open, high, low, volume}
                # Fallback to old format: {t, c, o, h, l, v}
                ts_raw = candle.get("unix_timestamp", candle.get("t", 0))
                ts = datetime.fromtimestamp(int(ts_raw), WIB)
                open_price = candle.get("open", candle.get("o", 0))
                high = candle.get("high", candle.get("h", 0))
                low = candle.get("low", candle.get("l", 0))
                close = candle.get("close", candle.get("c", 0))
                volume = candle.get("volume", candle.get("v", 0))
                
                # Insert to intraday table (TimescaleDB hypertable)
                await execute("""
                    INSERT INTO stock_prices_1m (symbol, timestamp, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """, symbol, ts, open_price, high, low, close, volume)
                
                inserted += 1
                
            except Exception as e:
                # Log but continue
                print(f"Error storing intraday for {symbol}: {e}")
                continue
        
        return inserted
    
    async def store_daily(self, symbol: str, data: List[Dict]) -> int:
        """
        Store daily OHLCV data to TimescaleDB.
        Returns count of records inserted.
        """
        if not data:
            return 0
        
        inserted = 0
        
        for candle in data:
            try:
                ts = datetime.fromtimestamp(candle.get("t", 0), WIB).date()
                open_price = candle.get("o", 0)
                high = candle.get("h", 0)
                low = candle.get("l", 0)
                close = candle.get("c", 0)
                volume = candle.get("v", 0)
                
                # Insert to daily table
                result = await execute("""
                    INSERT INTO stock_prices_daily (symbol, timestamp, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        updated_at = NOW()
                    RETURNING symbol, timestamp
                """, symbol, ts, open_price, high, low, close, volume)
                
                inserted += 1
                
            except Exception as e:
                print(f"Error storing daily for {symbol}: {e}")
                continue
        
        # Calculate and update change/percentage after insert
        await self._update_daily_changes(symbol)
        
        return inserted
    
    async def _update_daily_changes(self, symbol: str):
        """Update change/percentage columns using CTE."""
        await execute("""
            WITH ranked AS (
                SELECT 
                    symbol,
                    timestamp,
                    close,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_close
                FROM stock_prices_daily
                WHERE symbol = $1
            )
            UPDATE stock_prices_daily sp
            SET 
                change = r.close - COALESCE(r.prev_close, r.close),
                percentage = CASE 
                    WHEN r.prev_close IS NOT NULL AND r.prev_close > 0 
                    THEN ROUND(((r.close - r.prev_close) / r.prev_close * 100)::numeric, 2)
                    ELSE 0
                END
            FROM ranked r
            WHERE sp.symbol = r.symbol AND sp.timestamp = r.timestamp
        """, symbol)
    
    async def sync_tier_stocks(self, symbols: List[str], token: str, 
                               intraday: bool = False) -> Dict[str, int]:
        """
        Sync multiple stocks. If intraday=True, fetches 1-minute data.
        Otherwise fetches daily data.
        """
        results = {}
        
        async with StockbitClient(token) as client:
            for symbol in symbols:
                try:
                    if intraday:
                        # Get last 24 hours of intraday data
                        to_ts = int(datetime.now(WIB).timestamp())
                        from_ts = to_ts - (24 * 3600)
                        data = await client.get_intraday(symbol, from_ts, to_ts)
                        count = await self.store_intraday(symbol, data, token)
                    else:
                        data = await client.get_daily(symbol)
                        count = await self.store_daily(symbol, data)
                    
                    results[symbol] = count
                    
                    # Small delay between requests
                    await asyncio.sleep(0.2)
                    
                except StockbitAPIError as e:
                    results[symbol] = -1  # Error indicator
                    print(f"Stockbit error for {symbol}: {e}")
                    continue
        
        return results


# Singleton worker
ingestion_worker = DataIngestionWorker()


async def sync_stock_data(symbol: str, token: str, intraday: bool = False) -> int:
    """Public API for single stock sync."""
    result = await ingestion_worker.sync_tier_stocks([symbol], token, intraday)
    return result.get(symbol, 0)