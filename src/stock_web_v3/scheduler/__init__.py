"""APScheduler - Background job scheduling for stock data sync."""

import asyncio
from datetime import datetime, time, timedelta
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import pytz

from ..config import get_settings, TIER1_SET, TIER2_SET
from ..redis_client import get_redis, STOCKBIT_TOKEN_PREFIX
from ..ingestion.stockbit_client import DataIngestionWorker, StockbitAPIError
from ..websocket.hub import manager, broadcast_market_status
from ..database import fetchone

WIB = pytz.timezone('Asia/Jakarta')


async def is_trading_holiday() -> bool:
    """Check if today is a trading holiday from trading_holidays table."""
    try:
        row = await fetchone(
            "SELECT 1 FROM trading_holidays WHERE holiday_date = CURRENT_DATE"
        )
        return row is not None
    except Exception:
        return False


def _extract_mover_stock(stock: dict) -> dict:
    """Normalize Stockbit market mover stock dict to flat fields."""
    sd = stock.get("stock_detail") or {}
    change_obj = stock.get("change") or {}
    volume_obj = stock.get("volume") or {}
    nf_buy = stock.get("net_foreign_buy") or {}
    nf_sell = stock.get("net_foreign_sell") or {}
    
    symbol = sd.get("code") or stock.get("symbol") or stock.get("stock")
    name = sd.get("name") or stock.get("name") or stock.get("company_name")
    price = stock.get("price") or stock.get("close")
    
    if isinstance(change_obj, dict):
        change_val = change_obj.get("value")
        pct = change_obj.get("percentage")
    else:
        change_val = change_obj
        pct = stock.get("percentage") or stock.get("change_pct")
    
    if isinstance(volume_obj, dict):
        vol = volume_obj.get("raw")
    else:
        vol = volume_obj
    
    if isinstance(nf_buy, dict):
        nf_buy_val = nf_buy.get("raw", 0)
    else:
        nf_buy_val = nf_buy or 0
    if isinstance(nf_sell, dict):
        nf_sell_val = nf_sell.get("raw", 0)
    else:
        nf_sell_val = nf_sell or 0
    
    foreign_flow = nf_buy_val - nf_sell_val
    board_type = sd.get("board") or stock.get("board_type") or stock.get("board")
    
    return {
        "symbol": symbol, "name": name, "price": price,
        "change": change_val, "percentage": pct, "volume": vol,
        "foreign_flow": foreign_flow, "board_type": board_type,
    }


class SchedulerManager:
    """Manages scheduled sync jobs."""
    
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.worker = DataIngestionWorker()
        self._sync_lock = asyncio.Lock()
        self._last_sync_status = {}
    
    def start(self):
        """Initialize and start the scheduler."""
        self.scheduler = AsyncIOScheduler(timezone=WIB)
        self._setup_jobs()
        self.scheduler.start()
        print(f"[Scheduler] Started at {datetime.now(WIB)}")
    
    def shutdown(self):
        """Shutdown scheduler gracefully."""
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
            print("[Scheduler] Shutdown complete")
    
    def _setup_jobs(self):
        """Configure all scheduled jobs."""
        settings = get_settings()
        
        # 1. Intraday sync every 5 minutes during market hours
        # Only for Tier 1-2 stocks
        self.scheduler.add_job(
            self._sync_intraday_tier12,
            trigger=IntervalTrigger(minutes=settings.intraday_sync_interval),
            id="intraday_tier12",
            name="Intraday sync Tier 1-2",
            replace_existing=True
        )
        
        # 2. Daily sync after market close
        # Parse time string "18:00"
        hour, minute = map(int, settings.daily_sync_time.split(":"))
        self.scheduler.add_job(
            self._sync_daily_all,
            trigger=CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri"),
            id="daily_all",
            name="Daily sync all stocks",
            replace_existing=True
        )
        
        # 3. Market status broadcast every minute during trading hours
        self.scheduler.add_job(
            self._broadcast_market_status,
            trigger=IntervalTrigger(minutes=1),
            id="market_status",
            name="Market status broadcast",
            replace_existing=True
        )
        
        # 4. Cleanup session tokens (every hour)
        self.scheduler.add_job(
            self._cleanup_expired_tokens,
            trigger=IntervalTrigger(hours=1),
            id="token_cleanup",
            name="Cleanup expired tokens",
            replace_existing=True
        )
        
        # 5. Watchlist sync (daily at 07:30 WIB, Monday-Friday)
        self.scheduler.add_job(
            self._sync_watchlists,
            trigger=CronTrigger(hour=7, minute=30, day_of_week="mon-fri"),
            id="watchlist_daily",
            name="Daily watchlist sync from Stockbit",
            replace_existing=True
        )
        
        print(f"[Scheduler] {len(self.scheduler.get_jobs())} jobs configured")
    
    async def _sync_intraday_tier12(self):
        """Sync intraday data for Tier 1-2 stocks (5-min cycle)."""
        # Check trading holiday
        if await is_trading_holiday():
            print(f"[Sync] Trading holiday, skipping intraday sync")
            return
        # Only run during market hours
        if not manager.is_market_open():
            return
        
        async with self._sync_lock:
            print(f"[Sync] Starting intraday sync: {datetime.now(WIB)}")
            
            # Get all active tokens from Redis
            tokens = await self._get_active_tokens()
            if not tokens:
                print("[Sync] No active tokens found")
                return
            
            # Use first available token
            session_id, token = tokens[0]
            
            # Tier 1-2 symbols
            tier_symbols = list(TIER1_SET | TIER2_SET)
            
            try:
                results = await self.worker.sync_tier_stocks(
                    tier_symbols, token, intraday=True
                )
                
                total_inserted = sum(v for v in results.values() if v > 0)
                errors = sum(1 for v in results.values() if v < 0)
                
                self._last_sync_status["intraday"] = {
                    "timestamp": datetime.now(WIB).isoformat(),
                    "symbols": len(tier_symbols),
                    "inserted": total_inserted,
                    "errors": errors
                }
                
                print(f"[Sync] Intraday complete: {total_inserted} records, {errors} errors")
                
            except Exception as e:
                print(f"[Sync] Intraday error: {e}")
                self._last_sync_status["intraday"] = {
                    "timestamp": datetime.now(WIB).isoformat(),
                    "error": str(e)
                }
    
    async def _sync_daily_all(self):
        """Sync daily data for all monitored stocks."""
        # This runs after market close
        if await is_trading_holiday():
            print(f"[Sync] Trading holiday, skipping daily sync")
            return
        print(f"[Sync] Starting daily sync: {datetime.now(WIB)}")
        
        async with self._sync_lock:
            tokens = await self._get_active_tokens()
            if not tokens:
                print("[Sync] No active tokens for daily sync")
                return
            
            session_id, token = tokens[0]
            
            # All Tier 1-3 symbols (monitor ~40-50 stocks)
            all_symbols = list(TIER1_SET | TIER2_SET)
            # Add some common Tier 3 if needed
            tier3_common = ["GOTO", "BUKA", "ADMR", "AMRT", "ACES", "MIKA", "HEAL"]
            all_symbols.extend(tier3_common)
            
            try:
                results = await self.worker.sync_tier_stocks(
                    all_symbols, token, intraday=False
                )
                
                total_inserted = sum(v for v in results.values() if v > 0)
                errors = sum(1 for v in results.values() if v < 0)
                
                self._last_sync_status["daily"] = {
                    "timestamp": datetime.now(WIB).isoformat(),
                    "symbols": len(all_symbols),
                    "inserted": total_inserted,
                    "errors": errors
                }
                
                print(f"[Sync] Daily complete: {total_inserted} records, {errors} errors")
                
            except Exception as e:
                print(f"[Sync] Daily error: {e}")
    
    async def _broadcast_market_status(self):
        """Broadcast market open/close status to WebSocket clients."""
        await broadcast_market_status()
    
    async def _cleanup_expired_tokens(self):
        """Remove expired session tokens from Redis."""
        # Redis handles TTL automatically, but we can clean orphaned entries
        redis = get_redis()
        # Keys are auto-expired by Redis, nothing to do manually
        print("[Cleanup] Token cleanup complete (Redis TTL handles expiration)")
    
    async def _get_active_tokens(self) -> list:
        """Get list of active (session_id, token) tuples from Redis.
        3-tier fallback:
        1. User sessions in Redis (stockbit_token:{session_id})
        2. System Redis key stockbit_token:system:primary
        3. .env settings.stockbit_token
        """
        redis = get_redis()
        tokens = []

        # Tier 1: User sessions
        session_keys = await redis.keys("stockbit_token:*")
        for key in session_keys:
            if key == "stockbit_token:system:primary":
                continue
            session_id = key.replace("stockbit_token:", "")
            token = await redis.get(key)
            if token:
                tokens.append((session_id, token))

        # Tier 2: System primary token
        sys_token = await redis.get("stockbit_token:system:primary")
        if sys_token:
            tokens.insert(0, ("system:primary", sys_token))

        # Tier 3: .env fallback (ensure Redis has it)
        if not tokens:
            settings = get_settings()
            if settings.stockbit_token:
                tokens.append(("env:settings", settings.stockbit_token))
                # Cache it in Redis for 1 day
                await redis.setex("stockbit_token:system:primary", 86400, settings.stockbit_token)

        return tokens

    async def _sync_watchlists(self):
        """
        Daily sync of market movers (Top Volume + Net Foreign Buy) from Stockbit.
        Runs at 07:30 WIB before market opens.
        """
        if await is_trading_holiday():
            print(f"[Watchlist] Trading holiday, skipping watchlist sync")
            return
        from ..ingestion.stockbit_client import StockbitClient, StockbitAPIError
        from ..database import execute
        from datetime import date
        import json
        
        print(f"[Watchlist] Starting daily sync: {datetime.now(WIB)}")
        
        tokens = await self._get_active_tokens()
        if not tokens:
            print("[Watchlist] No active tokens found")
            return
        
        session_id, token = tokens[0]
        today = date.today()
        results = {}
        
        async with StockbitClient(token) as client:
            # Top Volume
            try:
                tv_data = await client.get_market_mover("MOVER_TYPE_TOP_VOLUME")
                tv_inserted = 0
                for stock in tv_data:
                    try:
                        s = _extract_mover_stock(stock)
                        await execute("""
                            INSERT INTO market_movers 
                            (mover_type, symbol, name, price, change, percentage, 
                             volume, foreign_flow, board_type, data, date)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            ON CONFLICT (mover_type, symbol, date) DO UPDATE SET
                                name = EXCLUDED.name, price = EXCLUDED.price,
                                change = EXCLUDED.change, percentage = EXCLUDED.percentage,
                                volume = EXCLUDED.volume, foreign_flow = EXCLUDED.foreign_flow,
                                board_type = EXCLUDED.board_type, data = EXCLUDED.data,
                                updated_at = NOW()
                        """,
                            "top_volume",
                            s["symbol"], s["name"], s["price"], s["change"],
                            s["percentage"], s["volume"], s["foreign_flow"],
                            s["board_type"], json.dumps(stock), today
                        )
                        tv_inserted += 1
                    except Exception as e:
                        print(f"[Watchlist] Insert error top_volume {stock}: {e}")
                        continue
                results["top_volume"] = {"count": len(tv_data), "inserted": tv_inserted}
                print(f"[Watchlist] Top Volume: {tv_inserted} inserted")
            except StockbitAPIError as e:
                results["top_volume"] = {"error": str(e)}
                print(f"[Watchlist] Top Volume error: {e}")
            
            # Net Foreign Buy
            try:
                nf_data = await client.get_market_mover("MOVER_TYPE_NET_FOREIGN_BUY")
                nf_inserted = 0
                for stock in nf_data:
                    try:
                        s = _extract_mover_stock(stock)
                        await execute("""
                            INSERT INTO market_movers 
                            (mover_type, symbol, name, price, change, percentage, 
                             volume, foreign_flow, board_type, data, date)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            ON CONFLICT (mover_type, symbol, date) DO UPDATE SET
                                name = EXCLUDED.name, price = EXCLUDED.price,
                                change = EXCLUDED.change, percentage = EXCLUDED.percentage,
                                volume = EXCLUDED.volume, foreign_flow = EXCLUDED.foreign_flow,
                                board_type = EXCLUDED.board_type, data = EXCLUDED.data,
                                updated_at = NOW()
                        """,
                            "net_foreign_buy",
                            s["symbol"], s["name"], s["price"], s["change"],
                            s["percentage"], s["volume"], s["foreign_flow"],
                            s["board_type"], json.dumps(stock), today
                        )
                        nf_inserted += 1
                    except Exception as e:
                        print(f"[Watchlist] Insert error net_foreign_buy {stock}: {e}")
                        continue
                results["net_foreign_buy"] = {"count": len(nf_data), "inserted": nf_inserted}
                print(f"[Watchlist] Net Foreign Buy: {nf_inserted} inserted")
            except StockbitAPIError as e:
                results["net_foreign_buy"] = {"error": str(e)}
                print(f"[Watchlist] Net Foreign Buy error: {e}")
        
        self._last_sync_status["watchlist"] = {
            "timestamp": datetime.now(WIB).isoformat(),
            "results": results
        }
    


# Singleton
scheduler_manager = SchedulerManager()


def get_scheduler() -> SchedulerManager:
    """Get scheduler instance."""
    return scheduler_manager


def init_scheduler():
    """Initialize scheduler (call on startup)."""
    scheduler_manager.start()


def shutdown_scheduler():
    """Shutdown scheduler (call on shutdown)."""
    scheduler_manager.shutdown()
