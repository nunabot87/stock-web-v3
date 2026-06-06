"""API Routes - FastAPI application setup."""

import json

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import time

from ..config import get_settings
from ..database import init_db, close_db, SafeJSONEncoder
from ..redis_client import init_redis, close_redis
from ..scheduler import init_scheduler, shutdown_scheduler

from ..auth.router import router as auth_router
from ..websocket.hub import router as ws_router
from ..analysis.engine import analysis_engine


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    settings = get_settings()
    print(f"[Startup] {settings.app_name} v{app.version}")
    
    # Initialize database
    await init_db()
    print("[Startup] Database connected")
    
    # Initialize Redis
    await init_redis()
    print("[Startup] Redis connected")
    
    # Initialize scheduler
    init_scheduler()
    print("[Startup] APScheduler initialized")
    
    yield
    
    # Shutdown
    print("[Shutdown] Cleaning up...")
    shutdown_scheduler()
    await close_redis()
    await close_db()
    print("[Shutdown] Complete")


# Create FastAPI app
def create_app() -> FastAPI:
    """Factory function to create FastAPI application."""
    settings = get_settings()
    
    app = FastAPI(
        title=settings.app_name,
        version="3.0.0",
        description="Real-time IDX Stock Monitoring System v3",
        lifespan=lifespan
    )
    
    # CORS middleware
    origins = [
        "https://stock.web.id",
        "https://*.stock.web.id",
        "http://localhost:3000",
        "http://localhost:8000",
    ]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Session-ID"]
    )
    
    # Request timing middleware
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    
    # Include routers
    app.include_router(auth_router)
    app.include_router(ws_router)
    
    # Stock data endpoints
    app.include_router(stock_router, prefix="/api/v3/stocks", tags=["stocks"])
    app.include_router(analysis_router, prefix="/api/v3/analysis", tags=["analysis"])
    app.include_router(watchlist_router, prefix="/api/v3/watchlist", tags=["watchlist"])
    app.include_router(scheduler_router, prefix="/api/v3/scheduler", tags=["scheduler"])
    
    return app


# Stock data router
from fastapi import APIRouter, Query

stock_router = APIRouter()


@stock_router.get("/list")
async def get_stock_list(tier: int = Query(None, ge=1, le=3)):
    """Get list of monitored stocks by tier."""
    from ..config import TIER1_SET, TIER2_SET
    
    stocks = []
    
    if tier is None or tier == 1:
        for s in TIER1_SET:
            stocks.append({"symbol": s, "tier": 1, "name": None})
    
    if tier is None or tier == 2:
        for s in TIER2_SET:
            stocks.append({"symbol": s, "tier": 2, "name": None})
    
    return {
        "count": len(stocks),
        "tier": tier,
        "stocks": stocks
    }


@stock_router.get("/{symbol}/prices")
async def get_stock_prices(symbol: str, days: int = Query(30, ge=1, le=365)):
    """Get historical prices for a stock."""
    from ..database import fetchall
    
    query = """
        SELECT symbol, timestamp, open, high, low, close, volume, change, percentage
        FROM stock_prices_daily
        WHERE symbol = $1
        AND timestamp >= NOW() - INTERVAL '%s days'
        ORDER BY timestamp DESC
        LIMIT $2
    """ % days
    
    rows = await fetchall(query, symbol, days)
    
    return {
        "symbol": symbol,
        "count": len(rows),
        "data": rows
    }


# Analysis router
analysis_router = APIRouter()


@analysis_router.get("/{symbol}")
async def analyze_stock(symbol: str):
    """Get full analysis for a stock."""
    result = await analysis_engine.analyze_stock(symbol)
    return result


@analysis_router.get("/{symbol}/indicators")
async def get_indicators(symbol: str):
    """Get technical indicators only."""
    from ..analysis.engine import PricePoint
    
    prices_data = await analysis_engine.get_historical_prices(symbol)
    indicators = analysis_engine.calculate_indicators(prices_data)
    
    return {
        "symbol": symbol,
        "timestamp": indicators.timestamp.isoformat() if indicators.timestamp else None,
        "rsi": indicators.rsi,
        "macd": indicators.macd,
        "macd_signal": indicators.macd_signal,
        "sma_20": indicators.sma_20,
        "sma_50": indicators.sma_50,
        "bb_upper": indicators.bb_upper,
        "bb_lower": indicators.bb_lower
    }


@analysis_router.get("/{symbol}/score")
async def get_score(symbol: str):
    """Get scoring result only."""
    prices_data = await analysis_engine.get_historical_prices(symbol)
    indicators = analysis_engine.calculate_indicators(prices_data)
    score = analysis_engine.calculate_score(prices_data, indicators)
    
    return {
        "symbol": symbol,
        "score": score.total_score,
        "verdict": score.verdict,
        "confidence": score.confidence,
        "recommendation": {
            "entry": score.entry_price,
            "target": score.target_price,
            "stop_loss": score.stop_loss,
            "rr_ratio": score.risk_reward_ratio
        }
    }


# Scheduler router
scheduler_router = APIRouter()


# Watchlist router
watchlist_router = APIRouter()


@watchlist_router.get("/top-volume")
async def get_top_volume_watchlist():
    """Get Top Volume watchlist data (cached from Stockbit, updated daily)."""
    from ..redis_client import get_redis
    from ..database import fetchall
    
    # 1. Try Redis cache first (expires in 1 hour)
    redis = get_redis()
    cached = await redis.get("watchlist:top_volume")
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    
    # 2. Try database (last 2 days)
    rows = await fetchall("""
        SELECT id, mover_type, symbol, name, price, change, percentage, 
               volume, foreign_flow, board_type, data, date, created_at
        FROM market_movers
        WHERE mover_type = 'top_volume'
          AND date >= CURRENT_DATE - INTERVAL '2 days'
        ORDER BY volume DESC
        LIMIT 50
    """)
    
    if rows:
        # Normalize response data for frontend clarity
        stocks = []
        for row in rows:
            stocks.append({
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "price": float(row.get("price", 0)) if row.get("price") is not None else 0,
                "change": float(row.get("change", 0)) if row.get("change") is not None else 0,
                "change_pct": float(row.get("percentage", 0)) if row.get("percentage") is not None else 0,
                "volume": int(row.get("volume", 0)) if row.get("volume") is not None else 0,
                "foreign_flow": float(row.get("foreign_flow", 0)) if row.get("foreign_flow") is not None else 0,
                "board_type": row.get("board_type"),
                "date": str(row.get("date")) if row.get("date") else None
            })
        result = {
            "watchlist": "top_volume",
            "source": "database",
            "count": len(stocks),
            "date": str(rows[0].get("date")) if rows else None,
            "stocks": stocks
        }
        # Cache for 1 hour
        await redis.setex("watchlist:top_volume", 3600, json.dumps(result, cls=SafeJSONEncoder))
        return result
    
    # 3. Fallback: return empty with metadata
    return {
        "watchlist": "top_volume",
        "source": "none",
        "count": 0,
        "date": None,
        "stocks": [],
        "message": "No data available. Run sync to populate."
    }


@watchlist_router.get("/net-foreign-buy")
async def get_net_foreign_buy_watchlist():
    """Get Net Foreign Buy watchlist data (cached from Stockbit, updated daily)."""
    from ..redis_client import get_redis
    from ..database import fetchall
    
    # 1. Try Redis cache first (expires in 1 hour)
    redis = get_redis()
    cached = await redis.get("watchlist:net_foreign_buy")
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    
    # 2. Try database (last 2 days)
    rows = await fetchall("""
        SELECT id, mover_type, symbol, name, price, change, percentage, 
               volume, foreign_flow, board_type, data, date, created_at
        FROM market_movers
        WHERE mover_type = 'net_foreign_buy'
          AND date >= CURRENT_DATE - INTERVAL '2 days'
        ORDER BY foreign_flow DESC NULLS LAST
        LIMIT 50
    """)
    
    if rows:
        # Normalize response data for frontend clarity
        stocks = []
        for row in rows:
            stocks.append({
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "price": float(row.get("price", 0)) if row.get("price") is not None else 0,
                "change": float(row.get("change", 0)) if row.get("change") is not None else 0,
                "change_pct": float(row.get("percentage", 0)) if row.get("percentage") is not None else 0,
                "volume": int(row.get("volume", 0)) if row.get("volume") is not None else 0,
                "foreign_flow": float(row.get("foreign_flow", 0)) if row.get("foreign_flow") is not None else 0,
                "board_type": row.get("board_type"),
                "date": str(row.get("date")) if row.get("date") else None
            })
        result = {
            "watchlist": "net_foreign_buy",
            "source": "database",
            "count": len(stocks),
            "date": str(rows[0].get("date")) if rows else None,
            "stocks": stocks
        }
        await redis.setex("watchlist:net_foreign_buy", 3600, json.dumps(result, cls=SafeJSONEncoder))
        return result
    
    return {
        "watchlist": "net_foreign_buy",
        "source": "none",
        "count": 0,
        "date": None,
        "stocks": [],
        "message": "No data available. Run sync to populate."
    }




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
    
    # change can be dict {value, percentage} or scalar
    if isinstance(change_obj, dict):
        change_val = change_obj.get("value")
        pct = change_obj.get("percentage")
    else:
        change_val = change_obj
        pct = stock.get("percentage") or stock.get("change_pct")
    
    # volume can be dict {raw, formatted} or scalar
    if isinstance(volume_obj, dict):
        vol = volume_obj.get("raw")
    else:
        vol = volume_obj
    
    # net foreign flow: net_foreign_buy - net_foreign_sell
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
        "symbol": symbol,
        "name": name,
        "price": price,
        "change": change_val,
        "percentage": pct,
        "volume": vol,
        "foreign_flow": foreign_flow,
        "board_type": board_type,
    }

@watchlist_router.post("/sync")
async def sync_watchlists():
    """
    Manually trigger watchlist sync from Stockbit API.
    Fetches both Top Volume and Net Foreign Buy, stores to database.
    """
    from ..ingestion.stockbit_client import StockbitClient, StockbitAPIError
    from ..redis_client import get_redis
    from ..database import execute
    from datetime import date
    
    # Get active token
    redis = get_redis()
    token = await redis.get("stockbit_token:system:primary")
    if not token:
        # Fallback: find any available token
        keys = await redis.keys("stockbit_token:*")
        for key in keys:
            token = await redis.get(key)
            if token:
                break
    
    if not token:
        raise HTTPException(status_code=503, detail="No Stockbit token available")
    
    today = date.today()
    results = {}
    
    async with StockbitClient(token) as client:
        # Sync Top Volume
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
                            name = EXCLUDED.name,
                            price = EXCLUDED.price,
                            change = EXCLUDED.change,
                            percentage = EXCLUDED.percentage,
                            volume = EXCLUDED.volume,
                            foreign_flow = EXCLUDED.foreign_flow,
                            board_type = EXCLUDED.board_type,
                            data = EXCLUDED.data,
                            updated_at = NOW()
                    """,
                        "top_volume",
                        s["symbol"], s["name"], s["price"], s["change"],
                        s["percentage"], s["volume"], s["foreign_flow"],
                        s["board_type"], json.dumps(stock), today
                    )
                    tv_inserted += 1
                except Exception as e:
                    print(f"[Watchlist] Error inserting top_volume {stock}: {e}")
                    continue
            
            results["top_volume"] = {"count": len(tv_data), "inserted": tv_inserted}
            
            # Cache result
            cache_data = {
                "watchlist": "top_volume",
                "source": "stockbit",
                "count": len(tv_data),
                "date": str(today),
                "stocks": tv_data
            }
            await redis.setex("watchlist:top_volume", 3600, json.dumps(cache_data, cls=SafeJSONEncoder))
            
        except StockbitAPIError as e:
            results["top_volume"] = {"error": str(e)}
        
        # Sync Net Foreign Buy
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
                            name = EXCLUDED.name,
                            price = EXCLUDED.price,
                            change = EXCLUDED.change,
                            percentage = EXCLUDED.percentage,
                            volume = EXCLUDED.volume,
                            foreign_flow = EXCLUDED.foreign_flow,
                            board_type = EXCLUDED.board_type,
                            data = EXCLUDED.data,
                            updated_at = NOW()
                    """,
                        "net_foreign_buy",
                        s["symbol"], s["name"], s["price"], s["change"],
                        s["percentage"], s["volume"], s["foreign_flow"],
                        s["board_type"], json.dumps(stock), today
                    )
                    nf_inserted += 1
                except Exception as e:
                    print(f"[Watchlist] Error inserting net_foreign_buy {stock}: {e}")
                    continue
            
            results["net_foreign_buy"] = {"count": len(nf_data), "inserted": nf_inserted}
            
            cache_data = {
                "watchlist": "net_foreign_buy",
                "source": "stockbit",
                "count": len(nf_data),
                "date": str(today),
                "stocks": nf_data
            }
            await redis.setex("watchlist:net_foreign_buy", 3600, json.dumps(cache_data, cls=SafeJSONEncoder))
            
        except StockbitAPIError as e:
            results["net_foreign_buy"] = {"error": str(e)}
    
    return {
        "message": "Watchlist sync complete",
        "date": str(today),
        "results": results
    }


@scheduler_router.get("/status")
async def get_scheduler_status():
    """Get scheduler status and last sync info."""
    from ..scheduler import get_scheduler
    return get_scheduler().get_status()


@scheduler_router.post("/trigger/{job_id}")
async def trigger_job(job_id: str):
    """Manually trigger a scheduled job."""
    from ..scheduler import get_scheduler
    
    scheduler = get_scheduler()
    job = scheduler.scheduler.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    # Execute job immediately
    job.modify(next_run_time=datetime.now())
    
    return {"message": f"Job {job_id} triggered", "job_id": job_id}


from datetime import datetime


# Health check endpoint
@stock_router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "3.0.0",
        "timestamp": datetime.now().isoformat()
    }