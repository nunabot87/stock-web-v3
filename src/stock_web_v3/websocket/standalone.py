"""WebSocket Standalone Server - Separate process for WebSocket hub."""

import asyncio
import json
import signal
import sys
from functools import partial
from datetime import datetime, timedelta

import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

# Import websocket hub components
from ..config import get_settings, get_stock_tier
from ..redis_client import init_redis, close_redis, get_redis
from ..ingestion.stockbit_client import StockbitClient

WIB = pytz.timezone('Asia/Jakarta')


class StandaloneConnectionManager:
    """Standalone WebSocket connection manager with price broadcasting."""
    
    def __init__(self):
        self.tier1_connections: dict = {}
        self.tier2_connections: dict = {}
        self.tier3_connections: dict = {}
        self.subscriptions: dict = {}
        self.running = False
    
    def is_market_open(self) -> bool:
        """Check if BEI market is currently open."""
        from datetime import time
        now = datetime.now(WIB)
        current_time = now.time()
        weekday = now.weekday()
        
        if weekday >= 5:  # Weekend
            return False
        
        if current_time < time(8, 45) or current_time >= time(16, 0):
            return False
        
        if time(11, 30) <= current_time < time(13, 30):
            return False
        
        return True
    
    async def connect(self, websocket: WebSocket, session_id: str, tier: int = 3):
        """Accept new WebSocket connection."""
        await websocket.accept()
        
        if tier == 1:
            self.tier1_connections[session_id] = websocket
        elif tier == 2:
            self.tier2_connections[session_id] = websocket
        else:
            self.tier3_connections[session_id] = websocket
        
        self.subscriptions[session_id] = set()
        
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
            "tier": tier,
            "market_open": self.is_market_open(),
            "timestamp": datetime.now(WIB).isoformat()
        })
        print(f"[WS] Client connected: {session_id[:8]}... (Tier {tier})")
    
    def disconnect(self, session_id: str):
        """Remove disconnected client."""
        self.tier1_connections.pop(session_id, None)
        self.tier2_connections.pop(session_id, None)
        self.tier3_connections.pop(session_id, None)
        self.subscriptions.pop(session_id, None)
        print(f"[WS] Client disconnected: {session_id[:8]}...")
    
    async def broadcast_tier(self, tier: int, message: dict):
        """Broadcast message to all connections in a tier."""
        if tier == 1:
            connections = self.tier1_connections
        elif tier == 2:
            connections = self.tier2_connections
        else:
            connections = self.tier3_connections
        
        disconnected = []
        for session_id, ws in connections.items():
            try:
                await ws.send_json(message)
            except Exception:
                disconnected.append(session_id)
        
        for sid in disconnected:
            self.disconnect(sid)
    
    async def broadcast_to_all(self, message: dict):
        """Broadcast to all connected clients."""
        await self.broadcast_tier(1, message)
        await self.broadcast_tier(2, message)
        await self.broadcast_tier(3, message)
    
    def get_stats(self) -> dict:
        """Get connection statistics."""
        return {
            "tier1": len(self.tier1_connections),
            "tier2": len(self.tier2_connections),
            "tier3": len(self.tier3_connections),
            "total": len(self.tier1_connections) + len(self.tier2_connections) + len(self.tier3_connections),
            "market_open": self.is_market_open()
        }


# Create standalone manager
standalone_manager = StandaloneConnectionManager()


async def fetch_and_broadcast_prices():
    """Background task to fetch and broadcast stock prices."""
    redis = get_redis()
    settings = get_settings()
    
    while standalone_manager.running:
        try:
            # Only broadcast during market hours
            if not standalone_manager.is_market_open():
                await asyncio.sleep(60)  # Check every minute when closed
                continue
            
            # Get token from Redis
            # Prioritize system:primary, filter out metadata keys
            token_keys = await redis.keys("stockbit_token:*")
            token = None
            for key in token_keys:
                if key == "stockbit_token:system:primary":
                    token = await redis.get(key)
                    if token:
                        break
            if not token:
                for key in token_keys:
                    if key in ("stockbit_token:system:primary", "stockbit_token:system:meta"):
                        continue
                    token = await redis.get(key)
                    if token:
                        break
            if not token:
                await asyncio.sleep(5)
                continue
            
            # Get Tier 1 & 2 symbols for real-time updates
            from ..config import TIER1_SET, TIER2_SET
            tier1_symbols = list(TIER1_SET)
            tier2_symbols = list(TIER2_SET)
            
            # Fetch prices using StockbitClient
            async with StockbitClient(token) as client:
                # Get real-time data for Tier 1 (highest frequency)
                tier1_data = {}
                for symbol in tier1_symbols[:5]:  # Batch first 5
                    try:
                        # Get last 15 min of intraday data for real-time
                        now = datetime.now(WIB)
                        to_ts = int(now.timestamp())
                        from_ts = to_ts - (15 * 60)
                        intraday = await client.get_intraday(symbol, from_ts, to_ts)
                        if intraday and len(intraday) > 0:
                            latest = intraday[-1]
                            tier1_data[symbol] = {
                                "price": latest.get("close", latest.get("c", 0)),
                                "volume": latest.get("volume", latest.get("v", 0)),
                                "timestamp": latest.get("unix_timestamp", latest.get("t", 0))
                            }
                    except Exception as e:
                        print(f"[WS] Error fetching {symbol}: {e}")
                
                if tier1_data:
                    await standalone_manager.broadcast_tier(1, {
                        "type": "price_update",
                        "data": tier1_data,
                        "tier": 1,
                        "timestamp": datetime.now(WIB).isoformat()
                    })
                
                # Tier 2 - less frequent
                if standalone_manager.tier2_connections:
                    tier2_data = {}
                    for symbol in tier2_symbols[:5]:
                        try:
                            # Get last 15 min of intraday data for real-time
                            now = datetime.now(WIB)
                            to_ts = int(now.timestamp())
                            from_ts = to_ts - (15 * 60)
                            intraday = await client.get_intraday(symbol, from_ts, to_ts)
                            if intraday and len(intraday) > 0:
                                latest = intraday[-1]
                                tier2_data[symbol] = {
                                    "price": latest.get("close", latest.get("c", 0)),
                                    "volume": latest.get("volume", latest.get("v", 0)),
                                    "timestamp": latest.get("unix_timestamp", latest.get("t", 0))
                                }
                        except Exception:
                            pass
                    
                    if tier2_data:
                        await standalone_manager.broadcast_tier(2, {
                            "type": "price_update",
                            "data": tier2_data,
                            "tier": 2,
                            "timestamp": datetime.now(WIB).isoformat()
                        })
            
            # Wait 5 seconds before next fetch (Tier 1: 5s, Tier 2: 30s)
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"[WS] Broadcast error: {e}")
            await asyncio.sleep(10)


async def periodic_market_broadcast():
    """Broadcast market status every minute."""
    while standalone_manager.running:
        try:
            await standalone_manager.broadcast_to_all({
                "type": "market_status",
                "open": standalone_manager.is_market_open(),
                "timestamp": datetime.now(WIB).isoformat(),
                "connections": standalone_manager.get_stats()
            })
            await asyncio.sleep(60)
        except Exception as e:
            print(f"[WS] Market broadcast error: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage WebSocket server lifecycle."""
    print("[WS] Starting standalone WebSocket server...")
    await init_redis()
    standalone_manager.running = True
    
    # Start background tasks
    tasks = [
        asyncio.create_task(fetch_and_broadcast_prices()),
        asyncio.create_task(periodic_market_broadcast())
    ]
    
    yield
    
    # Shutdown
    print("[WS] Shutting down WebSocket server...")
    standalone_manager.running = False
    for task in tasks:
        task.cancel()
    await close_redis()
    print("[WS] Shutdown complete")


# Create FastAPI app for standalone WebSocket server
app = FastAPI(title="Stock Web V3 - WebSocket Hub", lifespan=lifespan)


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint - requires valid session."""
    from starlette.websockets import WebSocketState
    
    # Validate session via Redis
    redis = get_redis()
    session_data = await redis.get(f"session:{session_id}")
    
    if not session_data:
        await websocket.close(code=4001, reason="Invalid session")
        return
    
    session = json.loads(session_data)
    tier = 1 if session.get("tier") == "premium" else 3
    
    await standalone_manager.connect(websocket, session_id, tier)
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "ping":
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": datetime.now(WIB).isoformat()
                    })
                elif msg_type == "subscribe":
                    symbols = message.get("symbols", [])
                    standalone_manager.subscriptions[session_id].update(symbols)
                    await websocket.send_json({
                        "type": "subscribed",
                        "symbols": list(standalone_manager.subscriptions[session_id])
                    })
                elif msg_type == "get_stats":
                    await websocket.send_json({
                        "type": "stats",
                        "connections": standalone_manager.get_stats()
                    })
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON"})
    except WebSocketDisconnect:
        standalone_manager.disconnect(session_id)
    except Exception as e:
        print(f"[WS] Client error: {e}")
        standalone_manager.disconnect(session_id)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "websocket-hub",
        "connections": standalone_manager.get_stats(),
        "timestamp": datetime.now(WIB).isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "stock_web_v3.websocket.standalone:app",
        host="127.0.0.1",
        port=8001,
        reload=False,
        workers=1
    )
