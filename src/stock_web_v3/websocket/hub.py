"""WebSocket Hub - Tier-based broadcasting for real-time stock data."""

import asyncio
import json
from typing import Dict, Set, List, Optional, Any
from datetime import datetime, time
import pytz
from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from starlette.websockets import WebSocketState

from ..config import get_settings, get_stock_tier, TIER1_SET, TIER2_SET
from ..redis_client import get_session

# WIB timezone for BEI market hours
WIB = pytz.timezone('Asia/Jakarta')

router = APIRouter(prefix="/ws", tags=["websocket"])

# Connection management
class ConnectionManager:
    """Manages WebSocket connections grouped by tier."""
    
    def __init__(self):
        # Tier 1 connections (highest priority, ~10 stocks)
        self.tier1_connections: Dict[str, WebSocket] = {}
        # Tier 2 connections (~15-20 stocks)
        self.tier2_connections: Dict[str, WebSocket] = {}
        # Tier 3 connections (on-demand, lower frequency)
        self.tier3_connections: Dict[str, WebSocket] = {}
        
        # Subscription tracking: session_id -> set of symbols
        self.subscriptions: Dict[str, Set[str]] = {}
        
        # Last broadcast data cache
        self.last_broadcast: Dict[str, Dict[str, Any]] = {}
        
        # Broadcast statistics
        self.stats = {
            "messages_sent": 0,
            "connections_total": 0,
            "connections_active": 0
        }
    
    def is_market_open(self) -> bool:
        """Check if BEI market is currently open."""
        now = datetime.now(WIB)
        current_time = now.time()
        weekday = now.weekday()
        
        # Weekend
        if weekday >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Pre-open: 08:45 - 09:00 (no trading, just preparation)
        # Session 1: 09:00 - 11:30
        # Break: 11:30 - 13:30 (PAUSE - no data)
        # Session 2: 13:30 - 16:00
        
        if current_time < time(8, 45) or current_time >= time(16, 0):
            return False
        
        # Lunch break - NO DATA
        if time(11, 30) <= current_time < time(13, 30):
            return False
        
        return True
    
    async def connect(self, websocket: WebSocket, session_id: str, tier: int = 3):
        """Accept connection and store by tier."""
        await websocket.accept()
        
        # Store in appropriate tier bucket
        if tier == 1:
            self.tier1_connections[session_id] = websocket
        elif tier == 2:
            self.tier2_connections[session_id] = websocket
        else:
            self.tier3_connections[session_id] = websocket
        
        self.subscriptions[session_id] = set()
        self.stats["connections_total"] += 1
        self.stats["connections_active"] = (
            len(self.tier1_connections) +
            len(self.tier2_connections) +
            len(self.tier3_connections)
        )
        
        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
            "tier": tier,
            "market_open": self.is_market_open(),
            "timestamp": datetime.now(WIB).isoformat()
        })
    
    def disconnect(self, session_id: str):
        """Remove connection."""
        self.tier1_connections.pop(session_id, None)
        self.tier2_connections.pop(session_id, None)
        self.tier3_connections.pop(session_id, None)
        self.subscriptions.pop(session_id, None)
        
        self.stats["connections_active"] = (
            len(self.tier1_connections) +
            len(self.tier2_connections) +
            len(self.tier3_connections)
        )
    
    def subscribe(self, session_id: str, symbols: List[str]):
        """Subscribe client to symbols."""
        if session_id in self.subscriptions:
            self.subscriptions[session_id].update(symbols)
    
    def unsubscribe(self, session_id: str, symbols: List[str] = None):
        """Unsubscribe from symbols (or all if None)."""
        if session_id in self.subscriptions:
            if symbols:
                self.subscriptions[session_id].difference_update(symbols)
            else:
                self.subscriptions[session_id].clear()
    
    async def broadcast_tier(self, tier: int, message: Dict[str, Any]):
        """Broadcast to all connections in a tier."""
        if tier == 1:
            connections = self.tier1_connections
        elif tier == 2:
            connections = self.tier2_connections
        else:
            connections = self.tier3_connections
        
        disconnected = []
        for session_id, websocket in connections.items():
            try:
                # Check if websocket is still open
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json(message)
                    self.stats["messages_sent"] += 1
            except Exception:
                disconnected.append(session_id)
        
        # Clean up dead connections
        for sid in disconnected:
            self.disconnect(sid)
    
    async def send_to_session(self, session_id: str, message: Dict[str, Any]):
        """Send message to specific session."""
        websocket = (
            self.tier1_connections.get(session_id) or
            self.tier2_connections.get(session_id) or
            self.tier3_connections.get(session_id)
        )
        
        if websocket and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json(message)
            except Exception:
                self.disconnect(session_id)
    
    def get_tier_counts(self) -> Dict[str, int]:
        """Get connection counts by tier."""
        return {
            "tier1": len(self.tier1_connections),
            "tier2": len(self.tier2_connections),
            "tier3": len(self.tier3_connections),
            "total": self.stats["connections_active"]
        }


# Global connection manager
manager = ConnectionManager()


@router.websocket("/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint with tier-based broadcasting.
    URL: /ws/{session_id}
    """
    # Validate session
    session = await get_session(session_id)
    if not session:
        await websocket.close(code=4001, reason="Invalid session")
        return
    
    # Determine tier from session (default to basic = tier 3)
    tier = 1 if session.get("tier") == "premium" else 3
    
    await manager.connect(websocket, session_id, tier)
    
    try:
        while True:
            # Receive and process client messages
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "subscribe":
                    symbols = message.get("symbols", [])
                    manager.subscribe(session_id, symbols)
                    await websocket.send_json({
                        "type": "subscribed",
                        "symbols": symbols,
                        "count": len(symbols)
                    })
                
                elif msg_type == "unsubscribe":
                    symbols = message.get("symbols", [])
                    manager.unsubscribe(session_id, symbols)
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "symbols": symbols
                    })
                
                elif msg_type == "ping":
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": datetime.now(WIB).isoformat(),
                        "market_open": manager.is_market_open()
                    })
                
                elif msg_type == "get_stats":
                    await websocket.send_json({
                        "type": "stats",
                        "connections": manager.get_tier_counts(),
                        "subscribed_symbols": list(manager.subscriptions.get(session_id, [])),
                        "market_open": manager.is_market_open()
                    })
                
                else:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Unknown message type: {msg_type}"
                    })
                    
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON"
                })
                
    except WebSocketDisconnect:
        manager.disconnect(session_id)
    except Exception as e:
        manager.disconnect(session_id)


# Broadcast helpers for external use
async def broadcast_prices(prices: Dict[str, Dict[str, Any]]):
    """Broadcast price updates to appropriate tiers."""
    tier1_data = {}
    tier2_data = {}
    
    for symbol, data in prices.items():
        tier = get_stock_tier(symbol)
        if tier == 1:
            tier1_data[symbol] = data
        elif tier == 2:
            tier2_data[symbol] = data
    
    # Broadcast to tier 1 (highest frequency)
    if tier1_data:
        await manager.broadcast_tier(1, {
            "type": "price_update",
            "data": tier1_data,
            "timestamp": datetime.now(WIB).isoformat()
        })
    
    # Broadcast to tier 2
    if tier2_data:
        await manager.broadcast_tier(2, {
            "type": "price_update",
            "data": tier2_data,
            "timestamp": datetime.now(WIB).isoformat()
        })


async def broadcast_market_status():
    """Broadcast market status to all connections."""
    status = {
        "type": "market_status",
        "open": manager.is_market_open(),
        "timestamp": datetime.now(WIB).isoformat()
    }
    
    await manager.broadcast_tier(1, status)
    await manager.broadcast_tier(2, status)
    await manager.broadcast_tier(3, status)


def get_ws_stats() -> Dict[str, Any]:
    """Get WebSocket statistics."""
    return {
        "connections": manager.get_tier_counts(),
        "messages_sent": manager.stats["messages_sent"],
        "market_open": manager.is_market_open()
    }