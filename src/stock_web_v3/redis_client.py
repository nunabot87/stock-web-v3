"""Redis client for sessions and caching."""

import redis.asyncio as redis
from typing import Optional, Dict, Any
import json
from datetime import datetime

from .config import get_settings

redis_client: Optional[redis.Redis] = None


async def init_redis():
    """Initialize Redis connection."""
    global redis_client
    settings = get_settings()
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)


async def close_redis():
    """Close Redis connection."""
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None


def get_redis() -> redis.Redis:
    if redis_client is None:
        raise RuntimeError("Redis not initialized")
    return redis_client


# Session management
SESSION_PREFIX = "session:"
STOCKBIT_TOKEN_PREFIX = "stockbit_token:"


async def create_session(session_id: str, user_data: Dict[str, Any], ttl: int = 86400):
    """Create user session in Redis."""
    data = {
        **user_data,
        "created_at": datetime.utcnow().isoformat()
    }
    await get_redis().setex(
        f"{SESSION_PREFIX}{session_id}",
        ttl,
        json.dumps(data)
    )


async def get_session(session_id: str) -> Optional[Dict[str, Any]]:
    """Get session data."""
    data = await get_redis().get(f"{SESSION_PREFIX}{session_id}")
    return json.loads(data) if data else None


async def delete_session(session_id: str):
    """Delete session."""
    await get_redis().delete(f"{SESSION_PREFIX}{session_id}")


# Stockbit token management (per-session, user-controlled)
async def set_stockbit_token(session_id: str, token: str):
    """Store Stockbit JWT token for session."""
    settings = get_settings()
    await get_redis().setex(
        f"{STOCKBIT_TOKEN_PREFIX}{session_id}",
        settings.session_ttl,
        token
    )


async def get_stockbit_token(session_id: str) -> Optional[str]:
    """Get Stockbit token for session."""
    return await get_redis().get(f"{STOCKBIT_TOKEN_PREFIX}{session_id}")


async def delete_stockbit_token(session_id: str):
    """Remove Stockbit token."""
    await get_redis().delete(f"{STOCKBIT_TOKEN_PREFIX}{session_id}")


# Cache utilities
async def cache_set(key: str, value: Any, ttl: int = 300):
    """Set cache value."""
    await get_redis().setex(key, ttl, json.dumps(value))


async def cache_get(key: str) -> Optional[Any]:
    """Get cached value."""
    data = await get_redis().get(key)
    return json.loads(data) if data else None


async def cache_delete(key: str):
    """Delete cache key."""
    await get_redis().delete(key)