"""TimescaleDB connection pool and utilities."""

import asyncpg
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator, List, Dict, Any
import json
from decimal import Decimal
from datetime import datetime, date

from .config import get_settings

# Connection pool
db_pool: Optional[asyncpg.Pool] = None


class SafeJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal and datetime."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def safe_json_dumps(obj) -> str:
    return json.dumps(obj, cls=SafeJSONEncoder)


async def init_db():
    """Initialize connection pool."""
    global db_pool
    settings = get_settings()
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=settings.pool_size,
        max_inactive_connection_lifetime=300,
        command_timeout=60,
    )


async def close_db():
    """Close connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None


@asynccontextmanager
async def get_conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """Get database connection from pool."""
    if db_pool is None:
        raise RuntimeError("Database not initialized")
    async with db_pool.acquire() as conn:
        yield conn


async def fetchone(query: str, *args) -> Optional[Dict[str, Any]]:
    """Fetch single row."""
    async with get_conn() as conn:
        row = await conn.fetchrow(query, *args)
        return dict(row) if row else None


async def fetchall(query: str, *args) -> List[Dict[str, Any]]:
    """Fetch all rows."""
    async with get_conn() as conn:
        rows = await conn.fetch(query, *args)
        return [dict(row) for row in rows]


async def execute(query: str, *args) -> str:
    """Execute query, return status."""
    async with get_conn() as conn:
        return await conn.execute(query, *args)