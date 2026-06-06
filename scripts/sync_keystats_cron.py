#!/opt/stock-web-v3/venv/bin/python3
"""
Keystats Ratio Sync — Production v3
====================================
Token source: Redis system:primary (Chrome Extension proxy).
DB connection: Unix socket peer auth (no password).
"""
import asyncio, asyncpg, httpx, os, sys, argparse, json, time
from datetime import datetime, timedelta

# ─── Token source hierarchy (matches stock-web-v3 auth) ──────────
# 1. Redis `stockbit_token:system:primary` (Chrome Extension proxy)
# 2. .env `STOCKBIT_TOKEN`
# 3. Fallback: ask user (cronjob will fail loud)

# Load .env if exists
env_path = '/opt/stock-web-v3/.env'
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k, v)

# Use DATABASE_URL from .env if available, else build manually with Unix socket default
DB_URL = os.getenv('DATABASE_URL')
if not DB_URL:
    DB_PASSWORD = os.getenv('DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', ''))
    DB_HOST = os.getenv('DB_HOST', '/var/run/postgresql')  # Unix socket for peer auth
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'stockdb')
    DB_USER = os.getenv('DB_USER', 'stock_app')
    if DB_HOST.startswith('/'):
        # Unix socket — no password needed for peer auth
        DB_URL = f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

KEYSTATS_URL = 'https://exodus.stockbit.com/keystats/ratio/v1/%s'
MAX_CONCURRENT = 5
HTTP_TIMEOUT = 8
BATCH_SIZE = 100
MIN_AGE_HOURS = 6

async def _get_token_redis():
    try:
        import redis.asyncio as redis_lib
        r = redis_lib.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        token = await r.get('stockbit_token:system:primary')
        await r.aclose()  # Redis 5.0+ use aclose
        return token
    except Exception:
        return None

async def get_token(pool: asyncpg.Pool = None) -> str:
    """Get token using same hierarchy as stock-web-v3."""
    # Priority 1: Redis system:primary
    token = await _get_token_redis()
    if token:
        return token
    
    # Priority 2: .env / environment
    env_token = os.getenv('STOCKBIT_TOKEN')
    if env_token:
        return env_token
    
    # Priority 3: Legacy system_meta table (deprecated, may be stale)
    if pool:
        row = await pool.fetchrow("SELECT value FROM system_meta WHERE key='stockbit_token'")
        if row:
            return row['value']
    
    raise RuntimeError("No valid token found. Ensure Redis has stockbit_token:system:primary or set STOCKBIT_TOKEN env var.")

async def sync_one(pool: asyncpg.Pool, client: httpx.AsyncClient, symbol: str, token: str) -> dict:
    url = KEYSTATS_URL % symbol
    t0 = time.time()
    try:
        resp = await client.get(
            url,
            headers={'Authorization': f'Bearer {token}', 'Accept':'application/json'},
            params={'year_limit':0},
            timeout=HTTP_TIMEOUT
        )
    except httpx.TimeoutException:
        return {'symbol': symbol, 'status': 'timeout', 'elapsed': round(time.time()-t0, 2)}
    except Exception as e:
        return {'symbol': symbol, 'status': 'error', 'error': str(e)[:200], 'elapsed': round(time.time()-t0, 2)}
    
    if resp.status_code != 200:
        return {'symbol': symbol, 'status': resp.status_code, 'error': resp.text[:300], 'elapsed': round(time.time()-t0, 2)}
    
    try:
        payload = resp.json()
    except Exception as e:
        return {'symbol': symbol, 'status': 'parse_error', 'error': str(e), 'elapsed': round(time.time()-t0, 2)}

    data = payload.get('data', {})
    categories = data.get('closure_fin_items_results', [])
    
    # Bulk delete + insert in single transaction
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM stock_keystats WHERE symbol = $1", symbol)
            
            inserted = 0
            for cat in categories:
                cat_name = cat.get('keystats_name', 'Unknown')
                for item in cat.get('fin_name_results', []):
                    fitem = item.get('fitem', {})
                    name = fitem.get('name', '')
                    value = fitem.get('value', '')
                    await conn.execute("""
                        INSERT INTO stock_keystats (symbol, category, item_name, value, raw_value)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (symbol, category, item_name) DO UPDATE
                        SET value = EXCLUDED.value,
                            raw_value = EXCLUDED.raw_value,
                            updated_at = CURRENT_TIMESTAMP
                    """, symbol, cat_name, name, value, json.dumps(fitem))
                    inserted += 1
            
            # Update raw cache
            await conn.execute("""
                INSERT INTO stock_keystats_raw (symbol, full_data, fetched_at)
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE
                SET full_data = EXCLUDED.full_data,
                    fetched_at = CURRENT_TIMESTAMP
            """, symbol, json.dumps(data))

    return {
        'symbol': symbol,
        'status': 200,
        'categories': len(categories),
        'items': inserted,
        'elapsed': round(time.time()-t0, 2)
    }

async def run_batch(pool: asyncpg.Pool, symbols: list, token: str) -> list:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async with httpx.AsyncClient(follow_redirects=True, timeout=HTTP_TIMEOUT*2) as client:
        async def bounded_sync(sym):
            async with semaphore:
                return await sync_one(pool, client, sym, token)
        
        tasks = [bounded_sync(s) for s in symbols]
        return await asyncio.gather(*tasks)

async def get_stale_symbols(pool: asyncpg.Pool, limit: int = None) -> list:
    """Return symbols that haven't been synced in MIN_AGE_HOURS."""
    query = f"""
        SELECT s.symbol 
        FROM stocks s
        LEFT JOIN stock_keystats_raw kr ON s.symbol = kr.symbol
        WHERE s.is_active = TRUE
          AND (kr.fetched_at IS NULL 
               OR kr.fetched_at < CURRENT_TIMESTAMP - INTERVAL '{MIN_AGE_HOURS} hours')
        ORDER BY COALESCE(kr.fetched_at, '1970-01-01'::timestamp) ASC
        {f"LIMIT {limit}" if limit else ""}
    """
    rows = await pool.fetch(query)
    return [r['symbol'] for r in rows]

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', type=str, help='Sync single symbol')
    parser.add_argument('--all', action='store_true', help='Full sync all (background use only)')
    parser.add_argument('--batch', type=int, default=BATCH_SIZE, help=f'Stocks per run (default {BATCH_SIZE})')
    parser.add_argument('--stale-only', action='store_true', help='Only sync stale records')
    parser.add_argument('--force', action='store_true', help='Force all regardless of age')
    args = parser.parse_args()
    
    start = time.time()
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=MAX_CONCURRENT+2)
    token = await get_token(pool)
    
    if args.symbol:
        symbols = [args.symbol.upper()]
    elif args.force:
        rows = await pool.fetch("SELECT symbol FROM stocks WHERE is_active = TRUE ORDER BY symbol")
        symbols = [r['symbol'] for r in rows][:args.batch]
    else:
        symbols = await get_stale_symbols(pool, args.batch)
    
    if not symbols:
        print("No stale symbols to sync. All up to date!")
        await pool.close()
        return
    
    print(f"Syncing {len(symbols)} stocks (batch limit={args.batch}, concurrent={MAX_CONCURRENT})...")
    results = await run_batch(pool, symbols, token)
    
    successes = [r for r in results if r.get('status') == 200]
    timeouts = [r for r in results if r.get('status') == 'timeout']
    errors = [r for r in results if r.get('status') not in (200, 'timeout')]
    
    avg_time = round(sum(r.get('elapsed', 0) for r in results) / len(results), 2) if results else 0
    
    print(f"\n{'='*50}")
    print(f"✅ Success: {len(successes)} | ⏱ Timeout: {len(timeouts)} | ❌ Error: {len(errors)}")
    print(f"⏳ Total time: {round(time.time()-start, 1)}s | Avg per stock: {avg_time}s")
    
    for f in errors[:5]:
        print(f"  Failed: {f['symbol']} → {f['status']}: {f.get('error', '')[:80]}")
    if len(errors) > 5:
        print(f"  ... and {len(errors)-5} more failures")

    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())
