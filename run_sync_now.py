#!/opt/stock-web-v3/venv/bin/python3
import asyncio
import aiohttp
import sys
sys.path.insert(0, '/opt/stock-web-v3/src')

from datetime import datetime
import pytz

WIB = pytz.timezone('Asia/Jakarta')

async def run_sync():
    from stock_web_v3.database import init_db, execute
    from stock_web_v3.redis_client import init_redis, get_redis
    
    await init_db()
    await init_redis()
    
    redis = get_redis()
    token = await redis.get('stockbit_token:system:primary')
    if not token:
        print("[ERROR] No token in Redis")
        return
    token = token.decode() if isinstance(token, bytes) else token
    print(f"[OK] Token loaded ({len(token)} chars)")
    
    symbols = ['ASII','BBCA','BBNI','BBRI','BMRI','ICBP','KLBF','PGAS','TLKM','UNVR',
               'ADRO','ITMG','PTBA','TINS','BRIS','BBTN','BBKP','SMRA','PWON','CTRA']
    
    now = datetime.now(WIB)
    to_ts = int(now.timestamp())
    from_ts = to_ts - (24 * 3600)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Origin": "https://stockbit.com"
    }
    
    total_inserted = 0
    errors = 0
    
    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                url = f"https://exodus.stockbit.com/chartbit/{symbol}/price/intraday?from={to_ts}&to={from_ts}"
                async with session.get(url, headers=headers) as resp:
                    data = await resp.json()
                    
                    if isinstance(data, dict) and "data" in data:
                        inner = data["data"]
                        candles = inner["chartbit"] if (isinstance(inner, dict) and "chartbit" in inner) else (inner if isinstance(inner, list) else [])
                    else:
                        candles = data if isinstance(data, list) else []
                    
                    inserted = 0
                    for c in candles:
                        try:
                            ts_raw = c.get("unix_timestamp", c.get("t", 0))
                            ts = datetime.fromtimestamp(int(ts_raw), WIB)
                            o = float(c.get("open", c.get("o", 0)))
                            h = float(c.get("high", c.get("h", 0)))
                            l = float(c.get("low", c.get("l", 0)))
                            cl = float(c.get("close", c.get("c", 0)))
                            v = int(c.get("volume", c.get("v", 0)))
                            
                            await execute("""
                                INSERT INTO stock_prices_1m (symbol, timestamp, open, high, low, close, volume)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                                    open = EXCLUDED.open, high = EXCLUDED.high,
                                    low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
                            """, symbol, ts, o, h, l, cl, v)
                            inserted += 1
                        except Exception as e:
                            print(f"  [{symbol}] insert err: {e}")
                    
                    total_inserted += inserted
                    print(f"[{symbol}] {inserted} candles")
                    
            except Exception as e:
                errors += 1
                print(f"[{symbol}] ERROR: {e}")
            
            await asyncio.sleep(0.2)
    
    print(f"\n[COMPLETE] {total_inserted} records, {errors} errors")

if __name__ == "__main__":
    asyncio.run(run_sync())
