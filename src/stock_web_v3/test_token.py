"""Test script untuk validate Stockbit token dan fetch data."""
import asyncio
import aiohttp
import asyncpg
import redis.asyncio as redis
from datetime import datetime, timedelta
import pytz
import base64
import json

WIB = pytz.timezone('Asia/Jakarta')

# Token dari user
STOCKBIT_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImExNWQ5OGE2LTdkYzgtNDM3NS05NDk0LTEyOWJlM2RlODVkNCIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InVzZSI6ImFuZGlhZGl0eWF3YXJtYW4iLCJlbWEiOiJhbmRpYWRpdHlhd2FybWFuQGdtYWlsLmNvbSIsImZ1bCI6IkNhcmkgQ3VhbiIsInNlcyI6IndMWDQ1WTlNV0J6ajFnZmoiLCJkdmMiOiJiN2MwOGFmNTgxYmIxZjFiZjI3MDg1NTkzYmI4NThmMCIsInVpZCI6Mzg4MzU1NiwiY291IjoiSUQifSwiZXhwIjoxNzc5NzU2ODE1LCJpYXQiOjE3Nzk2NzA0MTUsImlzcyI6IlNUT0NLQklUIiwianRpIjoiNDFlZWYzYjctNjI2Zi00YmE5LTk5MzEtMGU4YWRmNDk4NWQ2IiwibmJmIjoxNzc5NjcwNDE1LCJ2ZXIiOiJ2MSJ9.kFbTT7IZ1wAtSNP4xrjWpVEoqNJVuJ5hD9PRflqDPrwFrvt52TD_cpCuL6Gjye3ZyKYS7N3wykfch9MVQAhZdWSXuqDyj4WRzaLe22He2qiC0zB9Qs2bhKg5MnsjltQaSaEla2PUDhqCPGwy9Q445A_FoHqLy1g063qMYJCX1gttHCrKNvVYBiMbEP4HUYqPy4QC1-CygCYzuUhRSEp-dbdRW5WkhZlPbx5J50uIoFWxduTLmKJMK_8yshy3A9StA_1LAChgNBjoN5caTa0QDldxfko7fPXa3GObjgXn0EdAg8z_gaEMIKdGsFJQywpwt3628ZKTvkdk6tcS50O0jA"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Authorization": f"Bearer {STOCKBIT_TOKEN}",
    "Origin": "https://stockbit.com",
    "Referer": "https://stockbit.com/",
}

CHARTBIT_URL = "https://chartbit.stockbit.com"


def decode_token_payload():
    """Decode JWT payload untuk info user."""
    try:
        parts = STOCKBIT_TOKEN.split(".")
        payload = parts[1]
        # Add padding
        payload += "=" * (4 - len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        return data
    except Exception as e:
        return {"error": str(e)}


async def test_token_validation():
    """Test token dengan fetch IHSG daily."""
    async with aiohttp.ClientSession() as session:
        to_date = datetime.now(WIB).strftime("%Y-%m-%d")
        from_date = (datetime.now(WIB) - timedelta(days=7)).strftime("%Y-%m-%d")
        
        to_ts = int(datetime.strptime(to_date, "%Y-%m-%d").timestamp())
        from_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
        
        url = f"{CHARTBIT_URL}/IHSG/price/daily"
        params = {"from": to_ts, "to": from_ts}  # Reverse chronological
        
        print(f"[Test] Fetching IHSG daily data...")
        print(f"[Test] URL: {url}")
        print(f"[Test] Params: {params}")
        
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                print(f"[Test] Status: {resp.status}")
                
                if resp.status == 401:
                    print("[Test] ❌ Token invalid atau expired")
                    return False
                elif resp.status == 429:
                    print("[Test] ⚠️ Rate limited")
                    return False
                elif resp.status >= 400:
                    text = await resp.text()
                    print(f"[Test] ❌ Error: {text[:200]}")
                    return False
                
                data = await resp.json()
                candles = data.get("data", []) if isinstance(data, dict) else data
                
                if candles:
                    print(f"[Test] ✅ Token valid! Got {len(candles)} candles")
                    print(f"[Test] Sample: {candles[0] if candles else 'N/A'}")
                    return True
                else:
                    print("[Test] ⚠️ No data returned")
                    return False
                    
        except Exception as e:
            print(f"[Test] ❌ Exception: {e}")
            return False


async def test_intraday_bcba():
    """Test intraday fetch untuk BBCA."""
    async with aiohttp.ClientSession() as session:
        to_ts = int(datetime.now(WIB).timestamp())
        from_ts = to_ts - (24 * 3600)  # Last 24 hours
        
        url = f"{CHARTBIT_URL}/BBCA/price/intraday"
        params = {"from": from_ts, "to": to_ts}
        
        print(f"\n[Test] Fetching BBCA intraday...")
        
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"[Test] ❌ Failed: {resp.status} - {text[:100]}")
                    return False
                
                data = await resp.json()
                candles = data.get("data", []) if isinstance(data, dict) else data
                
                print(f"[Test] ✅ BBCA intraday: {len(candles)} candles")
                
                if candles:
                    latest = candles[-1]
                    print(f"[Test] Latest: O={latest.get('o')} H={latest.get('h')} L={latest.get('l')} C={latest.get('c')} V={latest.get('v')}")
                
                return True
                
        except Exception as e:
            print(f"[Test] ❌ Exception: {e}")
            return False


async def test_redis_store():
    """Test Redis connection dan simpan token."""
    try:
        r = redis.from_url("redis://localhost:6379/0", decode_responses=True)
        
        test_key = "test:stockbit_token"
        await r.setex(test_key, 3600, STOCKBIT_TOKEN)
        
        stored = await r.get(test_key)
        if stored == STOCKBIT_TOKEN:
            print("[Test] ✅ Redis test passed - token stored and retrieved")
            await r.delete(test_key)
            return True
        else:
            print("[Test] ❌ Redis data mismatch")
            return False
            
    except Exception as e:
        print(f"[Test] ❌ Redis error: {e}")
        return False


async def main():
    """Run all tests."""
    print("="*60)
    print("STOCKBIT TOKEN VALIDATION TEST")
    print("="*60)
    
    # Decode token info
    payload = decode_token_payload()
    print(f"\n[Info] Token payload:")
    print(f"  User: {payload.get('data', {}).get('use', 'N/A')}")
    print(f"  Email: {payload.get('data', {}).get('ema', 'N/A')}")
    print(f"  Full Name: {payload.get('data', {}).get('ful', 'N/A')}")
    
    exp_ts = payload.get('exp')
    if exp_ts:
        exp_dt = datetime.fromtimestamp(exp_ts, WIB)
        print(f"  Expires: {exp_dt.isoformat()}")
        print(f"  Valid for: {(exp_dt - datetime.now(WIB)).days} days")
    
    print("\n" + "-"*60)
    
    # Test 1: Token validation via API
    print("\n[1/3] Testing token validation via Stockbit API...")
    token_valid = await test_token_validation()
    
    # Test 2: Intraday fetch
    print("\n[2/3] Testing BBCA intraday fetch...")
    intraday_ok = await test_intraday_bcba()
    
    # Test 3: Redis
    print("\n[3/3] Testing Redis connection...")
    redis_ok = await test_redis_store()
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Token valid:  {'✅ YES' if token_valid else '❌ NO'}")
    print(f"Intraday OK:  {'✅ YES' if intraday_ok else '❌ NO'}")
    print(f"Redis OK:     {'✅ YES' if redis_ok else '❌ NO'}")
    print("="*60)
    
    if token_valid and intraday_ok and redis_ok:
        print("\n✅ All tests passed. Token ready for production.")
        print("\nSimpan token ini ke .env file:")
        print(f"STOCKBIT_TOKEN={STOCKBIT_TOKEN[:50]}...")
    else:
        print("\n⚠️ Some tests failed. Check output above.")


if __name__ == "__main__":
    asyncio.run(main())