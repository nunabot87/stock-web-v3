#!/usr/bin/env python3
"""
Generate dummy historical data for testing (no API needed)
Usage: python seed_dummy_data.py --days 30
"""

import asyncio
import sys
import os
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from stock_web_v3.config import TIER1_SET, TIER2_SET
from stock_web_v3.database import init_db, close_db, execute
import pytz

WIB = pytz.timezone('Asia/Jakarta')

# Base prices untuk simulasi
BASE_PRICES = {
    'BBCA': 8050, 'BBRI': 4150, 'BMRI': 5400, 'TLKM': 3800, 'UNVR': 4200,
    'ASII': 5200, 'ICBP': 11200, 'KLBF': 1350, 'PGAS': 1250, 'BBNI': 4850,
    'INDF': 6200, 'ANTM': 1850, 'BRIS': 2200, 'BBTN': 950, 'ADRO': 2850,
    'ITMG': 15200, 'PTBA': 2650, 'TINS': 1450, 'SMRA': 1120, 'PWON': 580,
    'CTRA': 620, 'BSDE': 550, 'MAPI': 2850, 'MNCN': 780, 'EXCL': 2350
}


def generate_random_walk(base_price: float, days: int) -> list:
    """Generate random walk price data."""
    prices = []
    current = base_price
    
    for i in range(days):
        # Random volatility 1-3%
        volatility = random.uniform(0.01, 0.03)
        change = random.choice([-1, 1]) * current * volatility * random.random()
        
        open_p = current
        close_p = current + change
        high_p = max(open_p, close_p) * (1 + random.uniform(0, 0.015))
        low_p = min(open_p, close_p) * (1 - random.uniform(0, 0.015))
        volume = random.randint(500000, 50000000)
        
        prices.append({
            'open': round(open_p, 2),
            'high': round(high_p, 2),
            'low': round(low_p, 2),
            'close': round(close_p, 2),
            'volume': volume
        })
        
        current = close_p
    
    return prices


async def seed_symbol(symbol: str, days: int = 30):
    """Insert dummy data for a symbol."""
    base = BASE_PRICES.get(symbol, 1000)
    prices = generate_random_walk(base, days)
    
    # Insert data backwards (oldest first)
    inserted = 0
    today = datetime.now(WIB).date()
    
    for i, p in enumerate(reversed(prices)):
        date = today - timedelta(days=i+1)
        
        try:
            await execute("""
                INSERT INTO stock_prices_daily (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, symbol, date, p['open'], p['high'], p['low'], p['close'], p['volume'])
            inserted += 1
        except Exception as e:
            print(f"  Error inserting {symbol} {date}: {e}")
    
    print(f"  {symbol}: {inserted} rows")
    return inserted


async def seed_all(symbols: list, days: int = 30):
    """Seed data for all symbols."""
    await init_db()
    
    try:
        total = 0
        print(f"Seeding {len(symbols)} symbols with {days} days of data...")
        print("=" * 50)
        
        for symbol in symbols:
            count = await seed_symbol(symbol, days)
            total += count
            await asyncio.sleep(0.05)  # Brief pause
        
        print("=" * 50)
        print(f"Total inserted: {total} rows")
        
        # Update change/percentage
        print("\nUpdating change calculations...")
        for symbol in symbols:
            await execute("""
                WITH ranked AS (
                    SELECT 
                        symbol, timestamp, close,
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
        
        print("Done!")
        
    finally:
        await close_db()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30, help='Days of data to generate')
    parser.add_argument('--tier', default='1,2', help='Which tiers to seed')
    args = parser.parse_args()
    
    tiers = [int(t) for t in args.tier.split(',')]
    symbols = []
    if 1 in tiers:
        symbols.extend(TIER1_SET)
    if 2 in tiers:
        symbols.extend(TIER2_SET)
    
    symbols = list(set(symbols))
    
    asyncio.run(seed_all(symbols, args.days))


if __name__ == '__main__':
    main()
