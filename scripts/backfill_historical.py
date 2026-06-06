#!/usr/bin/env python3
"""
Backfill historical data for stock-web-v3
Usage: python backfill_historical.py --days 90 --tier 1,2
"""

import asyncio
import argparse
import sys
import os
from datetime import datetime, timedelta
from typing import List

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from stock_web_v3.config import TIER1_SET, TIER2_SET, get_settings
from stock_web_v3.ingestion.stockbit_client import DataIngestionWorker, StockbitClient
import pytz

WIB = pytz.timezone('Asia/Jakarta')


async def backfill_symbol(symbol: str, token: str, days: int = 90):
    """Backfill daily data for a single symbol."""
    from stock_web_v3.ingestion.stockbit_client import StockbitClient
    from stock_web_v3.redis_client import init_redis, close_redis
    
    worker = DataIngestionWorker()
    
    async with StockbitClient(token) as client:
        print(f"[Backfill] {symbol} - fetching {days} days...")
        
        # Get date range
        to_date = datetime.now(WIB).strftime("%Y-%m-%d")
        from_date = (datetime.now(WIB) - timedelta(days=days)).strftime("%Y-%m-%d")
        
        try:
            data = await client.get_daily(symbol, from_date, to_date)
            if not data:
                print(f"[Backfill] {symbol} - no data received")
                return 0
            
            count = await worker.store_daily(symbol, data)
            print(f"[Backfill] {symbol} - inserted {count} records")
            return count
            
        except Exception as e:
            print(f"[Backfill] {symbol} - ERROR: {e}")
            return -1


async def backfill_batch(symbols: List[str], token: str, days: int = 90, concurrency: int = 5):
    """Backfill multiple symbols with controlled concurrency."""
    from stock_web_v3.database import init_db, close_db
    
    # Initialize DB
    await init_db()
    
    try:
        semaphore = asyncio.Semaphore(concurrency)
        
        async def backfill_with_semaphore(symbol):
            async with semaphore:
                # Add delay between requests to avoid rate limiting
                await asyncio.sleep(0.5)
                return await backfill_symbol(symbol, token, days)
        
        tasks = [backfill_with_semaphore(s) for s in symbols]
        results = await asyncio.gather(*tasks)
        
        total = sum(r for r in results if r > 0)
        errors = sum(1 for r in results if r < 0)
        
        print(f"\n[Backfill] Complete: {total} total records, {errors} errors")
        
    finally:
        await close_db()


async def backfill_intraday(symbol: str, token: str, lookback_hours: int = 24):
    """Backfill intraday (1-min) data for recent period."""
    worker = DataIngestionWorker()
    
    async with StockbitClient(token) as client:
        print(f"[Intraday] {symbol} - fetching last {lookback_hours}h...")
        
        to_ts = int(datetime.now(WIB).timestamp())
        from_ts = to_ts - (lookback_hours * 3600)
        
        try:
            data = await client.get_intraday(symbol, from_ts, to_ts)
            count = await worker.store_intraday(symbol, data, token)
            print(f"[Intraday] {symbol} - inserted {count} records")
            return count
            
        except Exception as e:
            print(f"[Intraday] {symbol} - ERROR: {e}")
            return -1


def main():
    parser = argparse.ArgumentParser(description='Backfill historical stock data')
    parser.add_argument('--token', required=True, help='Stockbit JWT token')
    parser.add_argument('--days', type=int, default=90, help='Days of historical data to fetch')
    parser.add_argument('--tier', default='1,2', help='Tier levels to backfill (comma-separated)')
    parser.add_argument('--symbols', help='Specific symbols (comma-separated, overrides tier)')
    parser.add_argument('--intraday', action='store_true', help='Also fetch intraday data')
    parser.add_argument('--concurrency', type=int, default=5, help='Parallel requests')
    
    args = parser.parse_args()
    
    # Build symbol list
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
    else:
        tiers = [int(t) for t in args.tier.split(',')]
        symbols = []
        if 1 in tiers:
            symbols.extend(TIER1_SET)
        if 2 in tiers:
            symbols.extend(TIER2_SET)
    
    symbols = list(set(symbols))  # Remove duplicates
    symbols.sort()
    
    print(f"[Backfill] Starting backfill for {len(symbols)} symbols: {symbols[:5]}{'...' if len(symbols) > 5 else ''}")
    print(f"[Backfill] Days: {args.days}, Token: {args.token[:20]}...")
    
    # Run backfill
    asyncio.run(backfill_batch(symbols, args.token, args.days, args.concurrency))
    
    # Optional: intraday backfill
    if args.intraday:
        print("\n[Intraday] Backfilling intraday data...")
        # Just do Tier 1 for intraday to save time/API calls
        intraday_symbols = list(TIER1_SET)[:10]
        for symbol in intraday_symbols:
            asyncio.run(backfill_intraday(symbol, args.token, lookback_hours=24))
            asyncio.run(asyncio.sleep(1))  # Rate limiting


if __name__ == '__main__':
    main()
