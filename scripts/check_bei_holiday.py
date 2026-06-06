#!/opt/stock-web-v3/venv/bin/python3
"""
BEI Trading Holiday Checker — v2
Return: 0 = libur, 1 = hari kerja, 2 = error
Uses Unix socket peer auth (no password).
"""
import asyncio, asyncpg, os, sys
from datetime import datetime

DB_URL = "postgresql://stock_app@/stockdb"

async def main():
    try:
        conn = await asyncpg.connect(DB_URL)
    except Exception as e:
        print(f"DB connection error: {e}", file=sys.stderr)
        sys.exit(2)

    # Cek akhir pekan
    dow = datetime.now().weekday()
    if dow >= 5:
        print(f"Weekend (dow={dow}) — BEI libur")
        await conn.close()
        sys.exit(0)

    # Cek trading_holidays table
    try:
        row = await conn.fetchrow(
            "SELECT 1 FROM trading_holidays WHERE holiday_date = CURRENT_DATE"
        )
        if row:
            print("Holiday in trading_holidays — BEI libur")
            await conn.close()
            sys.exit(0)
    except Exception as e:
        print(f"Holiday query error: {e}", file=sys.stderr)
        await conn.close()
        sys.exit(2)

    await conn.close()
    print("Trading day — BEI buka")
    sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
