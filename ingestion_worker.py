#!/usr/bin/env python3
"""
Standalone ingestion worker for stock-web-v3.
Initializes database, Redis, then starts APScheduler.
Uses asyncio.Event for graceful shutdown (fixes RuntimeError on loop.stop()).
"""

import asyncio
import signal
import sys

# Add src to path
sys.path.insert(0, '/opt/stock-web-v3/src')

async def main():
    from stock_web_v3.database import init_db
    from stock_web_v3.redis_client import init_redis
    from stock_web_v3.scheduler import scheduler_manager
    
    # Initialize database
    try:
        await init_db()
        print("[Worker] Database initialized")
    except Exception as e:
        print(f"[Worker] Database init error: {e}")
        sys.exit(1)
    
    # Initialize Redis
    try:
        await init_redis()
        print("[Worker] Redis initialized")
    except Exception as e:
        print(f"[Worker] Redis init error: {e}")
        sys.exit(1)
    
    # Start scheduler
    scheduler_manager.start()
    print("[Worker] Scheduler started")
    
    # Graceful shutdown via asyncio.Event (safe, no loop.stop())
    shutdown_event = asyncio.Event()
    
    def shutdown():
        print("[Worker] Shutting down...")
        scheduler_manager.shutdown()
        shutdown_event.set()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, shutdown)
    
    # Run forever until shutdown signal
    await shutdown_event.wait()
    print("[Worker] Exited cleanly")

if __name__ == "__main__":
    asyncio.run(main())
