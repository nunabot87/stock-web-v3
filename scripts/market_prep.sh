#!/bin/bash
# Market Open Preparation Script
# Run this before 09:00 WIB

set -e

LOGFILE="/var/log/stock-web-v3/market_prep.log"
mkdir -p $(dirname $LOGFILE)

echo "[$(date)] Starting market open preparation..." | tee -a $LOGFILE

# 1. Check services
echo "[$(date)] Checking services..." | tee -a $LOGFILE

if systemctl is-active --quiet stock-web-v3; then
    echo "✓ stock-web-v3: RUNNING" | tee -a $LOGFILE
else
    echo "✗ stock-web-v3: NOT RUNNING - starting..." | tee -a $LOGFILE
    sudo systemctl start stock-web-v3
fi

if systemctl is-active --quiet redis-server; then
    echo "✓ redis-server: RUNNING" | tee -a $LOGFILE
else
    echo "✗ redis-server: NOT RUNNING" | tee -a $LOGFILE
fi

if systemctl is-active --quiet postgresql; then
    echo "✓ postgresql: RUNNING" | tee -a $LOGFILE
else
    echo "✗ postgresql: NOT RUNNING" | tee -a $LOGFILE
fi

# 2. Verify API health
echo "[$(date)] Testing API health..." | tee -a $LOGFILE
HEALTH=$(curl -s http://localhost:8000/api/v3/stocks/health | grep -o '"status":"healthy"' || echo "FAIL")
if [ "$HEALTH" = '"status":"healthy"' ]; then
    echo "✓ API Health: OK" | tee -a $LOGFILE
else
    echo "✗ API Health: FAIL" | tee -a $LOGFILE
fi

# 3. Check active sessions (users logged in)
echo "[$(date)] Checking active sessions..." | tee -a $LOGFILE
SESSIONS=$(redis-cli keys 'session:*' 2>/dev/null | wc -l)
echo "ℹ Active sessions: $SESSIONS" | tee -a $LOGFILE

# 4. Check last data sync
echo "[$(date)] Checking data freshness..." | tee -a $LOGFILE
LATEST_DATA=$(sudo -u postgres psql -d stockdb -t -c \
    "SELECT MAX(timestamp) FROM stock_prices_daily WHERE timestamp < CURRENT_DATE" 2>/dev/null | xargs)
if [ -n "$LATEST_DATA" ]; then
    echo "ℹ Latest daily data: $LATEST_DATA" | tee -a $LOGFILE
else
    echo "⚠ No daily data found - run backfill before market open" | tee -a $LOGFILE
fi

# 5. Check disk space
echo "[$(date)] Checking disk space..." | tee -a $LOGFILE
DF_ROOT=$(df -h / | tail -1 | awk '{print $5}' | tr -d '%')
if [ "$DF_ROOT" -lt 80 ]; then
    echo "✓ Disk usage: ${DF_ROOT}%" | tee -a $LOGFILE
else
    echo "⚠ Disk usage HIGH: ${DF_ROOT}%" | tee -a $LOGFILE
fi

# 6. Summary
echo "[$(date)] Preparation complete. Market opens at 09:00 WIB" | tee -a $LOGFILE
echo "========================================" | tee -a $LOGFILE

# Optional: trigger immediate sync if stockbit token is available
if [ -f /opt/stock-web-v3/.env ]; then
    TOKEN=$(grep STOCKBIT_TOKEN /opt/stock-web-v3/.env | cut -d= -f2 | head -1)
    if [ -n "$TOKEN" ] && [ "$TOKEN" != "***" ]; then
        echo "ℹ Stockbit token configured - ready for live data ingestion" | tee -a $LOGFILE
    else
        echo "⚠ Stockbit token not configured - ingestion will fail" | tee -a $LOGFILE
    fi
fi

echo "[$(date)] Report saved to $LOGFILE"
