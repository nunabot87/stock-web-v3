#!/bin/bash
# keystats-sync-wrapper.sh — Production v3
# Run as stock_app user via Unix socket DB auth
set -euo pipefail

SCRIPT_DIR="/opt/stock-web-v3/scripts"
LOG_FILE="/tmp/keystats-wrapper-$(date +%Y%m%d).log"
PYTHON="/opt/stock-web-v3/venv/bin/python3"

# Log start
echo "[$(date +%Y-%m-%d_%H:%M:%S)] Starting keystats-sync-wrapper (v3)" >> "$LOG_FILE"

# Cek BEI holiday (run as stock_app for peer auth)
if sudo -u stock_app "$PYTHON" "$SCRIPT_DIR/check_bei_holiday.py" >> "$LOG_FILE" 2>&1; then
    echo "[$(date +%Y-%m-%d_%H:%M:%S)] BEI libur — skip sync_keystats" >> "$LOG_FILE"
    exit 0
fi

# Jalanin sync asli sebagai stock_app
echo "[$(date +%Y-%m-%d_%H:%M:%S)] BEI buka — jalankan sync_keystats" >> "$LOG_FILE"
sudo -u stock_app "$PYTHON" "$SCRIPT_DIR/sync_keystats_cron.py" "$@" >> "$LOG_FILE" 2>&1
