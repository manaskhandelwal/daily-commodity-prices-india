#!/bin/bash

# Start script for Daily Commodity Prices Updater
# This script handles cron daemon startup and optional initial update

set -e

echo "=== Daily Commodity Prices Updater - Container Starting ==="
echo "Timestamp: $(date)"

# Ensure log directory exists
mkdir -p /app/logs

# Install cron jobs
echo "Installing cron jobs..."
echo "0 19 * * * root cd /app && PYTHONPATH=/app python daily_update.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/daily-commodity-update
echo "59 23 * * * root cd /app && PYTHONPATH=/app python daily_update.py >> /app/logs/cron.log 2>&1" >> /etc/cron.d/daily-commodity-update
echo "" >> /etc/cron.d/daily-commodity-update
chmod 0644 /etc/cron.d/daily-commodity-update
crontab /etc/cron.d/daily-commodity-update

# Start cron daemon
echo "Starting cron daemon..."
service cron start

# Wait a moment for cron to start
sleep 2

# Check if cron is running using multiple methods
echo "Verifying cron daemon status..."
if pgrep cron > /dev/null 2>&1; then
    echo "Cron daemon started successfully (verified with pgrep)"
elif service cron status > /dev/null 2>&1; then
    echo "Cron daemon started successfully (verified with service status)"
elif ps aux | grep -v grep | grep cron > /dev/null 2>&1; then
    echo "Cron daemon started successfully (verified with ps)"
else
    echo "WARNING: Could not verify cron daemon status, but continuing..."
    echo "Checking running processes:"
    ps aux | grep cron || echo "No cron processes found"
fi

# Display cron configuration
echo "Current cron configuration:"
crontab -l

# Run initial update if requested
if [ "${RUN_INITIAL_UPDATE:-false}" = "true" ]; then
    echo "Running initial update as requested..."
    cd /app
    python daily_update.py
    echo "Initial update completed"
else
    echo "Skipping initial update (RUN_INITIAL_UPDATE not set to true)"
fi

echo "=== Container initialization complete ==="
echo "Cron jobs will run daily at 7:00 PM and 11:59 PM"
echo "Monitoring logs..."

# Keep container running and monitor logs
tail -f /app/logs/daily_update.log /var/log/cron.log 2>/dev/null || tail -f /dev/null