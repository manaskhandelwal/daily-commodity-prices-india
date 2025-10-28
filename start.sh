#!/bin/bash

# Robust start script for Daily Commodity Prices Updater
# This script properly handles cron daemon in Docker container

set -e

# Function to log with timestamp
log_with_timestamp() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to handle shutdown gracefully
cleanup() {
    log_with_timestamp "Received shutdown signal, stopping cron daemon..."
    service cron stop
    exit 0
}

# Set up signal handlers for graceful shutdown
trap cleanup SIGTERM SIGINT

log_with_timestamp "=== Daily Commodity Prices Updater - Container Starting ==="

# Ensure required directories exist
mkdir -p /app/logs
mkdir -p /app/data/commodity-prices

# Make scripts executable
chmod +x /app/run_update.sh

# Install cron jobs from crontab.txt
log_with_timestamp "Installing cron jobs..."
if [ -f "/app/crontab.txt" ]; then
    # Install crontab for the current user (not root)
    crontab /app/crontab.txt
    log_with_timestamp "Cron jobs installed from crontab.txt"
else
    log_with_timestamp "ERROR: crontab.txt not found!"
    exit 1
fi

# Start cron daemon in foreground mode for Docker
log_with_timestamp "Starting cron daemon..."
service cron start

# Verify cron is running
sleep 3
if pgrep cron > /dev/null 2>&1; then
    log_with_timestamp "✅ Cron daemon started successfully"
else
    log_with_timestamp "❌ Failed to start cron daemon"
    exit 1
fi

# Display current cron configuration
log_with_timestamp "Current cron configuration:"
crontab -l | while read line; do
    log_with_timestamp "  $line"
done

# Run initial update if requested
if [ "${RUN_INITIAL_UPDATE:-false}" = "true" ]; then
    log_with_timestamp "Running initial update as requested..."
    /app/run_update.sh
fi

log_with_timestamp "Container initialization completed successfully"
log_with_timestamp "Cron will run every 3 hours: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00"
log_with_timestamp "Logs will be written to: /app/logs/cron.log"

# Keep container running and monitor cron daemon
log_with_timestamp "Container ready - monitoring cron daemon..."
while true; do
    if ! pgrep cron > /dev/null 2>&1; then
        log_with_timestamp "❌ Cron daemon stopped unexpectedly, restarting..."
        service cron start
        sleep 5
    fi
    sleep 60  # Check every minute
done