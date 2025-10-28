#!/bin/bash

# Robust wrapper script for daily_update.py
# This script ensures proper environment setup and comprehensive logging

set -e

# Set up environment
export PYTHONPATH=/app
export PYTHONUNBUFFERED=1
cd /app

# Create timestamp for logging
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="/app/logs/cron.log"

# Ensure log directory exists
mkdir -p /app/logs

# Function to log with timestamp
log_message() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

# Function to log separator
log_separator() {
    echo "========================================" | tee -a "$LOG_FILE"
}

# Start logging
log_separator
log_message "STARTING DAILY COMMODITY PRICES UPDATE"
log_message "Process ID: $$"
log_message "Working Directory: $(pwd)"
log_message "Python Path: $PYTHONPATH"

# Check if Python script exists
if [ ! -f "/app/daily_update.py" ]; then
    log_message "ERROR: daily_update.py not found!"
    exit 1
fi

# Check Python availability
if ! command -v python &> /dev/null; then
    log_message "ERROR: Python not found!"
    exit 1
fi

# Log environment info
log_message "Python version: $(python --version)"
log_message "Environment variables loaded"

# Run the update script with comprehensive error handling
log_message "Executing daily_update.py..."

if python daily_update.py >> "$LOG_FILE" 2>&1; then
    EXIT_CODE=0
    log_message "SUCCESS: Daily update completed successfully"
else
    EXIT_CODE=$?
    log_message "ERROR: Daily update failed with exit code $EXIT_CODE"
fi

# Log completion
log_message "DAILY UPDATE PROCESS COMPLETED"
log_message "Exit code: $EXIT_CODE"
log_separator
echo "" >> "$LOG_FILE"

exit $EXIT_CODE