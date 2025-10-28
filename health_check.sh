#!/bin/bash

# Health check script for Daily Commodity Prices Updater
# This script checks the health of cron jobs and logs

set -e

# Function to log with timestamp
log_with_timestamp() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if cron is running
check_cron_status() {
    if pgrep cron > /dev/null 2>&1; then
        log_with_timestamp "✅ Cron daemon is running"
        return 0
    else
        log_with_timestamp "❌ Cron daemon is not running"
        return 1
    fi
}

# Function to check log files
check_log_files() {
    local log_dir="/app/logs"
    local cron_log="$log_dir/cron.log"
    
    if [ ! -d "$log_dir" ]; then
        log_with_timestamp "❌ Log directory $log_dir does not exist"
        return 1
    fi
    
    log_with_timestamp "📁 Log directory exists: $log_dir"
    
    # Check if cron log exists and has recent entries
    if [ -f "$cron_log" ]; then
        local log_size=$(stat -c%s "$cron_log" 2>/dev/null || echo "0")
        log_with_timestamp "📄 Cron log exists: $cron_log (size: ${log_size} bytes)"
        
        # Check for recent log entries (within last 4 hours)
        if find "$cron_log" -mmin -240 -type f | grep -q .; then
            log_with_timestamp "✅ Cron log has recent activity"
        else
            log_with_timestamp "⚠️  Cron log has no recent activity (last 4 hours)"
        fi
    else
        log_with_timestamp "⚠️  Cron log does not exist yet: $cron_log"
    fi
}

# Function to check cron jobs configuration
check_cron_config() {
    log_with_timestamp "🔧 Checking cron configuration..."
    
    if crontab -l > /dev/null 2>&1; then
        local job_count=$(crontab -l | grep -v '^#' | grep -v '^$' | wc -l)
        log_with_timestamp "✅ Cron jobs configured: $job_count active jobs"
        
        log_with_timestamp "📋 Active cron jobs:"
        crontab -l | grep -v '^#' | grep -v '^$' | while read line; do
            log_with_timestamp "   $line"
        done
    else
        log_with_timestamp "❌ No cron jobs configured"
        return 1
    fi
}

# Function to check disk space
check_disk_space() {
    local usage=$(df /app | tail -1 | awk '{print $5}' | sed 's/%//')
    log_with_timestamp "💾 Disk usage: ${usage}%"
    
    if [ "$usage" -gt 90 ]; then
        log_with_timestamp "❌ Disk usage is critically high (>90%)"
        return 1
    elif [ "$usage" -gt 80 ]; then
        log_with_timestamp "⚠️  Disk usage is high (>80%)"
    else
        log_with_timestamp "✅ Disk usage is normal"
    fi
}

# Function to check if daily_update.py is accessible
check_update_script() {
    if [ -f "/app/daily_update.py" ]; then
        log_with_timestamp "✅ daily_update.py exists"
        if [ -r "/app/daily_update.py" ]; then
            log_with_timestamp "✅ daily_update.py is readable"
        else
            log_with_timestamp "❌ daily_update.py is not readable"
            return 1
        fi
    else
        log_with_timestamp "❌ daily_update.py does not exist"
        return 1
    fi
}

# Main health check function
main() {
    log_with_timestamp "=== Daily Commodity Prices Updater - Health Check ==="
    
    local exit_code=0
    
    # Run all checks
    check_cron_status || exit_code=1
    check_cron_config || exit_code=1
    check_update_script || exit_code=1
    check_log_files || exit_code=1
    check_disk_space || exit_code=1
    
    if [ $exit_code -eq 0 ]; then
        log_with_timestamp "✅ All health checks passed"
    else
        log_with_timestamp "❌ Some health checks failed"
    fi
    
    log_with_timestamp "=== Health Check Complete ==="
    return $exit_code
}

# Run the health check
main "$@"