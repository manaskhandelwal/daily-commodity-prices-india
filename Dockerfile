# Use Python 3.11 slim image
FROM python:3.11-slim

# Install system dependencies including cron
RUN apt-get update && apt-get install -y \
    cron \
    procps \
    logrotate \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/commodity-prices /app/logs \
    && chmod 755 /app/data /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC

# Make scripts executable
RUN chmod +x /app/start.sh /app/run_update.sh /app/health_check.sh

# Set up log rotation
COPY logrotate.conf /etc/logrotate.d/commodity-updater
RUN chmod 644 /etc/logrotate.d/commodity-updater

# Create a non-root user for better security (optional but recommended)
# RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
# USER appuser

# Health check to ensure cron is running
HEALTHCHECK --interval=5m --timeout=30s --start-period=1m --retries=3 \
    CMD pgrep cron > /dev/null || exit 1

# Default command
CMD ["/app/start.sh"]