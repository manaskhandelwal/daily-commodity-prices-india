# Daily Commodity Prices India - Docker Container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including cron
RUN apt-get update && apt-get install -y \
    curl \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY daily_update.py .
COPY src/ ./src/
COPY crontab.txt .

# Create data directories, kaggle config, and logs directory
RUN mkdir -p /app/data/commodity-prices \
    && mkdir -p /home/appuser/.kaggle \
    && mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DATA_DIR=/app/data/commodity-prices
ENV KAGGLE_CONFIG_DIR=/home/appuser/.kaggle

# Set up cron job (must be done as root)
RUN echo "59 23 * * * cd /app && python daily_update.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/daily-commodity-update \
    && chmod 0644 /etc/cron.d/daily-commodity-update \
    && crontab /etc/cron.d/daily-commodity-update

# Create a non-root user and set permissions
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# Create startup script
RUN echo '#!/bin/bash\n\
# Start cron daemon\n\
service cron start\n\
\n\
# Always run initial update on first startup to handle seeding\n\
echo "Running initial update (includes seeding if needed)..."\n\
cd /app && python daily_update.py\n\
\n\
# Keep container running and show logs\n\
echo "Cron daemon started. Daily updates scheduled for 11:59 PM."\n\
echo "Monitoring logs..."\n\
tail -f /app/logs/cron.log /var/log/cron.log 2>/dev/null || sleep infinity\n\
' > /app/start.sh && chmod +x /app/start.sh

# Default command starts cron and keeps container running
CMD ["/app/start.sh"]