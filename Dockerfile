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
COPY start.sh .

# Create data directories, kaggle config, and logs directory
RUN mkdir -p /app/data/commodity-prices \
    && mkdir -p /home/appuser/.kaggle \
    && mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DATA_DIR=/app/data/commodity-prices
ENV KAGGLE_CONFIG_DIR=/home/appuser/.kaggle

# Create a non-root user and set permissions
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# Make start.sh executable
RUN chmod +x /app/start.sh

# Default command uses the start.sh script
CMD ["/app/start.sh"]