"""
Configuration module for Daily Commodity Prices India updater
Contains all constants, settings, and environment variables
"""

import os
from pathlib import Path

# API Configuration
API_BASE_URL = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"
API_KEY = os.getenv('API_KEY')
LIMIT = 10000  # Fetch more records for daily updates
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 5

# Directory Configuration
DATA_DIR = Path(os.getenv('DATA_DIR', '/app/data/commodity-prices'))
STATE_FILE = DATA_DIR / "state.json"
METADATA_FILE = DATA_DIR / "dataset-metadata.json"
CSV_DIR = DATA_DIR / "csv"
PARQUET_DIR = DATA_DIR / "parquet"

# Kaggle Configuration
KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')
KAGGLE_DATASET = os.getenv('KAGGLE_DATASET')
KAGGLE_DOWNLOAD_TIMEOUT = 600  # 10 minutes for large dataset download
KAGGLE_UPLOAD_TIMEOUT = 300   # 5 minutes for upload

# Data Processing Configuration
KEY_COLUMNS = ['Arrival_Date', 'State', 'District',
               'Market', 'Commodity', 'Variety', 'Grade']
STRING_COLUMNS = ['State', 'District',
                  'Market', 'Commodity', 'Variety', 'Grade']
PRICE_COLUMNS = ['Min_Price', 'Max_Price', 'Modal_Price']

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


def validate_config(seeding_mode: bool = False):
    """Validate required configuration

    Args:
        seeding_mode: If True, skip API_KEY validation for initial dataset download
    """
    if not seeding_mode and not API_KEY:
        raise ValueError("API_KEY environment variable is required")

    return True
