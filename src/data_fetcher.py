"""
Data fetcher module for API operations
Handles fetching data from the government API with retry logic
"""

import time
import hashlib
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from io import StringIO

from .config import (
    API_BASE_URL, API_KEY, LIMIT, REQUEST_TIMEOUT,
    MAX_RETRIES, RETRY_DELAY, STRING_COLUMNS, PRICE_COLUMNS
)

logger = logging.getLogger(__name__)


class DataFetcher:
    """Handles fetching and processing data from the government API"""

    def __init__(self):
        if not API_KEY:
            raise ValueError("API_KEY is required for data fetching")
        self.api_key = API_KEY

    def fetch_latest_data(self, days_back: int = 2) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data from the government API

        Args:
            days_back: Number of days to look back for data (default: 2)

        Returns:
            DataFrame with fetched data or None if failed
        """
        # Calculate date range for recent data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        params = {
            'api-key': self.api_key,
            'format': 'csv',
            'offset': 0,
            'limit': LIMIT,
            'range[Arrival_Date][gte]': start_date.strftime('%Y-%m-%d'),
            'range[Arrival_Date][lte]': end_date.strftime('%Y-%m-%d'),
        }

        for attempt in range(MAX_RETRIES):
            try:
                logger.info(
                    f"Fetching data from API (attempt {attempt + 1}/{MAX_RETRIES})")
                logger.info(
                    f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

                response = requests.get(
                    API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()

                if not response.text or response.text.strip() == "":
                    logger.warning("Empty response from API")
                    return None

                # Parse CSV data
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"Successfully fetched {len(df)} records from API")
                return df

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"API request failed (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error("All API request attempts failed")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error fetching data: {e}")
                return None

        return None

    def clean_and_process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and process the raw data

        Args:
            df: Raw DataFrame from API

        Returns:
            Cleaned and processed DataFrame
        """
        logger.info("Cleaning and processing data...")

        # Clean string fields with enhanced cleaning from testing patterns
        for col in STRING_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # Replace multiple spaces with single space
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                # Add space before opening bracket if missing (from testing patterns)
                df[col] = df[col].str.replace(r'([a-zA-Z0-9])(\()', r'\1 \2', regex=True)

        # Clean price fields with enhanced formatting
        for col in PRICE_COLUMNS:
            if col in df.columns:
                # Convert to numeric first
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Remove unnecessary decimal points (.0) for whole numbers
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x == int(x) else x)

        # Format dates (Indian format: dd/mm/yyyy)
        df['Arrival_Date'] = pd.to_datetime(
            df['Arrival_Date'], dayfirst=True).dt.strftime('%Y-%m-%d')

        # Remove duplicates based on key columns
        from .config import KEY_COLUMNS
        available_key_columns = [
            col for col in KEY_COLUMNS if col in df.columns]

        initial_count = len(df)
        df = df.drop_duplicates(subset=available_key_columns, keep='first')
        final_count = len(df)

        if initial_count > final_count:
            logger.info(
                f"Removed {initial_count - final_count} duplicate records")

        logger.info(f"Processed data: {final_count} clean records")
        return df

    def calculate_data_hash(self, df: pd.DataFrame) -> str:
        """
        Calculate a hash of the dataframe to detect changes

        Args:
            df: DataFrame to hash

        Returns:
            SHA-256 hash string
        """
        # Sort by key columns and create a hash
        df_sorted = df.sort_values(
            ['Arrival_Date', 'State', 'District', 'Market', 'Commodity'])
        data_string = df_sorted.to_csv(index=False)
        return hashlib.sha256(data_string.encode()).hexdigest()

    def is_new_data(self, df: pd.DataFrame, last_hash: str = None, processed_dates: list = None) -> bool:
        """
        Check if the fetched data contains new records

        Args:
            df: DataFrame to check
            last_hash: Previous data hash
            processed_dates: List of already processed dates

        Returns:
            True if new data is found, False otherwise
        """
        current_hash = self.calculate_data_hash(df)

        if last_hash == current_hash:
            logger.info("No new data detected (hash match)")
            return False

        # Check for new dates
        new_dates = df['Arrival_Date'].unique()
        processed_dates_set = set(processed_dates or [])

        unprocessed_dates = [
            date for date in new_dates if date not in processed_dates_set]

        if unprocessed_dates:
            logger.info(f"Found new data for dates: {unprocessed_dates}")
            return True
        else:
            logger.info("No new dates found in fetched data")
            return False
