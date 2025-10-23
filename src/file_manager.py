"""
File manager module for CSV and Parquet operations
Handles yearly file management, merging, and rollover operations
"""

import logging
import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional

from .config import DATA_DIR, CSV_DIR, PARQUET_DIR

logger = logging.getLogger(__name__)


def clean_string_field(text):
    """Clean string fields by fixing spacing issues and bracket formatting."""
    if pd.isna(text) or not isinstance(text, str):
        return text

    # Remove extra whitespace and strip
    text = text.strip()

    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)

    # Add space before opening bracket if missing
    text = re.sub(r'([a-zA-Z0-9])(\()', r'\1 \2', text)

    return text


def clean_price_field(price):
    """Remove unnecessary decimal points (.0) from price fields."""
    if pd.isna(price):
        return price

    # Convert to float first to handle string inputs
    try:
        price_float = float(price)
        # If it's a whole number, return as integer
        if price_float == int(price_float):
            return int(price_float)
        else:
            return price_float
    except (ValueError, TypeError):
        return price


def format_date_iso(date_input):
    """Parse date string or Timestamp and return in ISO 8601 format (YYYY-MM-DD)."""
    # Handle pandas Timestamp objects
    if pd.isna(date_input):
        return date_input

    # If it's already a pandas Timestamp, format it
    if isinstance(date_input, pd.Timestamp):
        return date_input.strftime("%Y-%m-%d")

    # If it's a string, parse and format it
    if isinstance(date_input, str):
        try:
            # Parse DD/MM/YYYY format
            dt = datetime.strptime(date_input, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            try:
                # Try if already in YYYY-MM-DD format
                dt = datetime.strptime(date_input, "%Y-%m-%d")
                return date_input  # Already in correct format
            except ValueError:
                logger.warning(f"Could not parse date: {date_input}")
                return date_input

    # Handle other datetime-like objects
    try:
        return pd.to_datetime(date_input).strftime("%Y-%m-%d")
    except Exception:
        logger.warning(f"Could not parse date: {date_input}")
        return str(date_input)


def parse_date(date_input):
    """Parse date string or Timestamp for sorting purposes."""
    # Handle pandas Timestamp objects
    if pd.isna(date_input):
        return datetime.min

    # If it's already a pandas Timestamp, convert to datetime
    if isinstance(date_input, pd.Timestamp):
        return date_input.to_pydatetime()

    # If it's a string, parse it
    if isinstance(date_input, str):
        try:
            # Try YYYY-MM-DD format first
            return datetime.strptime(date_input, "%Y-%m-%d")
        except ValueError:
            try:
                # Try DD/MM/YYYY format
                return datetime.strptime(date_input, "%d/%m/%Y")
            except ValueError:
                logger.warning(
                    f"Could not parse date for sorting: {date_input}")
                return datetime.min

    # Handle other datetime-like objects
    try:
        return pd.to_datetime(date_input).to_pydatetime()
    except Exception:
        logger.warning(f"Could not parse date for sorting: {date_input}")
        return datetime.min


class FileManager:
    """Handles file operations for CSV and Parquet data"""

    def __init__(self):
        self.data_dir = DATA_DIR
        self.csv_dir = CSV_DIR
        self.parquet_dir = PARQUET_DIR
        self._ensure_directories()

    def _ensure_directories(self):
        """Ensure all required directories exist"""
        for directory in [self.data_dir, self.csv_dir, self.parquet_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def get_current_year_files(self) -> Tuple[Path, Path]:
        """
        Get the current year's CSV and Parquet file paths

        Returns:
            Tuple of (csv_path, parquet_path)
        """
        current_year = datetime.now().strftime('%Y')
        csv_file = self.csv_dir / f"{current_year}.csv"
        parquet_file = self.parquet_dir / f"{current_year}.parquet"
        return csv_file, parquet_file

    def load_current_year_data(self) -> Optional[pd.DataFrame]:
        """
        Load the current year's data from CSV file

        Returns:
            DataFrame with current year's data or None if file doesn't exist
        """
        csv_file, _ = self.get_current_year_files()

        if csv_file.exists():
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"Loaded {len(df)} records from {csv_file.name}")
                return df
            except Exception as e:
                logger.error(f"Error loading current year data: {e}")
                return None
        else:
            logger.info(f"Current year file {csv_file.name} does not exist")
            return None

    def merge_and_save_data(self, new_data: pd.DataFrame) -> bool:
        """
        Memory-optimized merge of new data with existing current year data

        Args:
            new_data: New DataFrame to merge

        Returns:
            True if successful, False otherwise
        """
        try:
            csv_file, parquet_file = self.get_current_year_files()

            # Clean and validate new data first (small dataset)
            logger.info(f"Cleaning new API data ({len(new_data)} records)...")
            new_data = self.validate_and_clean_data(new_data)

            # Load existing data
            existing_data = self.load_current_year_data()

            if existing_data is not None:
                logger.info(f"Merging with existing data ({len(existing_data)} records)...")
                
                # Memory-optimized merge: avoid cleaning the large existing dataset
                # Only clean the new data and do minimal processing on combined data
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)
                
                # Only perform essential operations on the combined dataset
                logger.info("Performing essential deduplication and sorting...")
                
                # Remove duplicates based on key columns (memory efficient)
                key_columns = ['Arrival_Date', 'State', 'District', 'Market', 'Commodity', 'Variety', 'Grade']
                available_key_columns = [col for col in key_columns if col in combined_data.columns]
                
                if available_key_columns:
                    initial_count = len(combined_data)
                    combined_data = combined_data.drop_duplicates(subset=available_key_columns, keep='last')
                    duplicates_removed = initial_count - len(combined_data)
                    if duplicates_removed > 0:
                        logger.info(f"Removed {duplicates_removed:,} duplicate rows")
                
                # Sort by date (memory efficient)
                combined_data['Arrival_Date'] = pd.to_datetime(combined_data['Arrival_Date'], errors='coerce')
                combined_data = combined_data.dropna(subset=['Arrival_Date'])
                combined_data = combined_data.sort_values('Arrival_Date')
                combined_data['Arrival_Date'] = combined_data['Arrival_Date'].dt.strftime('%Y-%m-%d')
                
            else:
                logger.info("No existing data found, using cleaned new data...")
                combined_data = new_data.copy()

            # Save files with progress logging
            logger.info(f"Saving {len(combined_data)} records to CSV...")
            combined_data.to_csv(csv_file, index=False)
            logger.info(f"✅ Saved CSV: {csv_file.name}")

            logger.info(f"Saving {len(combined_data)} records to Parquet...")
            combined_data.to_parquet(parquet_file, index=False)
            logger.info(f"✅ Saved Parquet: {parquet_file.name}")

            return True

        except Exception as e:
            logger.error(f"Error merging and saving data: {e}")
            return False

    def check_year_rollover(self) -> bool:
        """
        Check if we need to handle year rollover

        Returns:
            True if rollover is needed, False otherwise
        """
        csv_file, _ = self.get_current_year_files()

        if not csv_file.exists():
            return False

        # Check if the current year file has data from previous year
        try:
            df = pd.read_csv(csv_file)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])

            current_year = datetime.now().strftime('%Y')
            file_years = df['Arrival_Date'].dt.strftime('%Y').unique()

            # If file contains data from years other than current, rollover is needed
            other_years = [
                year for year in file_years if year != current_year]

            if other_years:
                logger.info(
                    f"Year rollover detected. File contains data from: {other_years}")
                return True

        except Exception as e:
            logger.error(f"Error checking year rollover: {e}")

        return False

    def handle_year_rollover(self) -> bool:
        """
        Handle year rollover by creating separate files for each year

        Returns:
            True if successful, False otherwise
        """
        try:
            csv_file, parquet_file = self.get_current_year_files()

            if not csv_file.exists():
                logger.info("No current year file to process for rollover")
                return True

            # Load current data
            df = pd.read_csv(csv_file)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])

            # Group by year
            df['year'] = df['Arrival_Date'].dt.strftime('%Y')
            years = df['year'].unique()

            logger.info(f"Processing rollover for years: {years}")

            for year in years:
                year_data = df[df['year'] == year].copy()
                year_data = year_data.drop('year', axis=1)
                year_data['Arrival_Date'] = year_data['Arrival_Date'].dt.strftime(
                    '%Y-%m-%d')

                # Apply comprehensive cleaning and validation to year data
                year_data = self.validate_and_clean_data(year_data)

                # Create year-specific files
                year_csv = self.csv_dir / f"{year}.csv"
                year_parquet = self.parquet_dir / f"{year}.parquet"

                if year_csv.exists() and year_parquet.exists():
                    logger.info(
                        f"Historical files for {year} already exist - preserving existing data")
                    continue

                # Only save files for years that don't already exist
                year_data.to_csv(year_csv, index=False)
                year_data.to_parquet(year_parquet, index=False)

                logger.info(
                    f"Created {year_csv.name} with {len(year_data)} records")
                logger.info(
                    f"Created {year_parquet.name} with {len(year_data)} records")

            return True

        except Exception as e:
            logger.error(f"Error handling year rollover: {e}")
            return False

    def get_all_data_files(self) -> dict:
        """
        Get information about all data files

        Returns:
            Dictionary with file information
        """
        files_info = {
            'csv_files': [],
            'parquet_files': [],
            'total_csv_size': 0,
            'total_parquet_size': 0
        }

        # CSV files
        for csv_file in self.csv_dir.glob("*.csv"):
            size = csv_file.stat().st_size
            files_info['csv_files'].append({
                'name': csv_file.name,
                'size': size,
                'path': str(csv_file)
            })
            files_info['total_csv_size'] += size

        # Parquet files
        for parquet_file in self.parquet_dir.glob("*.parquet"):
            size = parquet_file.stat().st_size
            files_info['parquet_files'].append({
                'name': parquet_file.name,
                'size': size,
                'path': str(parquet_file)
            })
            files_info['total_parquet_size'] += size

        return files_info

    def validate_and_clean_data_light(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Lightweight data validation for large datasets - minimal operations only

        Args:
            df: DataFrame to validate and clean

        Returns:
            Lightly cleaned DataFrame
        """
        logger.info(f"Performing lightweight validation on {len(df)} records...")

        # Only essential operations for large datasets
        initial_count = len(df)

        # Remove rows with missing critical fields
        critical_fields = ['Arrival_Date', 'State', 'District', 'Market', 'Commodity']
        df = df.dropna(subset=critical_fields)

        # Basic date validation
        if 'Arrival_Date' in df.columns:
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
            df['Arrival_Date'] = df['Arrival_Date'].dt.strftime('%Y-%m-%d')

        final_count = len(df)
        if initial_count != final_count:
            logger.info(f"Lightweight validation: {initial_count} → {final_count} records")

        return df

    def validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced data validation and cleaning matching transform_data.py standards

        Args:
            df: DataFrame to validate and clean

        Returns:
            Cleaned and validated DataFrame
        """
        logger.info("Performing enhanced data validation and cleaning...")

        initial_count = len(df)

        # Remove rows with missing critical fields
        critical_fields = ['Arrival_Date', 'State',
                           'District', 'Market', 'Commodity']
        df = df.dropna(subset=critical_fields)

        # Clean string fields using transform_data.py logic
        string_fields = ['State', 'District',
                         'Market', 'Commodity', 'Variety', 'Grade']
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_string_field)
        logger.info(
            f"Cleaned string fields: {', '.join([f for f in string_fields if f in df.columns])}")

        # Clean price fields using transform_data.py logic
        price_fields = ['Min_Price', 'Max_Price', 'Modal_Price']
        for field in price_fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_price_field)
        logger.info(
            f"Cleaned price fields: {', '.join([f for f in price_fields if f in df.columns])}")

        # Format dates to ISO 8601 standard using transform_data.py logic
        if 'Arrival_Date' in df.columns:
            df['Arrival_Date'] = df['Arrival_Date'].apply(format_date_iso)
            logger.info("Converted dates to ISO 8601 format (YYYY-MM-DD)")

        # Remove rows with invalid dates after formatting
        try:
            df['Arrival_Date'] = pd.to_datetime(
                df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
        except Exception as e:
            logger.warning(f"Date validation error: {e}")

        # Remove rows with invalid price data (all price fields are null or zero)
        available_price_fields = [
            col for col in price_fields if col in df.columns]

        if available_price_fields:
            # Convert price fields to numeric
            for col in available_price_fields:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Remove rows where all price fields are null or zero
            price_mask = df[available_price_fields].notna().any(axis=1) & \
                (df[available_price_fields] > 0).any(axis=1)
            df = df[price_mask]

        # Remove duplicate records based on key columns (matching transform_data.py)
        key_columns = ['Arrival_Date', 'State', 'District',
                       'Market', 'Commodity', 'Variety', 'Grade']
        available_key_columns = [
            col for col in key_columns if col in df.columns]

        if available_key_columns:
            # Remove duplicates based on key columns, keeping the first occurrence
            duplicate_count_before = len(df)
            df = df.drop_duplicates(subset=available_key_columns, keep='first')
            duplicates_removed = duplicate_count_before - len(df)
            if duplicates_removed > 0:
                logger.info(
                    f"Removed {duplicates_removed:,} duplicate rows based on key columns: {', '.join(available_key_columns)}")
            else:
                logger.info("No duplicates found based on key columns")

        # Parse dates for sorting (matching transform_data.py)
        df['Arrival_DateTime'] = df['Arrival_Date'].apply(parse_date)

        # Sort by date first, then by state, then by other fields (matching transform_data.py)
        sort_columns = ['Arrival_DateTime', 'State',
                        'District', 'Market', 'Commodity']
        available_sort_columns = [
            col for col in sort_columns if col in df.columns]
        df = df.sort_values(available_sort_columns)

        # Remove the temporary datetime column
        df = df.drop('Arrival_DateTime', axis=1)

        # Final validation: Ensure no duplicates remain (matching transform_data.py)
        if available_key_columns:
            duplicate_count = df.duplicated(subset=available_key_columns).sum()
            if duplicate_count > 0:
                logger.warning(
                    f"WARNING: {duplicate_count} duplicates still found! Removing them now...")
                df = df.drop_duplicates(
                    subset=available_key_columns, keep='first')
            else:
                logger.info("Final validation: No duplicates found")

        # Data quality checks
        final_count = len(df)
        removed_count = initial_count - final_count

        if removed_count > 0:
            logger.info(
                f"Data validation removed {removed_count} invalid/duplicate records")
            logger.info(
                f"Validation summary: {initial_count} → {final_count} records (sorted by date and state)")

        return df

    def get_data_quality_report(self, df: pd.DataFrame) -> dict:
        """
        Generate a data quality report

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary with data quality metrics
        """
        report = {
            'total_records': len(df),
            'date_range': {},
            'missing_data': {},
            'unique_values': {},
            'price_statistics': {}
        }

        # Date range analysis
        if 'Arrival_Date' in df.columns:
            df['Arrival_Date'] = pd.to_datetime(
                df['Arrival_Date'], errors='coerce')
            valid_dates = df['Arrival_Date'].dropna()
            if not valid_dates.empty:
                report['date_range'] = {
                    'earliest': valid_dates.min().strftime('%Y-%m-%d'),
                    'latest': valid_dates.max().strftime('%Y-%m-%d'),
                    'unique_dates': valid_dates.nunique()
                }

        # Missing data analysis
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                report['missing_data'][col] = {
                    'count': int(missing_count),
                    'percentage': round((missing_count / len(df)) * 100, 2)
                }

        # Unique values for key categorical fields
        categorical_fields = ['State', 'District',
                              'Market', 'Commodity', 'Variety', 'Grade']
        for field in categorical_fields:
            if field in df.columns:
                report['unique_values'][field] = int(df[field].nunique())

        # Price statistics
        price_fields = ['Min_Price', 'Max_Price', 'Modal_Price']
        for field in price_fields:
            if field in df.columns:
                price_data = pd.to_numeric(df[field], errors='coerce').dropna()
                if not price_data.empty:
                    report['price_statistics'][field] = {
                        'min': float(price_data.min()),
                        'max': float(price_data.max()),
                        'mean': round(float(price_data.mean()), 2),
                        'median': round(float(price_data.median()), 2)
                    }

        return report
