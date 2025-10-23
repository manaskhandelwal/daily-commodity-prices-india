"""
File manager module for CSV and Parquet operations
Handles monthly file management, merging, and rollover operations
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional

from .config import DATA_DIR, CSV_DIR, PARQUET_DIR

logger = logging.getLogger(__name__)


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

    def get_current_month_files(self) -> Tuple[Path, Path]:
        """
        Get the current month's CSV and Parquet file paths

        Returns:
            Tuple of (csv_path, parquet_path)
        """
        current_month = datetime.now().strftime('%Y-%m')
        csv_file = self.csv_dir / f"commodity_prices_{current_month}.csv"
        parquet_file = self.parquet_dir / \
            f"commodity_prices_{current_month}.parquet"
        return csv_file, parquet_file

    def load_current_month_data(self) -> Optional[pd.DataFrame]:
        """
        Load the current month's data from CSV file

        Returns:
            DataFrame with current month's data or None if file doesn't exist
        """
        csv_file, _ = self.get_current_month_files()

        if csv_file.exists():
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"Loaded {len(df)} records from {csv_file.name}")
                return df
            except Exception as e:
                logger.error(f"Error loading current month data: {e}")
                return None
        else:
            logger.info(f"Current month file {csv_file.name} does not exist")
            return None

    def merge_and_save_data(self, new_data: pd.DataFrame) -> bool:
        """
        Merge new data with existing current month data and save

        Args:
            new_data: New DataFrame to merge

        Returns:
            True if successful, False otherwise
        """
        try:
            csv_file, parquet_file = self.get_current_month_files()

            # Load existing data
            existing_data = self.load_current_month_data()

            if existing_data is not None:
                # Merge with existing data
                combined_data = pd.concat(
                    [existing_data, new_data], ignore_index=True)

                # Remove duplicates
                from .config import KEY_COLUMNS
                available_key_columns = [
                    col for col in KEY_COLUMNS if col in combined_data.columns]

                initial_count = len(combined_data)
                combined_data = combined_data.drop_duplicates(
                    subset=available_key_columns, keep='last')
                final_count = len(combined_data)

                if initial_count > final_count:
                    logger.info(
                        f"Removed {initial_count - final_count} duplicate records during merge")
            else:
                combined_data = new_data.copy()

            # Sort by date
            combined_data['Arrival_Date'] = pd.to_datetime(
                combined_data['Arrival_Date'])
            combined_data = combined_data.sort_values('Arrival_Date')
            combined_data['Arrival_Date'] = combined_data['Arrival_Date'].dt.strftime(
                '%Y-%m-%d')

            # Save CSV file
            combined_data.to_csv(csv_file, index=False)
            logger.info(
                f"Saved {len(combined_data)} records to {csv_file.name}")

            # Save Parquet file
            combined_data.to_parquet(parquet_file, index=False)
            logger.info(
                f"Saved {len(combined_data)} records to {parquet_file.name}")

            return True

        except Exception as e:
            logger.error(f"Error merging and saving data: {e}")
            return False

    def check_month_rollover(self) -> bool:
        """
        Check if we need to handle month rollover

        Returns:
            True if rollover is needed, False otherwise
        """
        csv_file, _ = self.get_current_month_files()

        if not csv_file.exists():
            return False

        # Check if the current month file has data from previous month
        try:
            df = pd.read_csv(csv_file)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])

            current_month = datetime.now().strftime('%Y-%m')
            file_months = df['Arrival_Date'].dt.strftime('%Y-%m').unique()

            # If file contains data from months other than current, rollover is needed
            other_months = [
                month for month in file_months if month != current_month]

            if other_months:
                logger.info(
                    f"Month rollover detected. File contains data from: {other_months}")
                return True

        except Exception as e:
            logger.error(f"Error checking month rollover: {e}")

        return False

    def handle_month_rollover(self) -> bool:
        """
        Handle month rollover by creating separate files for each month

        Returns:
            True if successful, False otherwise
        """
        try:
            csv_file, parquet_file = self.get_current_month_files()

            if not csv_file.exists():
                logger.info("No current month file to process for rollover")
                return True

            # Load current data
            df = pd.read_csv(csv_file)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])

            # Group by month
            df['month'] = df['Arrival_Date'].dt.strftime('%Y-%m')
            months = df['month'].unique()

            logger.info(f"Processing rollover for months: {months}")

            for month in months:
                month_data = df[df['month'] == month].copy()
                month_data = month_data.drop('month', axis=1)
                month_data['Arrival_Date'] = month_data['Arrival_Date'].dt.strftime(
                    '%Y-%m-%d')

                # Create month-specific files
                month_csv = self.csv_dir / f"commodity_prices_{month}.csv"
                month_parquet = self.parquet_dir / \
                    f"commodity_prices_{month}.parquet"

                # Save files
                month_data.to_csv(month_csv, index=False)
                month_data.to_parquet(month_parquet, index=False)

                logger.info(
                    f"Created {month_csv.name} with {len(month_data)} records")
                logger.info(
                    f"Created {month_parquet.name} with {len(month_data)} records")

            return True

        except Exception as e:
            logger.error(f"Error handling month rollover: {e}")
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

    def cleanup_old_files(self, keep_months: int = 12) -> bool:
        """
        Clean up old files beyond the specified number of months

        Args:
            keep_months: Number of recent months to keep

        Returns:
            True if successful, False otherwise
        """
        try:
            current_date = datetime.now()
            cutoff_date = current_date.replace(day=1)

            for _ in range(keep_months):
                if cutoff_date.month == 1:
                    cutoff_date = cutoff_date.replace(
                        year=cutoff_date.year - 1, month=12)
                else:
                    cutoff_date = cutoff_date.replace(
                        month=cutoff_date.month - 1)

            cutoff_month = cutoff_date.strftime('%Y-%m')
            logger.info(f"Cleaning up files older than {cutoff_month}")

            deleted_count = 0

            # Clean CSV files
            for csv_file in self.csv_dir.glob("commodity_prices_*.csv"):
                file_month = csv_file.stem.replace('commodity_prices_', '')
                if file_month < cutoff_month:
                    csv_file.unlink()
                    deleted_count += 1
                    logger.info(f"Deleted old CSV file: {csv_file.name}")

            # Clean Parquet files
            for parquet_file in self.parquet_dir.glob("commodity_prices_*.parquet"):
                file_month = parquet_file.stem.replace('commodity_prices_', '')
                if file_month < cutoff_month:
                    parquet_file.unlink()
                    deleted_count += 1
                    logger.info(
                        f"Deleted old Parquet file: {parquet_file.name}")

            if deleted_count == 0:
                logger.info("No old files found to clean up")
            else:
                logger.info(f"Cleaned up {deleted_count} old files")

            return True

        except Exception as e:
            logger.error(f"Error cleaning up old files: {e}")
            return False

    def validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced data validation and cleaning inspired by testing directory patterns

        Args:
            df: DataFrame to validate and clean

        Returns:
            Cleaned and validated DataFrame
        """
        logger.info("Performing enhanced data validation and cleaning...")
        
        initial_count = len(df)
        
        # Remove rows with missing critical fields
        critical_fields = ['Arrival_Date', 'State', 'District', 'Market', 'Commodity']
        df = df.dropna(subset=critical_fields)
        
        # Remove rows with invalid dates
        try:
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
        except Exception as e:
            logger.warning(f"Date validation error: {e}")
        
        # Remove rows with invalid price data (all price fields are null or zero)
        price_fields = ['Min_Price', 'Max_Price', 'Modal_Price']
        available_price_fields = [col for col in price_fields if col in df.columns]
        
        if available_price_fields:
            # Convert price fields to numeric
            for col in available_price_fields:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove rows where all price fields are null or zero
            price_mask = df[available_price_fields].notna().any(axis=1) & \
                        (df[available_price_fields] > 0).any(axis=1)
            df = df[price_mask]
        
        # Remove duplicate records based on key columns (more comprehensive)
        key_columns = ['Arrival_Date', 'State', 'District', 'Market', 'Commodity', 'Variety', 'Grade']
        available_key_columns = [col for col in key_columns if col in df.columns]
        
        if available_key_columns:
            df = df.drop_duplicates(subset=available_key_columns, keep='first')
        
        # Data quality checks
        final_count = len(df)
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            logger.info(f"Data validation removed {removed_count} invalid/duplicate records")
            logger.info(f"Validation summary: {initial_count} → {final_count} records")
        
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
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
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
        categorical_fields = ['State', 'District', 'Market', 'Commodity', 'Variety', 'Grade']
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
