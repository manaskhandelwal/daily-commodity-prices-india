#!/usr/bin/env python3

from src.config import LOG_LEVEL, DATA_DIR, validate_config
from src import (
    DataFetcher, FileManager, KaggleIntegration,
    StateManager, DataSeeder
)
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


class DailyUpdater:
    """Main orchestrator class using modular components"""

    def __init__(self):
        """Initialize the daily updater with all components"""
        self.logger = setup_logging()
        
        # Initialize core components
        self.file_manager = FileManager()
        self.state_manager = StateManager()
        self.seeder = DataSeeder()
        self.kaggle_integration = KaggleIntegration()
        
        # Check if seeding is needed (for Kaggle dataset download)
        seeding_needed = self.seeder.is_seeding_needed()
        
        # For seeding mode, we only validate basic config (no API_KEY required yet)
        if seeding_needed:
            try:
                validate_config(seeding_mode=True)
                self.logger.info("Seeding mode: API_KEY will be validated before API operations")
            except ValueError as e:
                self.logger.error(f"Basic configuration validation failed: {e}")
                raise
        
        # Initialize data_fetcher as None initially (will be created when needed)
        self.data_fetcher = None

    def is_environment_initialized(self) -> bool:
        """Check if the data environment is properly initialized"""
        try:
            # Check if data directory exists and has files
            if not self.file_manager.data_dir.exists():
                self.logger.debug(
                    f"Data directory does not exist: {self.file_manager.data_dir}")
                return False

            # Check if there are any data files
            data_files = self.file_manager.get_all_data_files()
            if not data_files:
                self.logger.debug("No data files found in data directory")
                return False

            # Check if state file exists
            state = self.state_manager.load_state()
            if not state or 'initialization_date' not in state:
                self.logger.debug(
                    "State file does not exist or is not properly initialized")
                return False

            self.logger.debug("Environment is properly initialized")
            return True

        except Exception as e:
            self.logger.error(
                f"Error checking environment initialization: {e}")
            return False



    def run(self):
        """Main execution method with comprehensive deployment strategy"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("DAILY COMMODITY PRICES UPDATER STARTED")
            self.logger.info("=" * 60)

            # STEP 1: Ensure we have the Kaggle dataset locally (download if needed)
            if self.seeder.is_seeding_needed():
                self.logger.info("No local dataset found. Downloading from Kaggle...")
                
                if not self.seeder.seed_data():
                    self.logger.error("Failed to download dataset from Kaggle")
                    return False
                
                self.logger.info("Successfully downloaded dataset from Kaggle")

            # STEP 2: Initialize API data fetcher (required for all operations)
            try:
                validate_config(seeding_mode=False)  # Require API_KEY for all operations
                if not self.data_fetcher:
                    self.data_fetcher = DataFetcher()
            except ValueError as e:
                self.logger.error(f"API_KEY required for operations: {e}")
                return False

            # STEP 3: Always fetch latest data from API (on every deployment and daily run)
            self.logger.info("Fetching latest data from API...")
            raw_data = self.data_fetcher.fetch_latest_data()

            if raw_data is None:
                self.logger.warning("No data fetched from API")
                return False

            # STEP 4: Process and clean API data
            self.logger.info("Processing and cleaning API data...")
            processed_data = self.data_fetcher.clean_and_process_data(raw_data)

            if processed_data is None or processed_data.empty:
                self.logger.warning("No valid data after processing")
                return False

            # STEP 5: Load current state for comparison
            state = self.state_manager.load_state()
            last_hash = state.get('last_data_hash')
            processed_dates = state.get('processed_dates', [])

            # STEP 6: Check if we have new data (but always proceed on first deployment)
            is_new_data = self.data_fetcher.is_new_data(processed_data, last_hash, processed_dates)
            is_first_deployment = not state.get('last_successful_upload')
            
            if not is_new_data and not is_first_deployment:
                self.logger.info("No new data to process and not first deployment")
                return True

            # STEP 7: Merge API data with existing Kaggle dataset and save
            if is_first_deployment:
                self.logger.info("First deployment: Merging API data with downloaded Kaggle dataset...")
            else:
                self.logger.info("Daily update: Merging new API data with existing dataset...")
                
            if self.file_manager.merge_and_save_data(processed_data):
                # STEP 8: Update state with new data
                new_hash = self.data_fetcher.calculate_data_hash(processed_data)
                new_dates = processed_data['Arrival_Date'].unique().tolist()
                
                self.state_manager.update_data_hash(new_hash)
                self.state_manager.update_processed_dates(new_dates)
                self.state_manager.increment_records_processed(len(processed_data))
                
                # STEP 9: Upload merged dataset to Kaggle (always upload after merge)
                self.logger.info("Uploading merged dataset to Kaggle...")
                upload_success = self.kaggle_integration.upload_dataset()
                
                if upload_success:
                    self.logger.info("Merged dataset successfully uploaded to Kaggle")
                    self.state_manager.mark_successful_upload()
                    
                    if is_first_deployment:
                        self.logger.info("✅ First deployment completed: Kaggle dataset updated with latest API data")
                    else:
                        self.logger.info("✅ Daily update completed: Kaggle dataset updated with new data")
                else:
                    self.logger.warning("Failed to upload merged dataset to Kaggle, but local merge was successful")
                
                self.logger.info("=" * 60)
                self.logger.info("DEPLOYMENT/UPDATE COMPLETED SUCCESSFULLY")
                self.logger.info("=" * 60)
                return True
            else:
                self.logger.error("Failed to merge and save data")
                return False

        except Exception as e:
            self.logger.error(f"Daily update failed: {e}")
            return False


def main():
    """Entry point for the script"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        updater = DailyUpdater()
        updater.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
