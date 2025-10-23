#!/usr/bin/env python3

from src.config import LOG_LEVEL, DATA_DIR
from src import (
    DataFetcher, FileManager, KaggleIntegration,
    StateManager, validate_config
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
        self.logger = setup_logging()

        # Validate configuration
        try:
            validate_config()
        except ValueError as e:
            self.logger.error(f"Configuration error: {e}")
            sys.exit(1)

        # Initialize components
        self.data_fetcher = DataFetcher()
        self.file_manager = FileManager()
        self.kaggle_integration = KaggleIntegration()
        self.state_manager = StateManager()

        self.logger.info("DailyUpdater initialized with modular components")

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

    def seed_environment(self) -> bool:
        """Initialize the environment by downloading the complete dataset"""
        self.logger.info("=" * 60)
        self.logger.info("SEEDING MODE: Initializing data environment")
        self.logger.info("=" * 60)

        try:
            # Check Kaggle configuration
            if not self.kaggle_integration.check_kaggle_config():
                self.logger.error("Kaggle configuration check failed")
                return False

            # Download and extract dataset
            if not self.kaggle_integration.download_and_extract_dataset():
                self.logger.error("Failed to download and extract dataset")
                return False

            # Copy dataset-metadata.json to data directory
            if not self.kaggle_integration.copy_metadata_file():
                self.logger.warning("Failed to copy dataset-metadata.json")

            # Verify the environment is now properly initialized
            if not self.is_environment_initialized():
                self.logger.error(
                    "Environment still not initialized after seeding")
                return False

            # Initialize state
            self.state_manager.reset_state()
            self.state_manager.mark_initialization()

            self.logger.info("Environment seeding completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to seed environment: {e}")
            return False

    def run(self):
        """Main execution method with self-initialization capability"""
        self.logger.info("=" * 60)
        self.logger.info("Daily Commodity Prices Update - Starting")
        self.logger.info(f"Timestamp: {datetime.now().isoformat()}")
        self.logger.info("=" * 60)

        try:
            # Step 0: Check if environment is initialized
            self.logger.info("Step 0: Checking environment initialization...")

            if not self.is_environment_initialized():
                self.logger.info(
                    "Environment not initialized. Entering seeding mode...")

                if not self.seed_environment():
                    self.logger.error(
                        "Failed to initialize environment. Exiting.")
                    return

                self.logger.info(
                    "Environment successfully initialized. Proceeding with normal operation...")
            else:
                self.logger.info(
                    "Environment already initialized. Proceeding with daily update...")

            # Step 1: Fetch latest data
            self.logger.info("Step 1: Fetching latest data from API...")
            state = self.state_manager.load_state()

            raw_data = self.data_fetcher.fetch_latest_data()
            if raw_data is None or raw_data.empty:
                self.logger.info("No new data fetched from API")
                # Still upload current dataset even if no new data
                self.logger.info(
                    "Proceeding to upload current dataset to Kaggle...")
                if self.kaggle_integration.upload_dataset():
                    self.state_manager.mark_successful_upload()
                    self.logger.info("Dataset upload completed successfully")
                else:
                    self.logger.error("Dataset upload failed")
                return

            # Step 2: Clean and process data
            self.logger.info("Step 2: Cleaning and processing data...")
            cleaned_data = self.data_fetcher.clean_and_process_data(raw_data)

            # Step 2.1: Enhanced data validation and cleaning
            self.logger.info("Step 2.1: Performing enhanced data validation...")
            validated_data = self.file_manager.validate_and_clean_data(cleaned_data)
            
            # Step 2.2: Generate data quality report
            quality_report = self.file_manager.get_data_quality_report(validated_data)
            self.logger.info(f"Data quality report: {quality_report}")

            if validated_data.empty:
                self.logger.info("No data remaining after validation and cleaning")
                # Still upload current dataset even if no new data after cleaning
                self.logger.info(
                    "Proceeding to upload current dataset to Kaggle...")
                if self.kaggle_integration.upload_dataset():
                    self.state_manager.mark_successful_upload()
                    self.logger.info("Dataset upload completed successfully")
                else:
                    self.logger.error("Dataset upload failed")
                return

            # Step 3: Check for new data
            self.logger.info("Step 3: Checking for new data...")
            if not self.data_fetcher.is_new_data(
                validated_data,
                state.get('last_data_hash'),
                state.get('processed_dates')
            ):
                self.logger.info("No new data detected, skipping processing")
                # Still upload current dataset
                self.logger.info(
                    "Proceeding to upload current dataset to Kaggle...")
                if self.kaggle_integration.upload_dataset():
                    self.state_manager.mark_successful_upload()
                    self.logger.info("Dataset upload completed successfully")
                else:
                    self.logger.error("Dataset upload failed")
                return

            # Step 4: Handle month rollover if needed
            self.logger.info("Step 4: Checking for month rollover...")
            if self.file_manager.check_month_rollover():
                self.logger.info("Month rollover detected, processing...")
                if not self.file_manager.handle_month_rollover():
                    self.logger.error("Failed to handle month rollover")
                    return
                self.logger.info("Month rollover completed successfully")

            # Step 5: Merge and save data
            self.logger.info("Step 5: Merging and saving data...")
            if not self.file_manager.merge_and_save_data(validated_data):
                self.logger.error("Failed to merge and save data")
                return

            # Step 6: Update state
            self.logger.info("Step 6: Updating state...")
            new_hash = self.data_fetcher.calculate_data_hash(validated_data)
            new_dates = validated_data['Arrival_Date'].unique().tolist()

            # Update state using state manager
            self.state_manager.update_data_hash(new_hash)
            self.state_manager.update_processed_dates(new_dates)
            self.state_manager.increment_records_processed(len(validated_data))

            # Step 7: Upload to Kaggle
            self.logger.info("Step 7: Uploading dataset to Kaggle...")
            if not self.kaggle_integration.upload_dataset():
                self.logger.error("Failed to upload dataset to Kaggle")
                return

            # Mark successful upload
            self.state_manager.mark_successful_upload()

            self.logger.info("=== DAILY UPDATE COMPLETED SUCCESSFULLY ===")
            self.logger.info(f"Processed {len(validated_data)} new records")

            # Get updated state for logging
            updated_state = self.state_manager.load_state()
            self.logger.info(
                f"Total records processed to date: {updated_state.get('total_records_processed', 0)}")

        except Exception as e:
            self.logger.error(f"Unexpected error during daily update: {e}")
            raise

        finally:
            self.logger.info("=" * 60)
            self.logger.info("Daily Commodity Prices Update - Finished")
            self.logger.info("=" * 60)


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
