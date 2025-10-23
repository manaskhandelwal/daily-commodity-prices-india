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
        self.logger = self.setup_logging()
        
        # Initialize core components
        self.file_manager = FileManager()
        self.state_manager = StateManager()
        self.seeder = DataSeeder()
        
        # Check if seeding is needed
        seeding_needed = self.seeder.is_seeding_needed()
        
        # Validate configuration based on seeding mode
        try:
            validate_config(seeding_mode=seeding_needed)
        except ValueError as e:
            if seeding_needed:
                self.logger.info("API_KEY not required for initial seeding")
            else:
                self.logger.error(f"Configuration validation failed: {e}")
                raise
        
        # Initialize data_fetcher only if not in seeding mode
        if not seeding_needed:
            self.data_fetcher = DataFetcher()
        else:
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
        """Main execution method with built-in initialization"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("DAILY COMMODITY PRICES UPDATER STARTED")
            self.logger.info("=" * 60)

            # Check if environment needs seeding
            if self.seeder.is_seeding_needed():
                self.logger.info("Environment not initialized. Starting seeding process...")
                
                if not self.seeder.seed_data():
                    self.logger.error("Failed to seed environment")
                    return False
                
                # Now initialize data_fetcher after successful seeding
                try:
                    validate_config(seeding_mode=False)  # Require API_KEY now
                    self.data_fetcher = DataFetcher()
                except ValueError as e:
                    self.logger.error(f"API_KEY required for daily operations: {e}")
                    return False

            # Proceed with daily update
            self.logger.info("Starting daily data update process...")

            # Load current state
            state = self.state_manager.load_state()
            last_hash = state.get('last_data_hash')
            processed_dates = state.get('processed_dates', [])

            # Fetch latest data
            self.logger.info("Fetching latest data from API...")
            raw_data = self.data_fetcher.fetch_latest_data()

            if raw_data is None:
                self.logger.warning("No data fetched from API")
                return False

            # Clean and process data
            self.logger.info("Processing and cleaning data...")
            processed_data = self.data_fetcher.clean_and_process_data(raw_data)

            if processed_data is None or processed_data.empty:
                self.logger.warning("No valid data after processing")
                return False

            # Check for new data
            if not self.data_fetcher.is_new_data(processed_data, last_hash, processed_dates):
                self.logger.info("No new data to process")
                return True

            # Merge and save data
            self.logger.info("Merging and saving new data...")
            if self.file_manager.merge_and_save_data(processed_data):
                # Update state
                new_hash = self.data_fetcher.calculate_data_hash(processed_data)
                new_dates = processed_data['Arrival_Date'].unique().tolist()
                
                self.state_manager.update_state(
                    last_data_hash=new_hash,
                    processed_dates=list(set(processed_dates + new_dates))
                )
                
                self.logger.info("=" * 60)
                self.logger.info("DAILY UPDATE COMPLETED SUCCESSFULLY")
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
