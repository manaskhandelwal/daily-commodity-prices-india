"""
Data seeding module for initial dataset download
Only requires Kaggle credentials, not API_KEY
"""

import logging
from pathlib import Path
from .file_manager import FileManager
from .kaggle_integration import KaggleIntegration
from .state_manager import StateManager


class DataSeeder:
    """Handles initial data seeding from Kaggle"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.file_manager = FileManager()
        self.kaggle_integration = KaggleIntegration()
        self.state_manager = StateManager()
    
    def is_seeding_needed(self) -> bool:
        """Check if seeding is needed"""
        try:
            # Check if data directory exists and has files
            if not self.file_manager.data_dir.exists():
                self.logger.info("Data directory does not exist - seeding needed")
                return True
            
            # Check if there are any data files
            data_files = self.file_manager.get_all_data_files()
            has_csv_files = data_files.get('csv_files', [])
            has_parquet_files = data_files.get('parquet_files', [])
            
            if not has_csv_files and not has_parquet_files:
                self.logger.info("No data files found - seeding needed")
                return True
            
            # Check if state file exists and is properly initialized
            state = self.state_manager.load_state()
            if not state or 'initialization_date' not in state:
                self.logger.info("State not properly initialized - seeding needed")
                return True
            
            self.logger.info("Data environment already initialized - seeding not needed")
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking seeding status: {e}")
            return True
    
    def seed_data(self, force: bool = False) -> bool:
        """
        Seed the data environment with historical dataset
        
        Args:
            force: Force seeding even if data already exists
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not force and not self.is_seeding_needed():
                self.logger.info("Seeding not needed - environment already initialized")
                return True
            
            self.logger.info("=" * 60)
            self.logger.info("DATA SEEDING: Downloading historical dataset")
            self.logger.info("=" * 60)
            
            # Check Kaggle configuration
            if not self.kaggle_integration.check_kaggle_config():
                self.logger.error("Kaggle configuration check failed")
                self.logger.error("Please ensure KAGGLE_USERNAME, KAGGLE_KEY, and KAGGLE_DATASET are set")
                return False
            
            # Download dataset
            self.logger.info("Downloading dataset from Kaggle...")
            if not self.kaggle_integration.download_dataset():
                self.logger.error("Failed to download dataset")
                return False
            
            # Copy metadata
            self.kaggle_integration._copy_metadata_file()
            self.logger.info("Metadata file copied")
            
            # Initialize state
            self.state_manager.reset_state()
            self.state_manager.mark_initialization_complete()
            
            # Verify success
            data_files = self.file_manager.get_all_data_files()
            has_csv_files = data_files.get('csv_files', [])
            has_parquet_files = data_files.get('parquet_files', [])
            
            if has_csv_files or has_parquet_files:
                self.logger.info("=" * 60)
                self.logger.info("SEEDING COMPLETED SUCCESSFULLY!")
                self.logger.info("=" * 60)
                self.logger.info("Downloaded files:")
                
                if has_csv_files:
                    self.logger.info(f"  csv_files: {len(has_csv_files)} files")
                    years = sorted([f['name'].split('.')[0] for f in has_csv_files])
                    self.logger.info(f"    Years: {years}")
                
                if has_parquet_files:
                    self.logger.info(f"  parquet_files: {len(has_parquet_files)} files")
                    years = sorted([f['name'].split('.')[0] for f in has_parquet_files])
                    self.logger.info(f"    Years: {years}")
                
                return True
            else:
                self.logger.error("Seeding completed but no data files found")
                return False
                
        except Exception as e:
            self.logger.error(f"Seeding failed: {e}")
            return False


def main():
    """Standalone seeding function for command line usage"""
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    seeder = DataSeeder()
    
    # Check if force flag is provided
    force = '--force' in sys.argv
    
    if seeder.seed_data(force=force):
        print("Seeding completed successfully!")
        sys.exit(0)
    else:
        print("Seeding failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()