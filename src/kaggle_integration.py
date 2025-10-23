"""
Kaggle integration module for upload and download operations
Handles Kaggle dataset downloads and uploads with proper error handling
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
import shutil
import zipfile
from pathlib import Path
from typing import Optional

from config import (
    KAGGLE_USERNAME, KAGGLE_KEY, KAGGLE_DATASET,
    KAGGLE_DOWNLOAD_TIMEOUT, KAGGLE_UPLOAD_TIMEOUT,
    KAGGLE_MAX_RETRIES, KAGGLE_RETRY_DELAY, KAGGLE_TEMP_DIR,
    DATA_DIR, CSV_DIR, PARQUET_DIR
)

logger = logging.getLogger(__name__)


class KaggleIntegration:
    """Handles Kaggle dataset operations"""

    def __init__(self):
        self.username = KAGGLE_USERNAME
        self.key = KAGGLE_KEY
        self.dataset = KAGGLE_DATASET
        self.data_dir = DATA_DIR
        self._validate_credentials()

    def _validate_credentials(self):
        """Validate Kaggle credentials"""
        if not self.username or not self.key:
            raise ValueError(
                "KAGGLE_USERNAME and KAGGLE_KEY environment variables are required")

        if not self.dataset:
            raise ValueError("KAGGLE_DATASET environment variable is required")

    def download_dataset(self) -> bool:
        """
        Download the complete dataset from Kaggle with retry logic for large files

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting download of Kaggle dataset: {self.dataset}")
        logger.info(f"Timeout set to {KAGGLE_DOWNLOAD_TIMEOUT} seconds for large dataset")
        
        for attempt in range(1, KAGGLE_MAX_RETRIES + 1):
            logger.info(f"Download attempt {attempt}/{KAGGLE_MAX_RETRIES}")
            
            # Setup temp directory
            temp_dir_context = None
            temp_path = None
            use_custom_temp = False
            
            try:
                # Use custom temp directory if specified for large downloads
                if KAGGLE_TEMP_DIR and Path(KAGGLE_TEMP_DIR).exists():
                    temp_path = Path(KAGGLE_TEMP_DIR) / f"kaggle_download_{int(time.time())}"
                    temp_path.mkdir(parents=True, exist_ok=True)
                    use_custom_temp = True
                    logger.info(f"Using custom temp directory: {temp_path}")
                else:
                    temp_dir_context = tempfile.TemporaryDirectory()
                    temp_path = Path(temp_dir_context.name)
                    use_custom_temp = False
                    logger.info(f"Using system temp directory: {temp_path}")
                
                # Check available disk space
                disk_usage = shutil.disk_usage(temp_path)
                available_gb = disk_usage.free / (1024**3)
                logger.info(f"Available disk space: {available_gb:.1f} GB")
                
                if available_gb < 15:  # Need ~15GB for 7GB download + unzip
                    logger.error(f"Insufficient disk space. Need at least 15GB, have {available_gb:.1f}GB")
                    if attempt < KAGGLE_MAX_RETRIES:
                        logger.info(f"Retrying in {KAGGLE_RETRY_DELAY} seconds...")
                        time.sleep(KAGGLE_RETRY_DELAY)
                        continue
                    else:
                        return False

                # Download dataset to temporary directory (without unzip first)
                cmd = [
                    'kaggle', 'datasets', 'download',
                    self.dataset,
                    '--path', str(temp_path)
                    # Remove --unzip to download zip first, then unzip manually
                ]

                logger.info(f"Running command: {' '.join(cmd)}")
                logger.info("Note: Large dataset download may take 30-60 minutes...")

                result = subprocess.run(
                    cmd,
                    timeout=KAGGLE_DOWNLOAD_TIMEOUT,
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    error_msg = result.stderr.strip()
                    logger.error(f"Kaggle download failed (attempt {attempt}): {error_msg}")
                    
                    # Check if it's a retryable error
                    if attempt < KAGGLE_MAX_RETRIES and self._is_retryable_error(error_msg):
                        logger.info(f"Retrying in {KAGGLE_RETRY_DELAY} seconds...")
                        time.sleep(KAGGLE_RETRY_DELAY)
                        continue
                    else:
                        return False

                logger.info("Dataset downloaded successfully")
                logger.info(f"Download output: {result.stdout}")

                # Manual unzip for better control
                if not self._unzip_dataset(temp_path):
                    if attempt < KAGGLE_MAX_RETRIES:
                        logger.info(f"Unzip failed, retrying in {KAGGLE_RETRY_DELAY} seconds...")
                        time.sleep(KAGGLE_RETRY_DELAY)
                        continue
                    else:
                        return False

                # Copy contents to data directory
                success = self._copy_downloaded_data(temp_path)
                return success

            except subprocess.TimeoutExpired:
                logger.error(f"Download timed out after {KAGGLE_DOWNLOAD_TIMEOUT} seconds (attempt {attempt})")
                if attempt < KAGGLE_MAX_RETRIES:
                    logger.info(f"Retrying in {KAGGLE_RETRY_DELAY} seconds...")
                    time.sleep(KAGGLE_RETRY_DELAY)
                    continue
                else:
                    logger.error("All download attempts failed due to timeout")
                    return False
            except Exception as e:
                logger.error(f"Error downloading dataset (attempt {attempt}): {e}")
                if attempt < KAGGLE_MAX_RETRIES:
                    logger.info(f"Retrying in {KAGGLE_RETRY_DELAY} seconds...")
                    time.sleep(KAGGLE_RETRY_DELAY)
                    continue
                else:
                    logger.error("All download attempts failed")
                    return False
            finally:
                # Cleanup temp directory
                if use_custom_temp and temp_path and temp_path.exists():
                    try:
                        shutil.rmtree(temp_path)
                        logger.info(f"Cleaned up custom temp directory: {temp_path}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup custom temp directory: {e}")
                elif temp_dir_context:
                    try:
                        temp_dir_context.cleanup()
                    except Exception as e:
                        logger.warning(f"Failed to cleanup temp directory: {e}")

        return False

    def _copy_downloaded_data(self, temp_path: Path) -> bool:
        """
        Copy downloaded data from temp directory to data directory

        Args:
            temp_path: Path to temporary download directory

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure data directory exists
            self.data_dir.mkdir(parents=True, exist_ok=True)

            # Find and copy all files
            copied_files = 0

            for item in temp_path.rglob('*'):
                if item.is_file():
                    # Calculate relative path
                    rel_path = item.relative_to(temp_path)
                    dest_path = self.data_dir / rel_path

                    # Create parent directories if needed
                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                    # Copy file
                    shutil.copy2(item, dest_path)
                    copied_files += 1
                    logger.info(f"Copied: {rel_path}")

            logger.info(
                f"Successfully copied {copied_files} files to {self.data_dir}")

            # Copy dataset-metadata.json from script directory if it exists
            self._copy_metadata_file()

            return True

        except Exception as e:
            logger.error(f"Error copying downloaded data: {e}")
            return False

    def _copy_metadata_file(self):
        """Generate dataset-metadata.json directly in the data directory with dynamic id"""
        try:
            dest_metadata = self.data_dir / "dataset-metadata.json"
            
            # Get the dataset ID from environment variable
            kaggle_dataset = os.getenv('KAGGLE_DATASET')
            if not kaggle_dataset:
                logger.warning("KAGGLE_DATASET environment variable not set, using default id")
                kaggle_dataset = "khandelwalmanas/daily-commodity-prices-india"
            
            # Generate the metadata directly
            metadata = {
                "id": kaggle_dataset,
                "title": "Daily Market Prices of Commodity India",
                "description": "Daily market prices of agricultural commodities across India from **2001-2025**. Contains **75+ million** records covering **374 unique commodities** and **1,504 varieties** from various mandis (wholesale markets). Commodity Like: Vegetables, Fruits, Grains, Spices, etc.\r\n\r\nCleaned, deduplicated, and sorted by date and commodity for analysis.\r\n\r\n### Column Schema\r\n\r\n| Column         | Description                                                                         | Description |\r\n| -------------- | ----------------------------------------------------------------------------------- | ----------- |\r\n| State          | Name of the Indian state where the market is located                                | `province`  |\r\n| District       | Name of the district within the state where the market is located                   | `city`      |\r\n| Market         | Name of the specific market (mandi) where the commodity is traded                   | `string`    |\r\n| Commodity      | Name of the agricultural commodity being traded                                     | `string`    |\r\n| Variety        | Specific variety or type of the commodity                                           | `string`    |\r\n| Grade          | Quality grade of the commodity (e.g., FAQ, Medium, Good)                            | `string`    |\r\n| Arrival_Date   | The date of the price recording, in unambiguous ISO 8601 format (YYYY-MM-DD).       | `datetime`  |\r\n| Min_Price      | Minimum price of the commodity on the given date (in INR per quintal)               | `decimal`   |\r\n| Max_Price      | Maximum price of the commodity on the given date (in INR per quintal)               | `decimal`   |\r\n| Modal_Price    | Modal (most frequent) price of the commodity on the given date (in INR per quintal) | `decimal`   |\r\n| Commodity_Code | Unique code identifier for the commodity                                            | `numeric`   |\r\n\r\n---\r\n\r\nData sourced from the Government of India's Open Data Platform.\r\n\r\n**License:**\r\nGovernment Open Data License - India (GODL-India)\r\nhttps://www.data.gov.in/Godl\r\n",
                "keywords": ["Agriculture", "Economics", "Food", "Government", "India"],
                "licenses": [
                    {
                        "name": "other"
                    }
                ]
            }
            
            # Write the metadata to destination
            with open(dest_metadata, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Generated dataset-metadata.json with id '{kaggle_dataset}' at {dest_metadata}")
            
        except Exception as e:
            logger.error(f"Could not generate dataset-metadata.json: {e}")

    def upload_dataset(self) -> bool:
        """
        Upload the entire data directory to Kaggle

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(
                f"Starting upload of dataset to Kaggle: {self.dataset}")

            if not self.data_dir.exists():
                logger.error(f"Data directory does not exist: {self.data_dir}")
                return False

            # Ensure metadata file exists
            self._copy_metadata_file()
            
            # Verify metadata file exists in data directory
            metadata_path = self.data_dir / "dataset-metadata.json"
            if not metadata_path.exists():
                logger.error(f"Metadata file not found at {metadata_path} after copying")
                return False
            else:
                logger.info(f"Metadata file confirmed at {metadata_path}")

            # Create temporary upload directory excluding internal files
            upload_dir = self._prepare_upload_directory()
            if not upload_dir:
                logger.error("Failed to prepare upload directory")
                return False

            # Build upload command
            cmd = [
                'kaggle', 'datasets', 'version',
                '--path', '.',
                '--message', f'"Daily update - {self._get_current_timestamp()}"',
                '--dir-mode', 'zip'
            ]

            logger.info(f"Running upload command: {' '.join(cmd)}")

            # Run command and capture output for debugging
            result = subprocess.run(
                cmd,
                timeout=KAGGLE_UPLOAD_TIMEOUT,
                cwd=str(upload_dir),
                capture_output=True,
                text=True
            )

            # Log command output for debugging
            if result.stdout:
                logger.info(f"Kaggle CLI stdout: {result.stdout}")
            if result.stderr:
                logger.error(f"Kaggle CLI stderr: {result.stderr}")

            if result.returncode == 0:
                logger.info("Dataset uploaded successfully to Kaggle")
                return True
            else:
                logger.error(
                    f"Kaggle upload failed with return code: {result.returncode}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(
                f"Kaggle upload timed out after {KAGGLE_UPLOAD_TIMEOUT} seconds")
            return False
        except Exception as e:
            logger.error(f"Error uploading dataset: {e}")
            return False
        finally:
            # Clean up temporary upload directory if it exists
            if 'upload_dir' in locals() and upload_dir and upload_dir != self.data_dir:
                try:
                    shutil.rmtree(upload_dir)
                    logger.info(f"Cleaned up temporary upload directory: {upload_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary directory: {e}")

    def _prepare_upload_directory(self) -> Optional[Path]:
        """
        Prepare a temporary directory for upload, excluding internal files
        
        Returns:
            Path to upload directory or None if failed
        """
        try:
            # Files to exclude from upload (internal application files)
            exclude_files = {
                'state.json',  # Internal application state
                '.gitignore',
                '.DS_Store',
                'Thumbs.db'
            }
            
            # Create temporary directory
            temp_dir = tempfile.mkdtemp(prefix='kaggle_upload_')
            upload_path = Path(temp_dir)
            
            logger.info(f"Preparing upload directory at: {upload_path}")
            
            # Copy all files except excluded ones
            copied_files = 0
            for item in self.data_dir.rglob('*'):
                if item.is_file() and item.name not in exclude_files:
                    # Calculate relative path
                    rel_path = item.relative_to(self.data_dir)
                    dest_path = upload_path / rel_path
                    
                    # Create parent directories if needed
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Copy file
                    shutil.copy2(item, dest_path)
                    copied_files += 1
                    
            logger.info(f"Prepared {copied_files} files for upload (excluded: {exclude_files})")
            return upload_path
            
        except Exception as e:
            logger.error(f"Error preparing upload directory: {e}")
            return None

    def _get_current_timestamp(self) -> str:
        """Get current timestamp for upload message"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def check_kaggle_config(self) -> bool:
        """
        Check if Kaggle is properly configured

        Returns:
            True if configured, False otherwise
        """
        try:
            # Check if kaggle command is available
            result = subprocess.run(
                ['kaggle', '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode != 0:
                logger.error(
                    "Kaggle CLI is not properly installed or configured")
                return False

            logger.info(f"Kaggle CLI version: {result.stdout.strip()}")

            # Test authentication
            result = subprocess.run(
                ['kaggle', 'datasets', 'list', '--max-size', '1'],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                logger.error(f"Kaggle authentication failed: {result.stderr}")
                return False

            logger.info("Kaggle authentication successful")
            return True

        except subprocess.TimeoutExpired:
            logger.error("Kaggle configuration check timed out")
            return False
        except Exception as e:
            logger.error(f"Error checking Kaggle configuration: {e}")
            return False

    def get_dataset_info(self) -> Optional[dict]:
        """
        Get information about the Kaggle dataset

        Returns:
            Dictionary with dataset info or None if failed
        """
        try:
            cmd = ['kaggle', 'datasets', 'show', self.dataset]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                # Parse the output (basic parsing)
                info = {
                    'dataset': self.dataset,
                    'output': result.stdout,
                    'accessible': True
                }
                logger.info(f"Dataset info retrieved for: {self.dataset}")
                return info
            else:
                logger.error(f"Failed to get dataset info: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error getting dataset info: {e}")
            return None

    def _is_retryable_error(self, error_msg: str) -> bool:
        """
        Check if the error is retryable (network issues, temporary failures)
        
        Args:
            error_msg: Error message from Kaggle CLI
            
        Returns:
            True if error is retryable, False otherwise
        """
        retryable_errors = [
            'connection',
            'timeout',
            'network',
            'temporary',
            'rate limit',
            'server error',
            'service unavailable',
            'bad gateway',
            'gateway timeout'
        ]
        
        error_lower = error_msg.lower()
        return any(retryable in error_lower for retryable in retryable_errors)

    def _unzip_dataset(self, temp_path: Path) -> bool:
        """
        Manually unzip the downloaded dataset for better control
        
        Args:
            temp_path: Path to temporary directory containing zip file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Find the zip file
            zip_files = list(temp_path.glob("*.zip"))
            if not zip_files:
                logger.error("No zip file found in download directory")
                return False
            
            zip_file = zip_files[0]
            logger.info(f"Unzipping {zip_file.name} ({zip_file.stat().st_size / (1024**3):.1f} GB)")
            
            # Check if we have enough space for unzipped content (estimate 2x zip size)
            zip_size_gb = zip_file.stat().st_size / (1024**3)
            disk_usage = shutil.disk_usage(temp_path)
            available_gb = disk_usage.free / (1024**3)
            
            if available_gb < zip_size_gb * 2:
                logger.error(f"Insufficient space for unzip. Need ~{zip_size_gb * 2:.1f}GB, have {available_gb:.1f}GB")
                return False
            
            # Unzip with progress logging
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                total_files = len(zip_ref.infolist())
                logger.info(f"Extracting {total_files} files...")
                
                for i, member in enumerate(zip_ref.infolist()):
                    zip_ref.extract(member, temp_path)
                    if i % 100 == 0 or i == total_files - 1:
                        progress = (i + 1) / total_files * 100
                        logger.info(f"Extraction progress: {progress:.1f}% ({i + 1}/{total_files})")
            
            # Remove the zip file to save space
            zip_file.unlink()
            logger.info("Dataset unzipped successfully")
            return True
            
        except zipfile.BadZipFile:
            logger.error("Downloaded file is not a valid zip file")
            return False
        except Exception as e:
            logger.error(f"Error unzipping dataset: {e}")
            return False
