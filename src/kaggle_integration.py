"""
Kaggle integration module for upload and download operations
Handles Kaggle dataset downloads and uploads with proper error handling
"""

import json
import os
import shutil
import tempfile
import subprocess
import logging
from pathlib import Path
from typing import Optional

from .config import (
    KAGGLE_USERNAME, KAGGLE_KEY, KAGGLE_DATASET,
    KAGGLE_DOWNLOAD_TIMEOUT, KAGGLE_UPLOAD_TIMEOUT,
    DATA_DIR, METADATA_FILE
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
        Download the complete dataset from Kaggle

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting download of Kaggle dataset: {self.dataset}")

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Download dataset to temporary directory
                cmd = [
                    'kaggle', 'datasets', 'download',
                    self.dataset,
                    '--path', str(temp_path),
                    '--unzip'
                ]

                logger.info(f"Running command: {' '.join(cmd)}")

                result = subprocess.run(
                    cmd,
                    timeout=KAGGLE_DOWNLOAD_TIMEOUT,
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    logger.error(f"Kaggle download failed: {result.stderr}")
                    return False

                logger.info("Dataset downloaded successfully")
                logger.info(f"Download output: {result.stdout}")

                # Copy contents to data directory
                return self._copy_downloaded_data(temp_path)

        except subprocess.TimeoutExpired:
            logger.error(
                f"Kaggle download timed out after {KAGGLE_DOWNLOAD_TIMEOUT} seconds")
            return False
        except Exception as e:
            logger.error(f"Error downloading dataset: {e}")
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
        """Copy dataset-metadata.json from script directory to data directory with dynamic id"""
        try:
            # Get the script's parent directory
            script_dir = Path(__file__).parent.parent
            source_metadata = script_dir / "dataset-metadata.json"
            dest_metadata = self.data_dir / "dataset-metadata.json"

            if source_metadata.exists():
                # Read the metadata file
                with open(source_metadata, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                # Update the id field with KAGGLE_DATASET environment variable
                kaggle_dataset = os.getenv('KAGGLE_DATASET')
                if kaggle_dataset:
                    metadata['id'] = kaggle_dataset
                    logger.info(f"Updated dataset id to: {kaggle_dataset}")
                else:
                    logger.warning("KAGGLE_DATASET environment variable not set, using default id")
                
                # Write the updated metadata to destination
                with open(dest_metadata, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Copied and updated dataset-metadata.json to {dest_metadata}")
            else:
                logger.warning(
                    f"dataset-metadata.json not found at {source_metadata}")
        except Exception as e:
            logger.warning(f"Could not copy dataset-metadata.json: {e}")

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
                cwd=str(self.data_dir),
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
