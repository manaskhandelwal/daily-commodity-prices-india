"""
State manager module for handling application state
Manages state persistence, processed dates, and data hashes
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .config import STATE_FILE, DATA_DIR

logger = logging.getLogger(__name__)


class StateManager:
    """Handles application state persistence"""

    def __init__(self):
        self.state_file = STATE_FILE
        self.data_dir = DATA_DIR
        self._ensure_data_dir()

    def _ensure_data_dir(self):
        """Ensure data directory exists"""
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> Dict:
        """
        Load application state from file

        Returns:
            Dictionary with state data
        """
        default_state = {
            'last_update': None,
            'last_data_hash': None,
            'processed_dates': [],
            'total_records_processed': 0,
            'initialization_date': None,
            'last_successful_upload': None
        }

        if not self.state_file.exists():
            logger.info("State file does not exist, using default state")
            return default_state

        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)

            # Ensure all required keys exist
            for key, default_value in default_state.items():
                if key not in state:
                    state[key] = default_value

            logger.info(f"Loaded state from {self.state_file}")
            return state

        except Exception as e:
            logger.error(f"Error loading state file: {e}")
            logger.info("Using default state")
            return default_state

    def save_state(self, state: Dict) -> bool:
        """
        Save application state to file

        Args:
            state: State dictionary to save

        Returns:
            True if successful, False otherwise
        """
        try:
            # Add timestamp
            state['last_update'] = datetime.now().isoformat()

            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)

            logger.info(f"Saved state to {self.state_file}")
            return True

        except Exception as e:
            logger.error(f"Error saving state file: {e}")
            return False

    def update_processed_dates(self, new_dates: List[str]) -> bool:
        """
        Update the list of processed dates

        Args:
            new_dates: List of new dates to add

        Returns:
            True if successful, False otherwise
        """
        try:
            state = self.load_state()

            # Add new dates to processed dates
            processed_dates = set(state.get('processed_dates', []))
            processed_dates.update(new_dates)

            # Keep only recent dates (last 30 days for efficiency)
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=30)
            cutoff_str = cutoff_date.strftime('%Y-%m-%d')

            processed_dates = [
                date for date in processed_dates if date >= cutoff_str]

            state['processed_dates'] = sorted(list(processed_dates))

            return self.save_state(state)

        except Exception as e:
            logger.error(f"Error updating processed dates: {e}")
            return False

    def update_data_hash(self, new_hash: str) -> bool:
        """
        Update the last data hash

        Args:
            new_hash: New data hash

        Returns:
            True if successful, False otherwise
        """
        try:
            state = self.load_state()
            state['last_data_hash'] = new_hash
            return self.save_state(state)

        except Exception as e:
            logger.error(f"Error updating data hash: {e}")
            return False

    def increment_records_processed(self, count: int) -> bool:
        """
        Increment the total records processed counter

        Args:
            count: Number of records to add

        Returns:
            True if successful, False otherwise
        """
        try:
            state = self.load_state()
            state['total_records_processed'] = state.get(
                'total_records_processed', 0) + count
            return self.save_state(state)

        except Exception as e:
            logger.error(f"Error incrementing records processed: {e}")
            return False

    def mark_initialization_complete(self) -> bool:
        """
        Mark the initialization as complete

        Returns:
            True if successful, False otherwise
        """
        try:
            state = self.load_state()
            state['initialization_date'] = datetime.now().isoformat()
            return self.save_state(state)

        except Exception as e:
            logger.error(f"Error marking initialization complete: {e}")
            return False

    def mark_successful_upload(self) -> bool:
        """
        Mark a successful upload to Kaggle

        Returns:
            True if successful, False otherwise
        """
        try:
            state = self.load_state()
            state['last_successful_upload'] = datetime.now().isoformat()
            return self.save_state(state)

        except Exception as e:
            logger.error(f"Error marking successful upload: {e}")
            return False

    def get_state_summary(self) -> Dict:
        """
        Get a summary of the current state

        Returns:
            Dictionary with state summary
        """
        state = self.load_state()

        summary = {
            'initialized': state.get('initialization_date') is not None,
            'last_update': state.get('last_update'),
            'total_records': state.get('total_records_processed', 0),
            'processed_dates_count': len(state.get('processed_dates', [])),
            'last_upload': state.get('last_successful_upload'),
            'has_data_hash': state.get('last_data_hash') is not None
        }

        return summary

    def reset_state(self) -> bool:
        """
        Reset the state to default values

        Returns:
            True if successful, False otherwise
        """
        try:
            if self.state_file.exists():
                self.state_file.unlink()
                logger.info("State file deleted")

            # Create new default state
            default_state = {
                'last_update': None,
                'last_data_hash': None,
                'processed_dates': [],
                'total_records_processed': 0,
                'initialization_date': None,
                'last_successful_upload': None
            }

            return self.save_state(default_state)

        except Exception as e:
            logger.error(f"Error resetting state: {e}")
            return False
