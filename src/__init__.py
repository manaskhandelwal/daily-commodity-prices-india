"""
Daily Commodity Prices India - Modular Components
A modular system for fetching, processing, and managing commodity price data
"""

from .config import validate_config
from .data_fetcher import DataFetcher
from .file_manager import FileManager
from .kaggle_integration import KaggleIntegration
from .state_manager import StateManager

__version__ = "2.0.0"
__author__ = "Daily Commodity Prices India Team"

__all__ = [
    'DataFetcher',
    'FileManager',
    'KaggleIntegration',
    'StateManager',
    'validate_config'
]
