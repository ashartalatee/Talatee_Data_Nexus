import os
import pandas as pd
from abc import ABC, abstractmethod
from core.logger import CustomLogger

class BaseConnector(ABC):
    """
    Abstract Base Class for all data connectors.
    Ensures that every connector has a standard interface to fetch and validate data.
    """

    def __init__(self, source_path: str):
        self.source_path = source_path
        self.logger = CustomLogger(name=self.__class__.__name__).get_logger()
        
    def _check_file_exists(self):
        """Internal helper to verify the source file exists."""
        if not os.path.exists(self.source_path):
            self.logger.error(f"File not found: {self.source_path}")
            raise FileNotFoundError(f"Source file not found at {self.source_path}")

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        Abstract method to read raw data from the source.
        Must be implemented by every child connector.
        Returns:
            pd.DataFrame: The raw data from the marketplace/file.
        """
        pass

    def validate_source(self) -> bool:
        """
        Basic validation to check if the source is accessible and not empty.
        Can be overridden by child classes for more complex validation.
        """
        try:
            self._check_file_exists()
            # Check if file size is greater than 0
            if os.path.getsize(self.source_path) == 0:
                self.logger.warning(f"File at {self.source_path} is empty.")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Validation failed for {self.source_path}: {str(e)}")
            return False