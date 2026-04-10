import pandas as pd
import os
import logging

class DataLoader:
    """
    Handles data ingestion for various file formats.
    Ensures data is loaded into a pandas DataFrame for processing.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.supported_formats = {
            '.csv': self._load_csv,
            '.xlsx': self._load_excel,
            '.xls': self._load_excel
        }

    def load_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Generic entry point to load a file based on its extension.
        """
        if not os.path.exists(file_path):
            self.logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"Input file not found at {file_path}")

        file_extension = os.path.splitext(file_path)[1].lower()
        
        if file_extension not in self.supported_formats:
            self.logger.error(f"Unsupported file format: {file_extension}")
            raise ValueError(f"Extension {file_extension} is not supported.")

        try:
            self.logger.info(f"Ingesting file: {os.path.basename(file_path)}")
            df = self.supported_formats[file_extension](file_path, **kwargs)
            self.logger.info(f"Successfully loaded {len(df)} rows.")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load file {file_path}: {str(e)}")
            raise

    def _load_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Internal method to load CSV files.
        """
        # Default to utf-8 if encoding not provided in kwargs
        encoding = kwargs.get('encoding', 'utf-8')
        return pd.read_csv(file_path, encoding=encoding)

    def _load_excel(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Internal method to load Excel files.
        """
        # Uses openpyxl as engine for .xlsx
        return pd.read_excel(file_path, engine='openpyxl')