import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

class ShopeeLoader:
    """
    Handles data ingestion from Shopee marketplace.
    Supports local CSV exports and simulated API connectivity.
    """
    def __init__(self, source_config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = source_config
        self.base_dir = base_dir
        self.logger = logger
        self.platform = "shopee"

    def load(self) -> Optional[pd.DataFrame]:
        """
        Main entry point for loading Shopee data based on format.
        """
        # Menentukan apakah load lewat CSV atau API
        # Jika ada 'file_path' di config, otomatis gunakan CSV
        if "file_path" in self.config:
            format_type = "csv"
        else:
            format_type = self.config.get("format", "api").lower()
        
        try:
            if format_type == "csv":
                return self._load_from_csv()
            elif format_type == "api":
                return self._load_from_api()
            else:
                self.logger.error(f"Unsupported format '{format_type}' for Shopee loader.")
                return None
        except Exception as e:
            self.logger.error(f"ShopeeLoader failed during {format_type} ingestion: {str(e)}")
            return None

    def _load_from_csv(self) -> Optional[pd.DataFrame]:
        """
        Loads and performs initial structural normalization on Shopee CSV exports.
        """
        # SINKRONISASI KEY: Menggunakan 'file_path' sesuai JSON client
        relative_path = self.config.get("file_path") or self.config.get("path")
        
        if not relative_path:
            self.logger.error("No path or file_path provided in Shopee configuration.")
            return None

        # Gabungkan path dengan benar
        file_path = (self.base_dir / relative_path).resolve()
        encoding = self.config.get("encoding", "utf-8")
        
        # Cek apakah path benar-benar file, bukan folder (mencegah Errno 13)
        if file_path.is_dir():
            self.logger.error(f"Path is a directory, not a file: {file_path}")
            return None

        if not file_path.exists():
            self.logger.error(f"Shopee CSV file not found: {file_path}")
            return None

        self.logger.info(f"Reading Shopee CSV from {file_path}")
        
        try:
            # Menggunakan engine='python' untuk kompatibilitas karakter khusus Shopee
            df = pd.read_csv(file_path, encoding=encoding, engine='python')
            
            if df.empty:
                self.logger.warning(f"Shopee CSV at {file_path} is empty.")
                return df

            return df

        except Exception as e:
            self.logger.error(f"Failed to parse Shopee CSV: {str(e)}")
            return None

    def _load_from_api(self) -> Optional[pd.DataFrame]:
        """
        Simulates Shopee Open Platform API integration.
        """
        endpoint = self.config.get("endpoint")
        auth_key = self.config.get("auth_key")
        
        self.logger.info(f"Initiating Shopee API request to {endpoint}")
        
        if not auth_key:
            self.logger.error("Shopee API auth_key is missing in configuration.")
            return None

        try:
            # Placeholder untuk integrasi requests ke API Shopee
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Shopee API connection error: {str(e)}")
            return None