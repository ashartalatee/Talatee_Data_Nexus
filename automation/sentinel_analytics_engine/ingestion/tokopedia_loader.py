import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

class TokopediaLoader:
    """
    Handles data ingestion from Tokopedia marketplace.
    Supports local CSV exports and Tokopedia Open API connectivity.
    """
    def __init__(self, source_config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = source_config
        self.base_dir = base_dir
        self.logger = logger
        self.platform = "tokopedia"

    def load(self) -> Optional[pd.DataFrame]:
        """
        Main entry point for loading Tokopedia data based on format.
        """
        # Deteksi otomatis: Jika ada file_path, gunakan CSV.
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
                self.logger.error(f"Unsupported format '{format_type}' for Tokopedia loader.")
                return None
        except Exception as e:
            self.logger.error(f"TokopediaLoader failed during {format_type} ingestion: {str(e)}")
            return None

    def _load_from_csv(self) -> Optional[pd.DataFrame]:
        """
        Loads and performs initial structural normalization on Tokopedia CSV exports.
        """
        # SINKRONISASI KEY: Mengambil dari 'file_path' (sesuai JSON) atau 'path' (fallback)
        relative_path = self.config.get("file_path") or self.config.get("path")
        
        if not relative_path:
            self.logger.error("No path or file_path provided in Tokopedia configuration.")
            return None

        # Resolve path untuk memastikan file benar-benar ada
        file_path = (self.base_dir / relative_path).resolve()
        encoding = self.config.get("encoding", "utf-8")
        
        # Guard: Mencegah Errno 13 (membuka folder sebagai file)
        if file_path.is_dir():
            self.logger.error(f"Path is a directory, not a file: {file_path}")
            return None

        if not file_path.exists():
            self.logger.error(f"Tokopedia CSV file not found: {file_path}")
            return None

        self.logger.info(f"Reading Tokopedia CSV from {file_path}")
        
        try:
            # Menggunakan engine='python' dan sep=None untuk deteksi delimiter otomatis
            # Tokopedia kadang menggunakan koma (,) atau semicolon (;) tergantung regional setting
            df = pd.read_csv(file_path, encoding=encoding, sep=None, engine='python')
            
            if df.empty:
                self.logger.warning(f"Tokopedia CSV at {file_path} is empty.")
                return df

            return df

        except Exception as e:
            self.logger.error(f"Failed to parse Tokopedia CSV: {str(e)}")
            return None

    def _load_from_api(self) -> Optional[pd.DataFrame]:
        """
        Simulates Tokopedia API integration (OAuth2 placeholder).
        """
        endpoint = self.config.get("endpoint")
        auth_key = self.config.get("auth_key")
        
        self.logger.info(f"Initiating Tokopedia API request to {endpoint}")
        
        if not auth_key:
            self.logger.error("Tokopedia API auth_key (Client ID/Secret) is missing.")
            return None

        try:
            # Placeholder untuk logic requests.post (OAuth2)
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Tokopedia API connection error: {str(e)}")
            return None