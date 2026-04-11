import logging
import pandas as pd
from typing import Dict, Any

class MissingHandler:
    """
    Handles null and missing values based on client-specific configurations.
    Supports 'fill' with constants and 'drop' strategies.
    """
    def __init__(self, clean_cfg: Dict[str, Any], logger: logging.Logger):
        self.config = clean_cfg.get("handle_missing", {})
        self.logger = logger

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes missing value treatment based on the configured strategy.
        """
        if df is None or df.empty:
            return df

        strategy = self.config.get("strategy", "fill").lower()
        
        try:
            if strategy == "fill":
                return self._fill_missing(df)
            elif strategy == "drop":
                return self._drop_missing(df)
            else:
                self.logger.warning(f"Unknown missing handler strategy '{strategy}'. Defaulting to fill.")
                return self._fill_missing(df)
        except Exception as e:
            # Perbaikan: Log error yang lebih spesifik jika terjadi crash
            self.logger.error(f"Error during missing value handling: {str(e)}", exc_info=True)
            return df

    def _fill_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing values based on a column-to-value mapping or default types.
        """
        fill_map = self.config.get("columns", {})
        
        # Jika tidak ada mapping spesifik di JSON, gunakan fallback cerdas
        if not fill_map:
            # Menggunakan dtypes (plural) untuk iterasi kolom dan tipe datanya
            for col, dtype in df.dtypes.items():
                if pd.api.types.is_numeric_dtype(dtype):
                    df[col] = df[col].fillna(0)
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    # Jangan isi tanggal dengan 'Unknown', biarkan NaT atau drop nantinya
                    continue
                else:
                    df[col] = df[col].fillna("Unknown")
            return df

        self.logger.info(f"Filling missing values using specific mapping: {fill_map}")
        # fillna(value=...) sangat efisien untuk banyak kolom sekaligus
        return df.fillna(value=fill_map)

    def _drop_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drops rows where specific critical columns have null values.
        """
        target_cols = self.config.get("columns", [])
        
        # Jika 'columns' di JSON berupa dict (biasa dipakai di 'fill'), ambil key-nya saja
        if isinstance(target_cols, dict):
            target_cols = list(target_cols.keys())
            
        initial_count = len(df)
        
        if not target_cols:
            # Strategi pasif: Hanya buang baris yang benar-benar kosong semua kolomnya
            df = df.dropna(how='all')
        else:
            # Hanya periksa kolom yang benar-benar ada di DataFrame (menghindari KeyError)
            existing_targets = [c for c in target_cols if c in df.columns]
            if existing_targets:
                df = df.dropna(subset=existing_targets)
            else:
                self.logger.warning("Drop strategy failed: None of the target columns found in DataFrame.")

        dropped_count = initial_count - len(df)
        if dropped_count > 0:
            self.logger.info(f"MissingHandler: Dropped {dropped_count} rows due to missing critical data.")
            
        return df