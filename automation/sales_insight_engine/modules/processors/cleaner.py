import pandas as pd
import numpy as np
from core.logger import CustomLogger
from core.schema import DataSchema

class DataCleaner:
    """
    Processor responsible for cleaning and ensuring data integrity.
    Handles missing values, duplicates, and type enforcement.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.logger = CustomLogger(name="DataCleaner").get_logger()
        self.schema = DataSchema()

    def process(self) -> pd.DataFrame:
        """
        Executes the full cleaning pipeline.
        """
        if self.df.empty:
            self.logger.warning("DataCleaner received an empty DataFrame.")
            return self.df

        initial_rows = len(self.df)
        self.logger.info(f"Starting data cleaning for {initial_rows} rows.")

        self._handle_missing_values()
        self._remove_duplicates()
        self._enforce_data_types()
        self._sanitize_strings()

        final_rows = len(self.df)
        self.logger.info(f"Cleaning completed. {final_rows} rows remaining ({initial_rows - final_rows} removed).")
        
        return self.df

    def _handle_missing_values(self):
        """
        Handles null values based on column importance.
        Critical columns result in row removal, others get defaults.
        """
        # Critical columns: If these are null, the record is useless for analytics
        critical_cols = [
            self.schema.ORDER_ID, 
            self.schema.DATE, 
            self.schema.PRICE
        ]
        self.df.dropna(subset=critical_cols, inplace=True)

        # Non-critical columns: Fill with placeholders
        if self.schema.CATEGORY in self.df.columns:
            self.df[self.schema.CATEGORY] = self.df[self.schema.CATEGORY].fillna("Uncategorized")
        
        if self.schema.CUSTOMER_ID in self.df.columns:
            self.df[self.schema.CUSTOMER_ID] = self.df[self.schema.CUSTOMER_ID].fillna("Guest")

        # Quantity must be at least 1
        self.df[self.schema.QUANTITY] = self.df[self.schema.QUANTITY].fillna(1)

    def _remove_duplicates(self):
        """Removes duplicate transactions based on order_id and product name."""
        subset_cols = [self.schema.ORDER_ID, self.schema.PRODUCT]
        self.df.drop_duplicates(subset=subset_cols, keep='first', inplace=True)

    def _enforce_data_types(self):
        """Standardizes data types using the central schema definition."""
        dtype_map = self.schema.get_dtype_map()
        
        for col, dtype in dtype_map.items():
            if col in self.df.columns:
                try:
                    if dtype == 'datetime64[ns]':
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    else:
                        self.df[col] = self.df[col].astype(dtype)
                except Exception as e:
                    self.logger.error(f"Failed to convert column {col} to {dtype}: {str(e)}")

        # Drop rows where date conversion failed
        self.df.dropna(subset=[self.schema.DATE], inplace=True)

    def _sanitize_strings(self):
        """Trims whitespace and standardizes case for categorical data."""
        string_cols = [self.schema.PRODUCT, self.schema.CATEGORY, self.schema.CHANNEL]
        for col in string_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.strip().str.title()