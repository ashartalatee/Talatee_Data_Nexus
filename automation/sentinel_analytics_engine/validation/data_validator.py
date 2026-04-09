import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional

class DataValidator:
    """
    Performs deep data integrity checks including range validation, 
    logical consistency, and business rule enforcement.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.validation_cfg = config.get("validation", {})
        self.strict_mode = config.get("strict_mode", True)

    def validate_content(self, df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
        """
        Executes business-level validation rules on the dataset.
        Returns (is_passed, processed_dataframe).
        """
        if df is None or df.empty:
            return False, df

        try:
            initial_row_count = len(df)
            
            # 1. Non-Negative Value Enforcement
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col in ['quantity', 'unit_price', 'total_price']:
                    invalid_mask = df[col] < 0
                    if invalid_mask.any():
                        self.logger.warning(f"Detected negative values in {col}. Adjusting to 0.")
                        df.loc[invalid_mask, col] = 0

            # 2. Logical Consistency: total_price == quantity * unit_price
            # We allow a small epsilon for float precision issues
            if all(col in df.columns for col in ['quantity', 'unit_price', 'total_price']):
                expected_total = df['quantity'] * df['unit_price']
                mismatch_mask = (df['total_price'] - expected_total).abs() > 0.01
                
                if mismatch_mask.any():
                    self.logger.info(f"Correcting {mismatch_mask.sum()} price calculation mismatches.")
                    df.loc[mismatch_mask, 'total_price'] = expected_total[mismatch_mask]

            # 3. Date Range Sanity Check
            if 'transaction_date' in df.columns:
                future_dates = df['transaction_date'] > pd.Timestamp.now()
                if future_dates.any():
                    self.logger.error(f"Detected {future_dates.sum()} future transaction dates.")
                    if self.strict_mode:
                        df = df[~future_dates].copy()

            # 4. SKU Presence
            if 'sku' in df.columns:
                missing_sku = df['sku'].isna() | (df['sku'].astype(str).str.strip() == "")
                if missing_sku.any():
                    self.logger.warning(f"Found {missing_sku.sum()} records with missing SKU.")
                    if self.strict_mode:
                        df = df[~missing_sku].copy()

            final_row_count = len(df)
            drop_count = initial_row_count - final_row_count
            
            if drop_count > 0:
                self.logger.info(f"Data validation complete. Dropped {drop_count} invalid records.")
            
            return True, df

        except Exception as e:
            self.logger.critical(f"Critical error in DataValidator: {str(e)}", exc_info=True)
            return False, df