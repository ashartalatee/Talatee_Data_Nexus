import logging
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class SchemaValidator:
    """
    Ensures the ingested data conforms to the standardized Talatee Sentinel schema.
    Acts as a strict data contract between ingestion and processing layers.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.validation_cfg = config.get("validation", {})
        self.strict_mode = config.get("strict_mode", True)

    def validate(self, df: pd.DataFrame) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        Executes schema enforcement, column presence checks, and type casting.
        Returns (is_valid, validated_dataframe).
        """
        if df is None or df.empty:
            self.logger.error("Validation failed: Input DataFrame is None or empty.")
            return False, None

        try:
            # 1. Check Required Columns
            required_cols = self.validation_cfg.get("required_columns", [])
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                self.logger.error(f"Schema Mismatch. Missing required columns: {missing_cols}")
                if self.strict_mode:
                    return False, df

            # 2. Enforce Data Types
            type_mapping = self.validation_cfg.get("data_types", {})
            for col, dtype in type_mapping.items():
                if col in df.columns:
                    try:
                        if "datetime" in str(dtype):
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        else:
                            df[col] = df[col].astype(dtype)
                    except Exception as e:
                        self.logger.warning(f"Type conversion failed for column '{col}' to {dtype}: {e}")

            # 3. Handle Critical Nulls in Required Columns
            if self.strict_mode:
                critical_cols = [c for c in required_cols if c in df.columns]
                null_counts = df[critical_cols].isnull().sum().sum()
                if null_counts > 0:
                    self.logger.warning(f"Detected {null_counts} null values in required columns.")

            self.logger.info("Schema validation step completed.")
            return True, df

        except Exception as e:
            self.logger.critical(f"Critical error during schema validation: {str(e)}", exc_info=True)
            return False, df