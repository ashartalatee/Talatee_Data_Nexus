import pandas as pd
import logging

class SchemaValidator:
    """
    Validates the DataFrame against the rules defined in schema.json.
    Ensures data integrity before and after processing.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def validate(self, df: pd.DataFrame, schema_config: dict) -> bool:
        """
        Runs comprehensive validation checks.
        Returns True if valid, raises ValueError if critical checks fail.
        """
        self.logger.info("Starting schema validation...")
        
        try:
            self._check_required_columns(df, schema_config.get("required_columns", []))
            self._check_data_types(df, schema_config.get("data_types", {}))
            return True
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            raise ValueError(f"Data Validation Error: {str(e)}")

    def _check_required_columns(self, df: pd.DataFrame, required_cols: list):
        """
        Verifies that all mandatory columns exist in the DataFrame.
        """
        if not required_cols:
            return

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        self.logger.info("All required columns are present.")

    def _check_data_types(self, df: pd.DataFrame, type_mapping: dict):
        """
        Validates (or attempts to cast) columns to expected data types.
        Note: This is a soft validation. It logs warnings if types don't match.
        """
        if not type_mapping:
            return

        for col, expected_type in type_mapping.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                
                # Check for basic type compatibility
                if expected_type in ['int64', 'float64'] and 'object' in actual_type:
                    self.logger.warning(f"Type mismatch for '{col}': Expected {expected_type}, got {actual_type}. Data might need numeric cleaning.")
                elif 'datetime' in expected_type and 'datetime' not in actual_type:
                    self.logger.warning(f"Type mismatch for '{col}': Expected {expected_type}. Currently stored as {actual_type}.")
            
        self.logger.info("Data type consistency check completed.")