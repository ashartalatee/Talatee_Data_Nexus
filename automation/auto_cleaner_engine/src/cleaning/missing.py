import pandas as pd
import logging

class MissingHandler:
    """
    Handles missing values based on client-specific configurations.
    Supports drop and constant fill strategies.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def process(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """
        Orchestrates missing value handling based on the config rules.
        """
        if not rules.get("enabled", False):
            self.logger.info("Missing value handling is disabled. Skipping.")
            return df

        strategy = rules.get("strategy", "drop")
        target_columns = rules.get("target_columns", [])
        fill_value = rules.get("fill_value", None)

        try:
            if strategy == "drop":
                return self._handle_drop(df, target_columns)
            elif strategy == "constant":
                return self._handle_constant(df, target_columns, fill_value)
            else:
                self.logger.warning(f"Strategy '{strategy}' not recognized. Defaulting to no-op.")
                return df
        except Exception as e:
            self.logger.error(f"Error handling missing values: {str(e)}")
            raise

    def _handle_drop(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Removes rows where specific columns have null values.
        """
        if not columns:
            self.logger.info("Dropping rows with ANY missing values.")
            return df.dropna()
        
        # Ensure only columns existing in df are passed to dropna
        valid_cols = [c for c in columns if c in df.columns]
        self.logger.info(f"Dropping rows with missing values in: {valid_cols}")
        return df.dropna(subset=valid_cols)

    def _handle_constant(self, df: pd.DataFrame, columns: list, value: any) -> pd.DataFrame:
        """
        Fills missing values with a constant value.
        """
        if value is None:
            self.logger.warning("Constant strategy selected but no fill_value provided.")
            return df

        if not columns:
            self.logger.info(f"Filling all missing values with: {value}")
            return df.fillna(value)

        valid_cols = [c for c in columns if c in df.columns]
        self.logger.info(f"Filling missing values in {valid_cols} with: {value}")
        
        df_copy = df.copy()
        for col in valid_cols:
            df_copy[col] = df_copy[col].fillna(value)
            
        return df_copy