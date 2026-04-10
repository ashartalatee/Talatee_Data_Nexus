import pandas as pd
import logging

class DuplicateHandler:
    """
    Handles duplicate row detection and removal based on client configuration.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def process(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """
        Removes duplicate rows based on specified columns or the entire row.
        """
        if not rules.get("enabled", False):
            self.logger.info("Duplicate removal is disabled. Skipping.")
            return df

        subset = rules.get("subset")
        keep = rules.get("keep", "first") # Options: 'first', 'last', False

        try:
            initial_count = len(df)
            
            # If subset is provided, validate that columns exist in the DataFrame
            valid_subset = None
            if subset and isinstance(subset, list):
                valid_subset = [col for col in subset if col in df.columns]
                if not valid_subset:
                    self.logger.warning("No valid columns found for duplicate subset. Checking entire row.")
                    valid_subset = None
            
            # Execute removal
            df_cleaned = df.drop_duplicates(subset=valid_subset, keep=keep)
            
            removed_count = initial_count - len(df_cleaned)
            self.logger.info(f"Duplicate check complete. Removed {removed_count} duplicate rows.")
            
            return df_cleaned

        except Exception as e:
            self.logger.error(f"Error while removing duplicates: {str(e)}")
            raise