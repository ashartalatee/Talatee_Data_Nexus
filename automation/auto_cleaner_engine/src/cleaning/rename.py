import pandas as pd
import logging

class RenameModule:
    """
    Handles column renaming based on client-specific schema mappings.
    Ensures the internal engine works with standardized column names 
    regardless of the messy input headers.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def process(self, df: pd.DataFrame, schema_config: dict) -> pd.DataFrame:
        """
        Renames columns based on the 'column_mapping' found in schema.json.
        """
        mapping = schema_config.get("column_mapping", {})
        
        if not mapping:
            self.logger.warning("No column mapping provided in schema. Skipping rename.")
            return df

        try:
            # Identify which columns in the mapping actually exist in the input DF
            existing_mapping = {k: v for k, v in mapping.items() if k in df.columns}
            missing_cols = set(mapping.keys()) - set(df.columns)

            if missing_cols:
                self.logger.warning(f"Columns defined in mapping not found in input data: {missing_cols}")

            if not existing_mapping:
                self.logger.error("None of the mapped columns were found in the input file.")
                return df

            self.logger.info(f"Renaming {len(existing_mapping)} columns to standardized schema.")
            
            # Perform rename
            df_renamed = df.rename(columns=existing_mapping)
            
            # Optional: Filter to keep only the mapped columns if required by client logic
            # For this engine, we keep all but prioritize standardized names for cleaning modules
            return df_renamed

        except Exception as e:
            self.logger.error(f"Error during column renaming: {str(e)}")
            raise