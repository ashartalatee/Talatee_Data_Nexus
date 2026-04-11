import pandas as pd
from modules.connectors.base_connector import BaseConnector

class ShopeeConnector(BaseConnector):
    """
    Connector specialized for Shopee marketplace data exports.
    Handles Excel file formats and initial data ingestion.
    """

    def __init__(self, source_path: str):
        # Inherit logger and source_path from BaseConnector
        super().__init__(source_path)

    def fetch_data(self) -> pd.DataFrame:
        """
        Reads Shopee sales data from an Excel file.
        Returns:
            pd.DataFrame: Raw data from Shopee.
        """
        self.logger.info(f"Starting Shopee data ingestion from: {self.source_path}")
        
        if not self.validate_source():
            raise ValueError(f"Invalid source file for Shopee: {self.source_path}")

        try:
            # Shopee exports are typically Excel files. 
            # We use engine='openpyxl' for .xlsx or default for .xls
            df = pd.read_excel(self.source_path)
            
            if df.empty:
                self.logger.warning("Shopee data source is empty.")
                return pd.DataFrame()

            self.logger.info(f"Successfully loaded {len(df)} rows from Shopee.")
            return df

        except Exception as e:
            self.logger.error(f"Failed to read Shopee Excel file: {str(e)}")
            raise e

    def validate_source(self) -> bool:
        """
        Extended validation to ensure the file is an Excel format.
        """
        base_valid = super().validate_source()
        if not base_valid:
            return False
            
        valid_extensions = ('.xlsx', '.xls', '.csv')
        if not self.source_path.lower().endswith(valid_extensions):
            self.logger.error(f"Unsupported file extension for Shopee: {self.source_path}")
            return False
            
        return True