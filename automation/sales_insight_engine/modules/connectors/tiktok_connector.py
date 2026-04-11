import pandas as pd
from modules.connectors.base_connector import BaseConnector

class TikTokConnector(BaseConnector):
    """
    Connector specialized for TikTok Shop data exports.
    TikTok typically exports data in CSV format with specific column structures.
    """

    def __init__(self, source_path: str):
        super().__init__(source_path)

    def fetch_data(self) -> pd.DataFrame:
        """
        Reads TikTok Shop sales data from a CSV file.
        Returns:
            pd.DataFrame: Raw data from TikTok.
        """
        self.logger.info(f"Starting TikTok Shop data ingestion from: {self.source_path}")
        
        if not self.validate_source():
            raise ValueError(f"Invalid source file for TikTok: {self.source_path}")

        try:
            # TikTok Shop CSVs often use UTF-8. 
            # We use low_memory=False to ensure stable data type inference.
            df = pd.read_csv(self.source_path, encoding='utf-8')
            
            if df.empty:
                self.logger.warning("TikTok Shop data source is empty.")
                return pd.DataFrame()

            self.logger.info(f"Successfully loaded {len(df)} rows from TikTok.")
            return df

        except Exception as e:
            self.logger.error(f"Failed to read TikTok CSV file: {str(e)}")
            raise e

    def validate_source(self) -> bool:
        """
        Validation to ensure the file is a CSV.
        """
        base_valid = super().validate_source()
        if not base_valid:
            return False
            
        if not self.source_path.lower().endswith('.csv'):
            self.logger.error(f"TikTok connector expects a CSV file: {self.source_path}")
            return False
            
        return True