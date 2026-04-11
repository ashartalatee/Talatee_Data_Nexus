import pandas as pd
from modules.connectors.base_connector import BaseConnector

class TokopediaConnector(BaseConnector):
    """
    Connector specialized for Tokopedia marketplace data exports.
    Primarily handles CSV formats with specific encoding often found in Indo marketplaces.
    """

    def __init__(self, source_path: str):
        # Inherit logger and source_path from BaseConnector
        super().__init__(source_path)

    def fetch_data(self) -> pd.DataFrame:
        """
        Reads Tokopedia sales data from a CSV file.
        Returns:
            pd.DataFrame: Raw data from Tokopedia.
        """
        self.logger.info(f"Starting Tokopedia data ingestion from: {self.source_path}")
        
        if not self.validate_source():
            raise ValueError(f"Invalid source file for Tokopedia: {self.source_path}")

        try:
            # Tokopedia CSVs often use specific encodings or delimiters.
            # Using low_memory=False for large datasets to ensure type stability.
            df = pd.read_csv(self.source_path, encoding='utf-8', sep=',')
            
            if df.empty:
                self.logger.warning("Tokopedia data source is empty.")
                return pd.DataFrame()

            self.logger.info(f"Successfully loaded {len(df)} rows from Tokopedia.")
            return df

        except UnicodeDecodeError:
            # Fallback for files with different encoding (common in older Excel-generated CSVs)
            self.logger.warning("UTF-8 decoding failed, trying ISO-8859-1")
            return pd.read_csv(self.source_path, encoding='ISO-8859-1', sep=',')
        except Exception as e:
            self.logger.error(f"Failed to read Tokopedia CSV file: {str(e)}")
            raise e

    def validate_source(self) -> bool:
        """
        Validation to ensure the file exists and is a CSV.
        """
        base_valid = super().validate_source()
        if not base_valid:
            return False
            
        if not self.source_path.lower().endswith('.csv'):
            self.logger.error(f"Tokopedia connector expects a CSV file: {self.source_path}")
            return False
            
        return True