import pandas as pd
from modules.connectors.base_connector import BaseConnector

class ExcelConnector(BaseConnector):
    """
    Generic Excel Connector for manual reports, WhatsApp logs, or custom files.
    Supports both .xlsx and .xls formats.
    """

    def __init__(self, source_path: str):
        # Inherit logger and source_path from BaseConnector
        super().__init__(source_path)

    def fetch_data(self) -> pd.DataFrame:
        """
        Reads generic sales data from an Excel file.
        Returns:
            pd.DataFrame: Raw data.
        """
        self.logger.info(f"Accessing Excel source: {self.source_path}")
        
        if not self.validate_source():
            raise ValueError(f"Invalid or unsupported Excel file: {self.source_path}")

        try:
            # Using engine='openpyxl' for better compatibility with modern .xlsx
            # We read the first sheet by default as it's the industry standard for reports
            df = pd.read_excel(self.source_path, sheet_name=0)
            
            if df.empty:
                self.logger.warning(f"Excel file {self.source_path} contains no data.")
                return pd.DataFrame()

            # Remove potential empty rows/columns that often exist in manual Excel files
            df = df.dropna(how='all').dropna(axis=1, how='all')

            self.logger.info(f"Successfully ingested {len(df)} rows from Excel.")
            return df

        except Exception as e:
            self.logger.error(f"Error reading Excel file {self.source_path}: {str(e)}")
            raise e

    def validate_source(self) -> bool:
        """
        Validates if the file is a valid Excel format.
        """
        base_valid = super().validate_source()
        if not base_valid:
            return False
            
        valid_extensions = ('.xlsx', '.xls', '.xlsm')
        if not self.source_path.lower().endswith(valid_extensions):
            self.logger.error(f"File extension not supported for ExcelConnector: {self.source_path}")
            return False
            
        return True