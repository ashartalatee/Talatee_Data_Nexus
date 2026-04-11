import os
import pandas as pd
from core.logger import CustomLogger

class CSVExporter:
    """
    Exporter responsible for generating standardized CSV reports.
    Focuses on data portability and easy integration with BI tools.
    """

    def __init__(self, clean_df: pd.DataFrame, metrics: dict, insights: list, client_config: dict):
        self.df = clean_df
        self.metrics = metrics
        self.insights = insights
        self.client_config = client_config
        self.logger = CustomLogger(name="CSVExporter").get_logger()
        
        # Determine output path
        self.output_dir = "data/reports"
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Creates the report directory if it doesn't exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def export(self) -> str:
        """
        Generates standardized CSV files. 
        Main report is the cleaned transactional data, 
        with metrics exported as sidecar metadata files.
        Returns:
            str: Path to the main generated CSV report.
        """
        client_id = self.client_config.get("client_id", "Unknown")
        prefix = self.client_config.get("output_settings", {}).get("filename_prefix", "Sales_Report")
        
        # Main file path
        base_filename = f"{prefix}_{client_id}"
        main_filepath = os.path.join(self.output_dir, f"{base_filename}_data.csv")
        metrics_filepath = os.path.join(self.output_dir, f"{base_filename}_metrics.csv")

        self.logger.info(f"Generating CSV reports for Client: {client_id}")

        try:
            # 1. Export Main Cleaned Transactional Data
            self.df.to_csv(main_filepath, index=False, encoding='utf-8')
            
            # 2. Export Metrics Summary as a flat CSV
            # We flatten the 'overall' metrics for simple CSV structure
            overall = self.metrics.get("overall", {})
            metrics_df = pd.DataFrame([overall])
            
            # Append insights as a column in the metrics CSV for portability
            metrics_df['narrative_insights'] = " | ".join(self.insights)
            
            metrics_df.to_csv(metrics_filepath, index=False, encoding='utf-8')

            self.logger.info(f"CSV exports completed: {main_filepath} and {metrics_filepath}")
            return main_filepath

        except Exception as e:
            self.logger.error(f"Failed to export CSV report: {str(e)}")
            raise e