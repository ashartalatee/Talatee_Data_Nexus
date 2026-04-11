import os
import pandas as pd
from core.logger import CustomLogger

class ExcelExporter:
    """
    Exporter responsible for generating professional Multi-Sheet Excel reports.
    Includes cleaned data, calculated metrics, and business insights.
    """

    def __init__(self, clean_df: pd.DataFrame, metrics: dict, insights: list, client_config: dict):
        self.df = clean_df
        self.metrics = metrics
        self.insights = insights
        self.client_config = client_config
        self.logger = CustomLogger(name="ExcelExporter").get_logger()
        
        # Determine output path
        self.output_dir = "data/reports"
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Creates the report directory if it doesn't exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def export(self) -> str:
        """
        Generates a formatted Excel file with multiple sheets.
        Returns:
            str: Path to the generated report.
        """
        client_id = self.client_config.get("client_id", "Unknown")
        prefix = self.client_config.get("output_settings", {}).get("filename_prefix", "Sales_Report")
        filename = f"{prefix}_{client_id}.xlsx"
        filepath = os.path.join(self.output_dir, filename)

        self.logger.info(f"Generating Excel report: {filepath}")

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Sheet 1: Executive Summary (Insights)
                insights_df = pd.DataFrame({"Business Insights & Recommendations": self.insights})
                insights_df.to_excel(writer, sheet_name='Summary', index=False)

                # Sheet 2: Overall KPIs
                overall_metrics = pd.DataFrame([self.metrics.get("overall", {})])
                overall_metrics.to_excel(writer, sheet_name='KPIs', index=False)

                # Sheet 3: Channel Performance
                if "by_channel" in self.metrics:
                    self.metrics["by_channel"].to_excel(writer, sheet_name='By_Channel')

                # Sheet 4: Category Performance
                if "by_category" in self.metrics:
                    self.metrics["by_category"].to_excel(writer, sheet_name='By_Category')

                # Sheet 5: Monthly Trends
                if "monthly_trend" in self.metrics:
                    trend_df = self.metrics["monthly_trend"].copy()
                    # Convert Period index to string for Excel compatibility
                    trend_df.index = trend_df.index.astype(str)
                    trend_df.to_excel(writer, sheet_name='Trends')

                # Sheet 6: Cleaned Transactional Data (The source of truth)
                self.df.to_excel(writer, sheet_name='Raw_Cleaned_Data', index=False)

            self.logger.info(f"Excel report successfully exported for {client_id}")
            return filepath

        except Exception as e:
            self.logger.error(f"Failed to export Excel report: {str(e)}")
            raise e