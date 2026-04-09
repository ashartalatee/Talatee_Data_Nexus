import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

class ReportBuilder:
    """
    Constructs client-ready multi-tab reports for e-commerce performance.
    Combines raw data, calculated metrics, and automated insights.
    """
    def __init__(self, config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = config
        self.base_dir = base_dir
        self.logger = logger
        self.client_id = config.get("client_id", "default_client")
        self.output_cfg = config.get("output", {})
        
        self.report_path = self.base_dir / "exports" / self.client_id
        self.report_path.mkdir(parents=True, exist_ok=True)

    def build_excel_report(self, 
                           processed_df: pd.DataFrame, 
                           metrics_df: pd.DataFrame, 
                           insights: Dict[str, Any], 
                           filename: str) -> bool:
        """
        Creates a high-level Excel workbook with multiple analytical sheets.
        """
        if processed_df is None or processed_df.empty:
            self.logger.error("Report generation failed: No processed data available.")
            return False

        full_path = self.report_path / f"{filename}.xlsx"
        self.logger.info(f"Building executive Excel report at {full_path}")

        try:
            with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
                # 1. Performance Overview (Metrics)
                if metrics_df is not None and not metrics_df.empty:
                    metrics_df.to_excel(writer, sheet_name='Performance Summary', index=False)
                
                # 2. Detailed Transactions (Cleaned Data)
                processed_df.to_excel(writer, sheet_name='Detailed Transactions', index=False)

                # 3. Insights Sheet
                if insights:
                    insight_summary = self._format_insights_for_excel(insights)
                    insight_summary.to_excel(writer, sheet_name='Automated Insights', index=False)

                # 4. Platform Breakdown
                if "platform" in processed_df.columns:
                    platform_summary = processed_df.groupby("platform").agg({
                        "order_id": "nunique",
                        "total_price": "sum",
                        "quantity": "sum"
                    }).reset_index().rename(columns={"order_id": "Total Orders", "total_price": "Revenue"})
                    platform_summary.to_excel(writer, sheet_name='Marketplace Analysis', index=False)

            self.logger.info(f"Executive report built successfully: {full_path.name}")
            return True

        except Exception as e:
            self.logger.critical(f"Critical error building report {filename}: {str(e)}", exc_info=True)
            return False

    def _format_insights_for_excel(self, insights: Dict[str, Any]) -> pd.DataFrame:
        """
        Flattens the insights dictionary into a readable tabular format for Excel.
        """
        rows = []
        
        # Executive Summary section
        exec_sum = insights.get("executive_summary", {})
        for key, val in exec_sum.items():
            rows.append({"Category": "Executive Summary", "Metric": key.replace("_", " ").title(), "Value": val})

        # Top Performers section
        performers = insights.get("top_performers", {})
        for key, val in performers.items():
            rows.append({"Category": "Top Performers", "Metric": key.replace("_", " ").title(), "Value": str(val)})

        # Anomalies/Alerts
        alerts = insights.get("anomalies", [])
        for alert in alerts:
            rows.append({"Category": "Alerts & Anomalies", "Metric": "Insight", "Value": alert})

        return pd.DataFrame(rows)