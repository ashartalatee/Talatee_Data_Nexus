import logging
import pandas as pd
from typing import Dict, Any, Union, Optional, List
from pathlib import Path
from datetime import datetime

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("output_exporter")

class DataExporter:
    """
    Production-grade Export module.
    Handles multi-format (CSV, XLSX) and multi-tab exporting for 
    multi-client analytics reports.
    """

    def __init__(self, export_config: Dict[str, Any], client_id: str):
        """
        :param export_config: Dictionary containing 'output_dir', 'formats', and 'naming_convention'.
        :param client_id: The unique identifier for the client to organize folders.
        """
        self.config = export_config
        self.client_id = client_id
        self.base_path = Path(self.config.get("output_dir", "exports")) / self.client_id
        self._ensure_output_directory()
        
        logger.info(f"DataExporter initialized for client: {self.client_id}")

    def _ensure_output_directory(self):
        """Creates the client-specific output directory if it doesn't exist."""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create output directory {self.base_path}: {e}")

    def _generate_filename(self, suffix: str, extension: str) -> Path:
        """Generates a timestamped filename based on config."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.client_id}_{suffix}_{timestamp}.{extension}"
        return self.base_path / filename

    def export_to_csv(self, df: pd.DataFrame, suffix: str = "report") -> Optional[Path]:
        """Exports DataFrame to a standardized CSV file."""
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for CSV export.")
            return None

        file_path = self._generate_filename(suffix, "csv")
        try:
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.info(f"Successfully exported CSV to: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return None

    def export_to_excel(self, data_map: Dict[str, pd.DataFrame], suffix: str = "analytics_dashboard") -> Optional[Path]:
        """
        Exports multiple DataFrames to a single Excel workbook with multiple tabs.
        :param data_map: Dictionary where keys are sheet names and values are DataFrames.
        """
        if not data_map:
            logger.warning("No data provided for Excel export.")
            return None

        file_path = self._generate_filename(suffix, "xlsx")
        try:
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                for sheet_name, df in data_map.items():
                    if df is not None and not df.empty:
                        # Sanitize sheet name (max 31 chars)
                        safe_name = str(sheet_name)[:31]
                        df.to_excel(writer, sheet_name=safe_name, index=False)
                        
                        # Add basic formatting to header
                        workbook = writer.book
                        worksheet = writer.sheets[safe_name]
                        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
                        
                        for col_num, value in enumerate(df.columns.values):
                            worksheet.write(0, col_num, value, header_format)
                            # Auto-adjust column width
                            max_len = max(df[value].astype(str).map(len).max(), len(value)) + 2
                            worksheet.set_column(col_num, col_num, min(max_len, 50))
                
            logger.info(f"Successfully exported multi-tab Excel to: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to export Excel: {e}", exc_info=True)
            return None

    def export_summary_json(self, summary_dict: Dict[str, Any], suffix: str = "summary") -> Optional[Path]:
        """Exports executive summary dictionary to JSON for API or dashboard consumption."""
        import json
        file_path = self._generate_filename(suffix, "json")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(summary_dict, f, indent=4, default=str)
            logger.info(f"Successfully exported JSON summary to: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to export JSON: {e}")
            return None