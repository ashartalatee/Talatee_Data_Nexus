import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union

class DataExporter:
    """
    Handles the multi-format export of processed data and insights.
    Ensures output is saved in client-specific directories with standardized naming.
    """
    def __init__(self, config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = config
        self.base_dir = base_dir
        self.logger = logger
        self.output_cfg = config.get("output", {})
        self.client_id = config.get("client_id", "default_client")
        
        # Initialize output directory
        self.export_path = self.base_dir / "exports" / self.client_id
        self.export_path.mkdir(parents=True, exist_ok=True)

    def export(self, data: Union[pd.DataFrame, Dict[str, Any]], filename: str, file_format: Optional[str] = None) -> bool:
        """
        Orchestrates the export process based on the requested format.
        """
        if data is None:
            self.logger.error(f"Export failed: No data provided for {filename}")
            return False

        target_format = (file_format or self.output_cfg.get("format", "excel")).lower()
        full_path = self.export_path / f"{filename}.{self._get_extension(target_format)}"

        try:
            if isinstance(data, pd.DataFrame):
                return self._export_dataframe(data, full_path, target_format)
            elif isinstance(data, (dict, list)):
                return self._export_json(data, full_path)
            else:
                self.logger.error(f"Unsupported data type for export: {type(data)}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to export {filename}: {str(e)}", exc_info=True)
            return False

    def _export_dataframe(self, df: pd.DataFrame, path: Path, fmt: str) -> bool:
        """Exports pandas DataFrame to various formats."""
        self.logger.info(f"Exporting DataFrame to {path} as {fmt}")
        
        if fmt in ["excel", "xlsx"]:
            df.to_excel(path, index=False, engine="openpyxl")
        elif fmt == "csv":
            df.to_csv(path, index=False, encoding="utf-8")
        elif fmt == "json":
            df.to_json(path, orient="records", indent=4)
        else:
            self.logger.warning(f"Format {fmt} not explicitly supported. Defaulting to CSV.")
            df.to_csv(path.with_suffix(".csv"), index=False)
            
        return path.exists()

    def _export_json(self, data: Any, path: Path) -> bool:
        """Exports dictionaries or lists (Insights/Summaries) to JSON."""
        self.logger.info(f"Exporting JSON object to {path}")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)
        return path.exists()

    def _get_extension(self, fmt: str) -> str:
        """Helper to map format strings to file extensions."""
        mapping = {
            "excel": "xlsx",
            "xlsx": "xlsx",
            "csv": "csv",
            "json": "json"
        }
        return mapping.get(fmt, "csv")