import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union

class DataExporter:
    """
    Handles the multi-format export of processed data and insights.
    Optimized for Talatee Sentinel Engine with Multi-Sheet Excel support.
    """
    def __init__(self, config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = config
        self.base_dir = base_dir
        self.logger = logger
        self.output_cfg = config.get("output", {})
        self.client_id = config.get("client_id", "default_client")
        
        # Initialize output directory: exports/[client_id]/
        self.export_path = self.base_dir / "exports" / self.client_id
        self.export_path.mkdir(parents=True, exist_ok=True)

    def save(self, df: pd.DataFrame, analysis_results: Optional[pd.DataFrame] = None) -> bool:
        """
        ENTRY POINT UTAMA: Dipanggil oleh runner.py.
        Menyimpan data utama dan hasil analisis ke dalam satu file (Excel) atau file terpisah.
        """
        if df is None or df.empty:
            self.logger.error("Export failed: Primary DataFrame is empty.")
            return False

        filename = self.output_cfg.get("filename", f"report_{self.client_id}")
        target_format = self.output_cfg.get("format", "excel").lower()
        
        # Proteksi: Pastikan tidak ada kolom duplikat sebelum menulis ke file
        df = df.loc[:, ~df.columns.duplicated()].copy()
        if analysis_results is not None:
            analysis_results = analysis_results.loc[:, ~analysis_results.columns.duplicated()].copy()

        try:
            # Jika format Excel, buat multi-sheet
            if target_format in ["excel", "xlsx"]:
                full_path = self.export_path / f"{filename}.xlsx"
                self.logger.info(f"Saving combined Excel report to {full_path}")
                
                with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name="Cleaned_Data", index=False)
                    if analysis_results is not None:
                        analysis_results.to_excel(writer, sheet_name="Business_Insights", index=False)
                return True
            
            # Jika format lain (CSV/JSON), ekspor secara terpisah
            else:
                success_df = self.export(df, f"{filename}_data", target_format)
                if analysis_results is not None:
                    self.export(analysis_results, f"{filename}_insights", target_format)
                return success_df

        except Exception as e:
            self.logger.error(f"Critical error during save: {str(e)}", exc_info=True)
            return False

    def export(self, data: Union[pd.DataFrame, Dict[str, Any]], filename: str, file_format: Optional[str] = None) -> bool:
        """
        Orchestrates the export process for a single object.
        """
        if data is None: return False

        target_format = (file_format or self.output_cfg.get("format", "excel")).lower()
        ext = self._get_extension(target_format)
        full_path = self.export_path / f"{filename}.{ext}"

        try:
            if isinstance(data, pd.DataFrame):
                return self._export_dataframe(data, full_path, target_format)
            elif isinstance(data, (dict, list)):
                return self._export_json(data, full_path)
            return False
        except Exception as e:
            self.logger.error(f"Failed to export {filename}: {str(e)}")
            return False

    def _export_dataframe(self, df: pd.DataFrame, path: Path, fmt: str) -> bool:
        """Exports pandas DataFrame to various formats."""
        self.logger.info(f"Writing DataFrame to {path}")
        
        if fmt in ["excel", "xlsx"]:
            df.to_excel(path, index=False, engine="openpyxl")
        elif fmt == "csv":
            df.to_csv(path, index=False, encoding="utf-8")
        elif fmt == "json":
            df.to_json(path, orient="records", indent=4)
        else:
            df.to_csv(path, index=False)
            
        return path.exists()

    def _export_json(self, data: Any, path: Path) -> bool:
        """Exports dictionaries or lists to JSON."""
        self.logger.info(f"Writing JSON to {path}")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)
        return path.exists()

    def _get_extension(self, fmt: str) -> str:
        mapping = {"excel": "xlsx", "xlsx": "xlsx", "csv": "csv", "json": "json"}
        return mapping.get(fmt, "csv")