import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def export_to_csv(df: pd.DataFrame, output_path: Path) -> bool:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame, skipping CSV export")
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

        logger.info(f"Exported CSV to {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return False


def export_to_excel(df: pd.DataFrame, output_path: Path) -> bool:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame, skipping Excel export")
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(output_path, index=False)

        logger.info(f"Exported Excel to {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error exporting Excel: {e}")
        return False


def export_to_json(data: Dict[str, Any], output_path: Path) -> bool:
    try:
        if not data:
            logger.warning("Empty data, skipping JSON export")
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)

        import json
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info(f"Exported JSON to {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error exporting JSON: {e}")
        return False


def run_export(
    df: pd.DataFrame,
    metrics: Optional[Dict[str, Any]] = None,
    insights: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, bool]:
    try:
        results = {
            "csv": False,
            "excel": False,
            "json": False
        }

        if config:
            base_path = Path(config.get("base_path", "outputs"))
            client_name = config.get("client_name", "default_client")

            csv_path = base_path / client_name / "report.csv"
            excel_path = base_path / client_name / "report.xlsx"
            json_path = base_path / client_name / "report.json"

            if config.get("csv"):
                results["csv"] = export_to_csv(df, csv_path)

            if config.get("excel"):
                results["excel"] = export_to_excel(df, excel_path)

            if config.get("json"):
                combined_data = {
                    "metrics": metrics or {},
                    "insights": insights or {}
                }
                results["json"] = export_to_json(combined_data, json_path)

        logger.info(f"Export results: {results}")
        return results

    except Exception as e:
        logger.error(f"Error in export pipeline: {e}")
        return {"csv": False, "excel": False, "json": False}