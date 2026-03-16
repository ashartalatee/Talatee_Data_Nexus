from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_CSV = BASE_DIR / "input" / "csv"
INPUT_EXCEL = BASE_DIR / "input" / "excel"
INPUT_PDF = BASE_DIR / "input" / "pdf"

OUTPUT_DATA = BASE_DIR / "output"

LOG_DIR = BASE_DIR / "logs"

METADATA_DIR = BASE_DIR / "metadata"
PROCESSED_FILES = METADATA_DIR / "processed_files.json"

REPORT_DIR = BASE_DIR / "reports"
PIPELINE_REPORT = REPORT_DIR / "pipeline_run_report.json"