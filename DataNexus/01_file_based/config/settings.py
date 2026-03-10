from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_CSV = BASE_DIR / "input" / "csv"
INPUT_EXCEL = BASE_DIR / "input" / "excel"
INPUT_PDF = BASE_DIR / "input" / "pdf"

OUTPUT_REPORT = BASE_DIR / "output" / "reports"
LOG_DIR = BASE_DIR / "logs"