from pathlib import Path

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# INPUT FOLDERS
INPUT_CSV = BASE_DIR / "input" / "csv"
INPUT_EXCEL = BASE_DIR / "input" / "excel"
INPUT_PDF = BASE_DIR / "input" / "pdf"

# OUTPUT
OUTPUT_DATA = BASE_DIR / "output"
OUTPUT_REPORT = BASE_DIR / "output" / "reports"

# LOG DIRECTORY
LOG_DIR = BASE_DIR / "logs"