from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

BANK_FILE = INPUT_DIR / "bank_statement.csv"
INVOICE_FILE = INPUT_DIR / "invoice.csv"

OUTPUT_FILE = OUTPUT_DIR / "reconciliation_report.csv"
LOG_FILE = LOG_DIR / "engine.log"