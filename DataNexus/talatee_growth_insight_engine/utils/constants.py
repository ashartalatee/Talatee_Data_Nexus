"""
Global constants for the E-commerce Analytics Engine.
Centralized configuration for paths, column names, and pipeline settings.
"""

from pathlib import Path
from typing import Dict, List, Set, Any
from enum import Enum
import pandas as pd


# === PATHS ===
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
CONFIG_DIR = BASE_DIR / "config"

# Default directories
RAW_DATA_DIR = DATA_DIR / "raw"
CLEANED_DATA_DIR = DATA_DIR / "cleaned"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure base directories exist
for directory in [DATA_DIR, LOGS_DIR, REPORTS_DIR, RAW_DATA_DIR, CLEANED_DATA_DIR, PROCESSED_DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# === PIPELINE STAGES ===
class PipelineStage(Enum):
    """Pipeline execution stages."""
    INGESTION = "ingestion"
    CLEANING = "cleaning"
    TRANSFORM = "transform"
    ANALYSIS = "analysis"
    EXPORT = "export"


PIPELINE_STAGES = [stage.value for stage in PipelineStage]


# === DATA SOURCES ===
SUPPORTED_SOURCES: List[str] = ["shopee", "tokopedia", "tiktokshop", "whatsapp"]
MARKETPLACE_MAPPING: Dict[str, str] = {
    "shopee": "Shopee",
    "tokopedia": "Tokopedia", 
    "tiktokshop": "TikTok Shop",
    "whatsapp": "WhatsApp"
}


# === STANDARD COLUMNS ===
# Expected standardized column names after cleaning
STANDARD_COLUMNS: Dict[str, str] = {
    "date": "order_date",
    "product_name": "product_name",
    "product_id": "product_id",
    "price": "price",
    "quantity": "quantity",
    "total_amount": "total_amount",
    "platform": "platform",
    "order_id": "order_id",
    "customer_id": "customer_id",
    "status": "status"
}

REQUIRED_COLUMNS: Set[str] = {"order_date", "product_id", "total_amount", "platform"}
NUMERIC_COLUMNS: Set[str] = {"price", "quantity", "total_amount"}
DATE_COLUMNS: Set[str] = {"order_date"}


# === DATA CLEANING ===
DEFAULT_MISSING_STRATEGY: str = "forward_fill"
MISSING_THRESHOLD: float = 0.3  # Drop columns with >30% missing
DUPLICATE_COLUMNS: List[str] = ["order_id", "product_id"]


# === FEATURE ENGINEERING ===
DATE_FEATURES: List[str] = [
    "day_of_week", "day_of_month", "month", 
    "quarter", "week_of_year", "is_weekend"
]
ROLLING_WINDOWS: List[int] = [7, 14, 30, 90]
GROWTH_METRICS: List[str] = ["revenue", "orders", "units_sold"]


# === ANALYTICS ===
DEFAULT_TOP_N: int = 10
CORE_METRICS: List[str] = [
    "total_revenue", "total_orders", "total_units", 
    "avg_order_value", "conversion_rate"
]
TREND_PERIODS: List[str] = ["week_over_week", "month_over_month"]


# === EXPORT ===
SUPPORTED_EXPORT_FORMATS: List[str] = ["csv", "xlsx"]
DEFAULT_SHEETS: Dict[str, str] = {
    "summary": "Summary",
    "top_products": "Top Products", 
    "trends": "Trends",
    "insights": "Insights"
}
FILENAME_TEMPLATE: str = "{client_id}_{date:%Y%m%d}_report"


# === DATA TYPES ===
DTYPE_MAPPING: Dict[str, str] = {
    "order_date": "datetime64[ns]",
    "price": "float64",
    "quantity": "int64",
    "total_amount": "float64",
    "product_id": "string",
    "platform": "category"
}


# === STATUS & VALIDATION ===
ORDER_STATUSES: List[str] = [
    "completed", "shipped", "delivered", "cancelled", 
    "pending", "refunded", "returned"
]
VALID_PLATFORMS: Set[str] = set(MARKETPLACE_MAPPING.values())


# === ALERT THRESHOLDS (DEFAULTS) ===
ALERT_THRESHOLDS: Dict[str, float] = {
    "revenue_drop": 0.20,      # 20%
    "order_drop": 0.15,        # 15%
    "return_rate": 0.10,       # 10%
    "inventory_low": 50
}


# === PANDAS SETTINGS ===
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 50)
pd.set_option('display.float_format', '{:.2f}'.format)
pd.set_option('mode.chained_assignment', None)


# === FILE EXTENSIONS ===
FILE_EXTENSIONS: Dict[str, str] = {
    "csv": ".csv",
    "excel": ".xlsx",
    "parquet": ".parquet"
}


# === TIMEZONES ===
DEFAULT_TIMEZONE: str = "Asia/Jakarta"
SUPPORTED_TIMEZONES: List[str] = [
    "Asia/Jakarta", "Asia/Singapore", "UTC", "Asia/Hong_Kong"
]