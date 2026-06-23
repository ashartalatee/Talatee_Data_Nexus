"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Google Form Collector             ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : collectors/google_form.py
Purpose : Collect & auto-sync data from Google Forms via Google Sheets API
          (Google Forms stores responses directly in Google Sheets)
Author  : Python Automation Engine
Version : 1.0.0

Flow:
    Google Form → Google Sheets (auto-linked) → This Collector → Processor

Requirements:
    pip install google-auth google-auth-oauthlib google-auth-httplib2
                google-api-python-client pandas python-dotenv loguru

Google API Setup:
    1. Go to https://console.cloud.google.com/
    2. Create project → Enable Google Sheets API & Google Drive API
    3. Create Service Account → Download JSON key
    4. Share your Google Sheet with the service account email
    5. Set GOOGLE_SERVICE_ACCOUNT_PATH in .env
"""

from __future__ import annotations

import os
import time
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER SETUP
# ─────────────────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sink=LOGS_DIR / "google_form_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}",
    encoding="utf-8",
)
logger.add(
    sink=lambda msg: print(msg, end=""),
    level="INFO",
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

MAX_RETRIES      = int(os.getenv("GOOGLE_MAX_RETRIES", 3))
RETRY_DELAY      = float(os.getenv("GOOGLE_RETRY_DELAY", 2.0))   # seconds
BATCH_SIZE       = int(os.getenv("GOOGLE_BATCH_SIZE", 500))
DEFAULT_SHEET    = os.getenv("GOOGLE_DEFAULT_SHEET_NAME", "Form Responses 1")
SERVICE_ACCOUNT  = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "")


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────
class GoogleFormCollectorError(Exception):
    """Base exception for Google Form Collector."""


class AuthenticationError(GoogleFormCollectorError):
    """Raised when Google API authentication fails."""


class SpreadsheetNotFoundError(GoogleFormCollectorError):
    """Raised when spreadsheet ID is not found or inaccessible."""


class NoDataError(GoogleFormCollectorError):
    """Raised when the sheet contains no data."""


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────
class CollectionResult:
    """
    Immutable result object returned by GoogleFormCollector.collect().

    Attributes:
        dataframe   : Collected data as pandas DataFrame.
        total_rows  : Total raw rows fetched (including header).
        valid_rows  : Rows after dedup / empty-row removal.
        duplicates  : Number of duplicate rows detected.
        sheet_id    : Spreadsheet ID that was queried.
        sheet_name  : Worksheet tab name.
        collected_at: UTC timestamp of collection.
        checksum    : SHA-256 of the raw JSON payload for integrity checks.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        total_rows: int,
        valid_rows: int,
        duplicates: int,
        sheet_id: str,
        sheet_name: str,
        collected_at: datetime,
        checksum: str,
    ) -> None:
        self.dataframe    = dataframe
        self.total_rows   = total_rows
        self.valid_rows   = valid_rows
        self.duplicates   = duplicates
        self.sheet_id     = sheet_id
        self.sheet_name   = sheet_name
        self.collected_at = collected_at
        self.checksum     = checksum

    # ── helpers ──────────────────────────────────────────────────────────────
    @property
    def is_empty(self) -> bool:
        return self.dataframe.empty

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "sheet_id"    : self.sheet_id,
            "sheet_name"  : self.sheet_name,
            "total_rows"  : self.total_rows,
            "valid_rows"  : self.valid_rows,
            "duplicates"  : self.duplicates,
            "columns"     : list(self.dataframe.columns),
            "collected_at": self.collected_at.isoformat(),
            "checksum"    : self.checksum,
        }

    def __repr__(self) -> str:
        return (
            f"<CollectionResult sheet='{self.sheet_name}' "
            f"valid_rows={self.valid_rows} duplicates={self.duplicates}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN COLLECTOR CLASS
# ─────────────────────────────────────────────────────────────────────────────
class GoogleFormCollector:
    """
    Production-grade collector for Google Form responses.

    Google Forms automatically links responses to a Google Sheet.
    This collector reads that Sheet via the Sheets API v4.

    Usage:
        collector = GoogleFormCollector(
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
            sheet_name="Form Responses 1",
        )
        result = collector.collect()
        df = result.dataframe

    Args:
        spreadsheet_id  : The ID from the Google Sheet URL
                          (https://docs.google.com/spreadsheets/d/<ID>/edit)
        sheet_name      : Tab name inside the spreadsheet.
        service_account_path : Path to the service account JSON key file.
                               Falls back to GOOGLE_SERVICE_ACCOUNT_PATH env var.
        deduplicate     : Auto-remove duplicate rows (default True).
        timestamp_col   : Name of the timestamp column from Google Forms.
                          Used for sorting and incremental fetch.
    """

    def __init__(
        self,
        spreadsheet_id: str,
        sheet_name: str = DEFAULT_SHEET,
        service_account_path: Optional[str] = None,
        deduplicate: bool = True,
        timestamp_col: str = "Timestamp",
    ) -> None:
        self.spreadsheet_id      = spreadsheet_id.strip()
        self.sheet_name          = sheet_name.strip()
        self.service_account_path = service_account_path or SERVICE_ACCOUNT
        self.deduplicate         = deduplicate
        self.timestamp_col       = timestamp_col

        self._service = None   # lazy-loaded Sheets API client
        self._validate_config()

    # ── private: validation ───────────────────────────────────────────────────
    def _validate_config(self) -> None:
        if not self.spreadsheet_id:
            raise ValueError("spreadsheet_id cannot be empty.")
        if not self.service_account_path:
            raise AuthenticationError(
                "Service account path not set. "
                "Pass service_account_path= or set GOOGLE_SERVICE_ACCOUNT_PATH in .env"
            )
        sa_path = Path(self.service_account_path)
        if not sa_path.exists():
            raise AuthenticationError(
                f"Service account file not found: {sa_path.resolve()}\n"
                "Download it from Google Cloud Console → IAM → Service Accounts."
            )
        logger.debug(f"Config validated | sheet_id={self.spreadsheet_id} | tab='{self.sheet_name}'")

    # ── private: auth ─────────────────────────────────────────────────────────
    def _get_service(self):
        """Build (and cache) the Google Sheets API service client."""
        if self._service is not None:
            return self._service

        logger.info("🔑 Authenticating with Google Sheets API …")
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.service_account_path, scopes=SCOPES
            )
            self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
            logger.info("✅ Authentication successful.")
            return self._service
        except Exception as exc:
            raise AuthenticationError(f"Google authentication failed: {exc}") from exc

    # ── private: fetch raw data ───────────────────────────────────────────────
    def _fetch_raw(self) -> tuple[list[list[str]], str]:
        """
        Fetch all rows from the sheet in batches.

        Returns:
            (rows, checksum) where rows is list-of-lists and
            checksum is SHA-256 of the serialised payload.
        """
        service = self._get_service()
        sheet_range = f"'{self.sheet_name}'!A1:ZZ"

        logger.info(f"📥 Fetching data from spreadsheet …")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = (
                    service.spreadsheets()
                    .values()
                    .get(
                        spreadsheetId=self.spreadsheet_id,
                        range=sheet_range,
                        valueRenderOption="UNFORMATTED_VALUE",
                        dateTimeRenderOption="FORMATTED_STRING",
                    )
                    .execute()
                )

                rows     = response.get("values", [])
                raw_json = json.dumps(rows, ensure_ascii=False)
                checksum = hashlib.sha256(raw_json.encode()).hexdigest()

                logger.info(f"📦 Fetched {len(rows)} raw rows (attempt {attempt}/{MAX_RETRIES})")
                return rows, checksum

            except HttpError as exc:
                status = exc.resp.status
                if status == 404:
                    raise SpreadsheetNotFoundError(
                        f"Spreadsheet not found: {self.spreadsheet_id}\n"
                        "Make sure the sheet is shared with the service account email."
                    ) from exc
                if status in (429, 500, 503) and attempt < MAX_RETRIES:
                    wait = RETRY_DELAY * attempt
                    logger.warning(f"⚠️  HTTP {status} — retrying in {wait}s …")
                    time.sleep(wait)
                    continue
                raise GoogleFormCollectorError(f"Google Sheets API error: {exc}") from exc

        raise GoogleFormCollectorError(f"Failed after {MAX_RETRIES} attempts.")

    # ── private: parse to DataFrame ───────────────────────────────────────────
    def _parse_to_dataframe(
        self, rows: list[list[str]]
    ) -> tuple[pd.DataFrame, int, int]:
        """
        Convert raw rows → clean DataFrame.

        Returns:
            (df, total_rows, duplicates_removed)
        """
        if not rows:
            raise NoDataError(
                f"Sheet '{self.sheet_name}' is empty. "
                "Make sure the form has at least one response."
            )

        headers   = rows[0]
        data_rows = rows[1:]
        total_rows = len(data_rows)

        if not data_rows:
            raise NoDataError("Sheet has a header row but no response data yet.")

        # ── normalise jagged rows (Google Sheets omits trailing empty cells) ──
        max_cols = len(headers)
        normalised = [
            row + [""] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols]
            for row in data_rows
        ]

        df = pd.DataFrame(normalised, columns=headers)

        # ── clean column names ────────────────────────────────────────────────
        df.columns = [col.strip() for col in df.columns]

        # ── drop completely empty rows ────────────────────────────────────────
        df.replace("", pd.NA, inplace=True)
        df.dropna(how="all", inplace=True)

        # ── parse timestamp ───────────────────────────────────────────────────
        if self.timestamp_col in df.columns:
            df[self.timestamp_col] = pd.to_datetime(
                df[self.timestamp_col], errors="coerce", dayfirst=True
            )
            df.sort_values(self.timestamp_col, ascending=True, inplace=True)
            df.reset_index(drop=True, inplace=True)

        # ── deduplicate ───────────────────────────────────────────────────────
        duplicates = 0
        if self.deduplicate:
            before = len(df)
            df.drop_duplicates(inplace=True)
            duplicates = before - len(df)
            if duplicates:
                logger.warning(f"🔁 Removed {duplicates} duplicate row(s).")

        # ── add metadata columns ──────────────────────────────────────────────
        df["_collected_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        df["_source"]       = f"google_form:{self.spreadsheet_id[:8]}…"

        logger.info(
            f"✅ Parsed {len(df)} valid rows | "
            f"{duplicates} duplicates removed | "
            f"{len(df.columns)} columns"
        )
        return df, total_rows, duplicates

    # ── public: incremental fetch ─────────────────────────────────────────────
    def collect_since(self, since: datetime) -> CollectionResult:
        """
        Fetch only rows submitted after *since* (UTC).

        Useful for scheduled jobs that run every N minutes to avoid
        re-processing old data.

        Args:
            since: datetime (UTC) — only rows with Timestamp > since are kept.

        Returns:
            CollectionResult containing only new rows.
        """
        logger.info(f"🕐 Incremental collect since {since.isoformat()} …")
        result = self.collect()

        if self.timestamp_col not in result.dataframe.columns:
            logger.warning(
                f"Timestamp column '{self.timestamp_col}' not found — "
                "returning full dataset."
            )
            return result

        mask = result.dataframe[self.timestamp_col] > pd.Timestamp(since)
        filtered_df = result.dataframe[mask].reset_index(drop=True)

        logger.info(
            f"📊 Incremental result: {len(filtered_df)} new rows "
            f"(out of {result.valid_rows} total)"
        )

        return CollectionResult(
            dataframe    = filtered_df,
            total_rows   = result.total_rows,
            valid_rows   = len(filtered_df),
            duplicates   = result.duplicates,
            sheet_id     = self.spreadsheet_id,
            sheet_name   = self.sheet_name,
            collected_at = result.collected_at,
            checksum     = result.checksum,
        )

    # ── public: list all sheet tabs ───────────────────────────────────────────
    def list_sheets(self) -> list[str]:
        """
        Return all worksheet tab names in the spreadsheet.
        Useful when you don't know the exact tab name.
        """
        service = self._get_service()
        try:
            meta = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
            logger.info(f"📋 Found {len(tabs)} sheet(s): {tabs}")
            return tabs
        except HttpError as exc:
            raise GoogleFormCollectorError(f"Could not list sheets: {exc}") from exc

    # ── public: main collect ──────────────────────────────────────────────────
    def collect(self) -> CollectionResult:
        """
        Fetch ALL responses from the linked Google Sheet.

        Returns:
            CollectionResult with .dataframe, .summary, etc.

        Raises:
            AuthenticationError      : Credentials invalid or missing.
            SpreadsheetNotFoundError : Sheet ID wrong or not shared.
            NoDataError              : Sheet exists but has no responses.
            GoogleFormCollectorError : Any other API-level error.
        """
        logger.info(
            f"🚀 Starting Google Form collection | "
            f"sheet_id={self.spreadsheet_id[:12]}… | tab='{self.sheet_name}'"
        )
        start_time = time.perf_counter()

        rows, checksum = self._fetch_raw()
        df, total_rows, duplicates = self._parse_to_dataframe(rows)

        elapsed = time.perf_counter() - start_time
        logger.info(f"⏱️  Collection completed in {elapsed:.2f}s")

        result = CollectionResult(
            dataframe    = df,
            total_rows   = total_rows,
            valid_rows   = len(df),
            duplicates   = duplicates,
            sheet_id     = self.spreadsheet_id,
            sheet_name   = self.sheet_name,
            collected_at = datetime.utcnow(),
            checksum     = checksum,
        )

        logger.info(f"📊 Result → {result}")
        return result


# ─────────────────────────────────────────────────────────────────────────────
# FACTORY FUNCTION  (convenience wrapper)
# ─────────────────────────────────────────────────────────────────────────────
def collect_google_form(
    spreadsheet_id: str,
    sheet_name: str = DEFAULT_SHEET,
    since: Optional[datetime] = None,
    **kwargs,
) -> CollectionResult:
    """
    One-liner factory for quick usage in main.py or notebooks.

    Args:
        spreadsheet_id : Google Sheets ID.
        sheet_name     : Tab name (default: 'Form Responses 1').
        since          : If set, only return rows newer than this datetime.
        **kwargs       : Forwarded to GoogleFormCollector.__init__().

    Returns:
        CollectionResult

    Example:
        result = collect_google_form("1BxiMVs0XR…", since=datetime(2024, 6, 1))
        print(result.dataframe.head())
    """
    collector = GoogleFormCollector(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        **kwargs,
    )
    return collector.collect_since(since) if since else collector.collect()


# ─────────────────────────────────────────────────────────────────────────────
# CLI / QUICK TEST  (python collectors/google_form.py)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    print("=" * 70)
    print("  Google Form Collector — Quick Test")
    print("=" * 70)

    # ── read args ─────────────────────────────────────────────────────────────
    if len(sys.argv) < 2:
        print(
            "\nUsage:\n"
            "  python collectors/google_form.py <SPREADSHEET_ID> [SHEET_NAME]\n\n"
            "Example:\n"
            "  python collectors/google_form.py 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms\n"
        )
        sys.exit(0)

    sheet_id   = sys.argv[1]
    sheet_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SHEET

    try:
        # ── list available tabs ───────────────────────────────────────────────
        collector = GoogleFormCollector(
            spreadsheet_id=sheet_id,
            sheet_name=sheet_name,
        )
        print(f"\n📋 Available tabs: {collector.list_sheets()}")

        # ── full collect ──────────────────────────────────────────────────────
        result = collector.collect()

        # ── display summary ───────────────────────────────────────────────────
        print("\n" + "─" * 50)
        print("📊  COLLECTION SUMMARY")
        print("─" * 50)
        for k, v in result.summary.items():
            print(f"  {k:<18}: {v}")

        print("\n📋  FIRST 5 ROWS")
        print("─" * 50)
        print(result.dataframe.head().to_string(index=False))

        print("\n✅  Collection completed successfully!")

    except AuthenticationError as e:
        logger.error(f"🔑 Auth error: {e}")
        sys.exit(1)
    except SpreadsheetNotFoundError as e:
        logger.error(f"🔍 Sheet not found: {e}")
        sys.exit(1)
    except NoDataError as e:
        logger.warning(f"📭 No data: {e}")
        sys.exit(0)
    except GoogleFormCollectorError as e:
        logger.error(f"💥 Collector error: {e}")
        sys.exit(1)