"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Main Orchestrator                 ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : main.py
Purpose : Central entry point — orchestrate the full pipeline:
          Collect → Validate → Process → Store → Report → Notify

Usage:
    # Run full pipeline (collect ALL data)
    python main.py

    # Run incremental (only new data since last run)
    python main.py --mode incremental

    # Run specific collector only
    python main.py --collector google_form

    # Dry run (no writes, just preview)
    python main.py --dry-run

    # Custom spreadsheet at runtime
    python main.py --sheet-id 1BxiMVs0XRA5nFMdKvBdBZjgm --sheet-name "Sheet1"

Pipeline Flow:
    ┌─────────────┐    ┌───────────┐    ┌───────────┐    ┌─────────┐
    │  COLLECTORS │ →  │ VALIDATOR │ →  │  STORAGE  │ →  │ REPORT  │
    │ google_form │    │  email    │    │    CSV    │    │ summary │
    │ csv_import  │    │  phone    │    │   Excel   │    │         │
    │  webhook    │    │  dupes    │    │ Postgres  │    │         │
    └─────────────┘    └───────────┘    └─────────────┘    └─────────┘

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# PATH SETUP  (ensure project root is importable)
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env")

LOGS_DIR    = BASE_DIR / "logs"
OUTPUTS_DIR = BASE_DIR / "outputs"
STATE_FILE  = BASE_DIR / "logs" / ".last_run_state.json"   # incremental tracking

LOGS_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER  (file + console)
# ─────────────────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sink=LOGS_DIR / "main_{time:YYYY-MM-DD}.log",
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
# CONFIG  (all runtime settings in one place)
# ─────────────────────────────────────────────────────────────────────────────
CONFIG: dict[str, Any] = {
    # ── Google Form ───────────────────────────────────────────────────────────
    "spreadsheet_id"  : os.getenv("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE"),
    "sheet_name"      : os.getenv("GOOGLE_DEFAULT_SHEET_NAME", "Form Responses 1"),
    "timestamp_col"   : os.getenv("TIMESTAMP_COL", "Timestamp"),

    # ── Storage destinations (toggle on/off) ──────────────────────────────────
    "save_csv"        : os.getenv("SAVE_CSV",   "true").lower() == "true",
    "save_excel"      : os.getenv("SAVE_EXCEL", "true").lower() == "true",
    "save_postgres"   : os.getenv("SAVE_POSTGRES", "false").lower() == "true",

    # ── Output paths ──────────────────────────────────────────────────────────
    "output_csv"      : OUTPUTS_DIR / f"form_data_{datetime.now():%Y%m%d}.csv",
    "output_excel"    : OUTPUTS_DIR / f"form_data_{datetime.now():%Y%m%d}.xlsx",

    # ── Postgres (only needed if save_postgres=true) ──────────────────────────
    "pg_dsn"          : os.getenv("POSTGRES_DSN", "postgresql://user:pass@localhost/dbname"),
    "pg_table"        : os.getenv("POSTGRES_TABLE", "form_responses"),

    # ── Validation ────────────────────────────────────────────────────────────
    "validate_email"  : os.getenv("VALIDATE_EMAIL",  "true").lower() == "true",
    "validate_phone"  : os.getenv("VALIDATE_PHONE",  "true").lower() == "true",
    "check_duplicates": os.getenv("CHECK_DUPLICATES","true").lower() == "true",
    "email_col"       : os.getenv("EMAIL_COL",  "Email"),
    "phone_col"       : os.getenv("PHONE_COL",  "Phone"),

    # ── Pipeline behaviour ────────────────────────────────────────────────────
    "incremental_hours": int(os.getenv("INCREMENTAL_HOURS", 24)),  # look-back window
    "dry_run"         : False,   # overridden by --dry-run flag
    "mode"            : "full",  # "full" | "incremental"
}


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGEMENT  (for incremental mode)
# ─────────────────────────────────────────────────────────────────────────────
class StateManager:
    """
    Persist the last successful run timestamp to disk.
    Enables incremental collection — only fetch rows newer than last run.
    """

    @staticmethod
    def load() -> Optional[datetime]:
        """Return the last successful run datetime, or None if first run."""
        if not STATE_FILE.exists():
            logger.info("📂 No previous state found — will run full collection.")
            return None
        try:
            data = json.loads(STATE_FILE.read_text())
            dt   = datetime.fromisoformat(data["last_run"])
            logger.info(f"📂 Last successful run: {dt.isoformat()}")
            return dt
        except Exception as exc:
            logger.warning(f"⚠️  Could not read state file: {exc} — running full collection.")
            return None

    @staticmethod
    def save(run_time: datetime) -> None:
        """Persist the current run timestamp."""
        STATE_FILE.write_text(json.dumps({"last_run": run_time.isoformat()}, indent=2))
        logger.debug(f"💾 State saved: last_run={run_time.isoformat()}")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE STEPS
# ─────────────────────────────────────────────────────────────────────────────

def step_collect(cfg: dict, run_time: datetime) -> pd.DataFrame:
    """
    STEP 1 — COLLECT
    Fetch data from Google Form (via linked Google Sheet).
    Supports both full and incremental modes.
    """
    _banner("STEP 1 · COLLECT")

    # ── import here so the module is optional until needed ───────────────────
    from collectors.google_form import (
        GoogleFormCollector,
        AuthenticationError,
        SpreadsheetNotFoundError,
        NoDataError,
        GoogleFormCollectorError,
    )

    collector = GoogleFormCollector(
        spreadsheet_id=cfg["spreadsheet_id"],
        sheet_name=cfg["sheet_name"],
        timestamp_col=cfg["timestamp_col"],
        deduplicate=cfg["check_duplicates"],
    )

    # ── list available tabs (helpful for debugging) ───────────────────────────
    try:
        tabs = collector.list_sheets()
        logger.info(f"📋 Available tabs in spreadsheet: {tabs}")
    except Exception:
        pass   # non-critical

    # ── choose collect strategy ───────────────────────────────────────────────
    try:
        if cfg["mode"] == "incremental":
            since = StateManager.load()
            if since is None:
                # fallback: look-back N hours
                since = run_time - timedelta(hours=cfg["incremental_hours"])
                logger.info(f"🕐 Incremental fallback: collecting since {since.isoformat()}")
            result = collector.collect_since(since)
        else:
            result = collector.collect()

    except AuthenticationError as exc:
        logger.error(f"🔑 Authentication failed: {exc}")
        raise SystemExit(1) from exc
    except SpreadsheetNotFoundError as exc:
        logger.error(f"🔍 Spreadsheet not found: {exc}")
        raise SystemExit(1) from exc
    except NoDataError as exc:
        logger.warning(f"📭 No data available: {exc}")
        return pd.DataFrame()
    except GoogleFormCollectorError as exc:
        logger.error(f"💥 Collector error: {exc}")
        raise SystemExit(1) from exc

    # ── log summary ───────────────────────────────────────────────────────────
    logger.info(f"✅ Collected {result.valid_rows} rows | checksum={result.checksum[:12]}…")
    _print_summary_table(result.summary)

    return result.dataframe


def step_validate(df: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    STEP 2 — VALIDATE
    Run configurable validators. Returns (valid_df, invalid_df).

    Validators run in order:
        1. Email validator
        2. Phone validator
        3. Duplicate checker (cross-row)

    Invalid rows are quarantined, not dropped — they go to a separate
    'invalid' report for manual review.
    """
    _banner("STEP 2 · VALIDATE")

    if df.empty:
        logger.warning("⚠️  Empty DataFrame — skipping validation.")
        return df, pd.DataFrame()

    df = df.copy()
    df["_valid"]        = True
    df["_invalid_reason"] = ""

    total = len(df)

    # ── 1. Email validation ───────────────────────────────────────────────────
    if cfg["validate_email"] and cfg["email_col"] in df.columns:
        try:
            from validators.email_validator import is_valid_email
            mask = df[cfg["email_col"]].apply(
                lambda v: not is_valid_email(str(v)) if pd.notna(v) and str(v).strip() else False
            )
            df.loc[mask, "_valid"]          = False
            df.loc[mask, "_invalid_reason"] += "invalid_email;"
            bad = mask.sum()
            logger.info(f"📧 Email validation: {total - bad}/{total} valid | {bad} invalid")
        except ImportError:
            logger.warning("⚠️  validators/email_validator.py not found — skipping email check.")

    # ── 2. Phone validation ───────────────────────────────────────────────────
    if cfg["validate_phone"] and cfg["phone_col"] in df.columns:
        try:
            from validators.phone_validator import is_valid_phone
            mask = df[cfg["phone_col"]].apply(
                lambda v: not is_valid_phone(str(v)) if pd.notna(v) and str(v).strip() else False
            )
            df.loc[mask, "_valid"]          = False
            df.loc[mask, "_invalid_reason"] += "invalid_phone;"
            bad = mask.sum()
            logger.info(f"📱 Phone validation: {total - bad}/{total} valid | {bad} invalid")
        except ImportError:
            logger.warning("⚠️  validators/phone_validator.py not found — skipping phone check.")

    # ── 3. Duplicate checker (key-based) ─────────────────────────────────────
    if cfg["check_duplicates"] and cfg["email_col"] in df.columns:
        try:
            from validators.duplicate_checker import find_duplicates
            dup_mask = find_duplicates(df, key_col=cfg["email_col"])
            df.loc[dup_mask, "_valid"]          = False
            df.loc[dup_mask, "_invalid_reason"] += "duplicate_email;"
            bad = dup_mask.sum()
            logger.info(f"🔁 Duplicate check: {bad} duplicate row(s) flagged")
        except ImportError:
            logger.warning("⚠️  validators/duplicate_checker.py not found — skipping dupe check.")

    # ── split valid / invalid ─────────────────────────────────────────────────
    valid_df   = df[df["_valid"]].drop(columns=["_valid", "_invalid_reason"]).reset_index(drop=True)
    invalid_df = df[~df["_valid"]].reset_index(drop=True)

    logger.info(
        f"✅ Validation complete: {len(valid_df)} valid | {len(invalid_df)} invalid"
    )

    # ── save invalid rows for manual review ───────────────────────────────────
    if not invalid_df.empty and not cfg["dry_run"]:
        invalid_path = OUTPUTS_DIR / f"invalid_rows_{datetime.now():%Y%m%d_%H%M}.csv"
        invalid_df.to_csv(invalid_path, index=False, encoding="utf-8-sig")
        logger.warning(f"⚠️  Invalid rows saved → {invalid_path}")

    return valid_df, invalid_df


def step_store(df: pd.DataFrame, cfg: dict) -> dict[str, str]:
    """
    STEP 3 — STORE
    Save valid data to one or more configured storage backends.
    Returns a dict of {backend: path/status}.
    """
    _banner("STEP 3 · STORE")

    if df.empty:
        logger.warning("⚠️  No valid data to store.")
        return {}

    if cfg["dry_run"]:
        logger.info(f"🧪 DRY RUN — would save {len(df)} rows (no writes performed).")
        logger.info(f"   Preview (first 3 rows):\n{df.head(3).to_string()}")
        return {"dry_run": "no files written"}

    results: dict[str, str] = {}

    # ── CSV ───────────────────────────────────────────────────────────────────
    if cfg["save_csv"]:
        try:
            path = cfg["output_csv"]
            # append if file exists (for incremental runs)
            if Path(path).exists() and cfg["mode"] == "incremental":
                existing = pd.read_csv(path, encoding="utf-8-sig")
                df       = pd.concat([existing, df], ignore_index=True)
                df.drop_duplicates(inplace=True)
                logger.info(f"📎 Appending to existing CSV ({len(existing)} + {len(df)} rows)")

            df.to_csv(path, index=False, encoding="utf-8-sig")
            logger.info(f"💾 CSV saved → {path}")
            results["csv"] = str(path)
        except Exception as exc:
            logger.error(f"❌ CSV save failed: {exc}")

    # ── Excel ─────────────────────────────────────────────────────────────────
    if cfg["save_excel"]:
        try:
            path = cfg["output_excel"]
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Form Data", index=False)

                # ── auto-fit column widths ────────────────────────────────────
                ws = writer.sheets["Form Data"]
                for col in ws.columns:
                    max_len = max(
                        len(str(cell.value or "")) for cell in col
                    )
                    ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

            logger.info(f"💾 Excel saved → {path}")
            results["excel"] = str(path)
        except Exception as exc:
            logger.error(f"❌ Excel save failed: {exc}")

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    if cfg["save_postgres"]:
        try:
            import sqlalchemy as sa
            engine = sa.create_engine(cfg["pg_dsn"])
            df.to_sql(
                cfg["pg_table"],
                engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )
            logger.info(f"💾 PostgreSQL → table '{cfg['pg_table']}' updated ({len(df)} rows)")
            results["postgres"] = cfg["pg_table"]
        except ImportError:
            logger.warning("⚠️  sqlalchemy not installed — skipping Postgres. pip install sqlalchemy psycopg2-binary")
        except Exception as exc:
            logger.error(f"❌ PostgreSQL save failed: {exc}")

    return results


def step_report(
    df_valid: pd.DataFrame,
    df_invalid: pd.DataFrame,
    storage_results: dict,
    cfg: dict,
    run_time: datetime,
    elapsed: float,
) -> None:
    """
    STEP 4 — REPORT
    Print a structured run report to console + write JSON summary to logs/.
    """
    _banner("STEP 4 · REPORT")

    report = {
        "run_id"          : run_time.strftime("%Y%m%d_%H%M%S"),
        "run_time_utc"    : run_time.isoformat(),
        "mode"            : cfg["mode"],
        "dry_run"         : cfg["dry_run"],
        "elapsed_seconds" : round(elapsed, 2),
        "rows_collected"  : len(df_valid) + len(df_invalid),
        "rows_valid"      : len(df_valid),
        "rows_invalid"    : len(df_invalid),
        "storage"         : storage_results,
        "columns"         : list(df_valid.columns) if not df_valid.empty else [],
    }

    # ── console report ────────────────────────────────────────────────────────
    width = 60
    print("\n" + "═" * width)
    print(f"  📊  PIPELINE REPORT  —  {run_time:%Y-%m-%d %H:%M:%S}")
    print("═" * width)
    print(f"  {'Mode':<22}: {cfg['mode'].upper()}{' (DRY RUN)' if cfg['dry_run'] else ''}")
    print(f"  {'Rows collected':<22}: {report['rows_collected']}")
    print(f"  {'Rows valid':<22}: {report['rows_valid']}")
    print(f"  {'Rows invalid':<22}: {report['rows_invalid']}")
    print(f"  {'Storage backends':<22}: {', '.join(storage_results.keys()) or 'none'}")
    print(f"  {'Elapsed':<22}: {elapsed:.2f}s")
    print("─" * width)

    if not df_valid.empty:
        print(f"\n  📋  DATA PREVIEW (first 5 rows)")
        print("─" * width)
        pd.set_option("display.max_columns",  8)
        pd.set_option("display.width",        width)
        pd.set_option("display.max_colwidth", 20)
        print(df_valid.head(5).to_string(index=False))

    print("\n" + "═" * width + "\n")

    # ── write JSON report ─────────────────────────────────────────────────────
    if not cfg["dry_run"]:
        report_path = LOGS_DIR / f"run_report_{report['run_id']}.json"
        report_path.write_text(json.dumps(report, indent=2, default=str))
        logger.info(f"📄 Run report saved → {report_path}")

    # ── try summary_report module if available ────────────────────────────────
    try:
        from reports.summary_report import generate_summary
        generate_summary(df_valid, output_dir=OUTPUTS_DIR, run_time=run_time)
        logger.info("📊 Summary report generated via reports/summary_report.py")
    except ImportError:
        pass   # module not yet implemented — fine


# ─────────────────────────────────────────────────────────────────────────────
# FULL PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(cfg: dict) -> bool:
    """
    Execute the complete pipeline end-to-end.

    Returns True on success, False on failure.
    """
    run_time   = datetime.utcnow()
    start_time = time.perf_counter()

    _print_header(cfg, run_time)

    try:
        # ── STEP 1: Collect ───────────────────────────────────────────────────
        df_raw = step_collect(cfg, run_time)

        if df_raw.empty:
            logger.warning("Pipeline stopping — no data collected.")
            _print_footer(success=True, elapsed=time.perf_counter() - start_time)
            return True

        # ── STEP 2: Validate ──────────────────────────────────────────────────
        df_valid, df_invalid = step_validate(df_raw, cfg)

        # ── STEP 3: Store ─────────────────────────────────────────────────────
        storage_results = step_store(df_valid, cfg)

        # ── STEP 4: Report ────────────────────────────────────────────────────
        elapsed = time.perf_counter() - start_time
        step_report(df_valid, df_invalid, storage_results, cfg, run_time, elapsed)

        # ── Save state (for next incremental run) ─────────────────────────────
        if not cfg["dry_run"]:
            StateManager.save(run_time)

        _print_footer(success=True, elapsed=elapsed)
        return True

    except SystemExit:
        raise
    except KeyboardInterrupt:
        logger.warning("⛔ Pipeline interrupted by user.")
        _print_footer(success=False, elapsed=time.perf_counter() - start_time)
        return False
    except Exception as exc:
        logger.error(f"💥 Unexpected pipeline error: {exc}")
        logger.debug(traceback.format_exc())
        _print_footer(success=False, elapsed=time.perf_counter() - start_time)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _banner(title: str) -> None:
    logger.info(f"\n{'─'*55}\n  {title}\n{'─'*55}")


def _print_header(cfg: dict, run_time: datetime) -> None:
    print("\n" + "╔" + "═" * 62 + "╗")
    print("║   FORM DATA COLLECTOR ENGINE #10                             ║")
    print("║   Python Business Automation Engine                          ║")
    print("╠" + "═" * 62 + "╣")
    print(f"║   Run time  : {run_time:%Y-%m-%d %H:%M:%S} UTC{' ' * 22}║")
    print(f"║   Mode      : {cfg['mode'].upper():<47}║")
    print(f"║   Dry run   : {'YES ⚠️' if cfg['dry_run'] else 'NO':<47}║")
    print(f"║   Sheet     : {str(cfg['spreadsheet_id'])[:46]:<47}║")
    print("╚" + "═" * 62 + "╝\n")


def _print_footer(success: bool, elapsed: float) -> None:
    status = "✅  SUCCESS" if success else "❌  FAILED"
    print("\n" + "═" * 64)
    print(f"  {status}   |   Elapsed: {elapsed:.2f}s")
    print("═" * 64 + "\n")


def _print_summary_table(summary: dict) -> None:
    print("\n  ┌─────────────────────────────────────────┐")
    print("  │  Collection Summary                      │")
    print("  ├──────────────────────┬──────────────────┤")
    for k, v in summary.items():
        if k == "columns":
            v = f"{len(v)} columns"
        elif k == "checksum":
            v = f"{str(v)[:12]}…"
        print(f"  │ {k:<20} │ {str(v):<16} │")
    print("  └──────────────────────┴──────────────────┘\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ARGUMENT PARSER
# ─────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Form Data Collector Engine #10 — Google Form → Storage Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                    # full run
  python main.py --mode incremental                 # only new data
  python main.py --dry-run                          # preview, no writes
  python main.py --sheet-id 1BxiMV… --sheet-name "Sheet1"
  python main.py --mode incremental --no-excel      # skip Excel output
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="'full' fetches all data; 'incremental' fetches only new rows since last run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline but do not write any files or update state.",
    )
    parser.add_argument(
        "--sheet-id",
        metavar="SPREADSHEET_ID",
        help="Override SPREADSHEET_ID from .env at runtime.",
    )
    parser.add_argument(
        "--sheet-name",
        metavar="SHEET_NAME",
        help="Override GOOGLE_DEFAULT_SHEET_NAME from .env at runtime.",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Disable CSV output even if SAVE_CSV=true in .env.",
    )
    parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Disable Excel output even if SAVE_EXCEL=true in .env.",
    )
    parser.add_argument(
        "--incremental-hours",
        type=int,
        default=None,
        metavar="N",
        help="Look-back window in hours for incremental mode (default: 24).",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()

    # ── apply CLI overrides to CONFIG ─────────────────────────────────────────
    cfg = CONFIG.copy()

    cfg["mode"]    = args.mode
    cfg["dry_run"] = args.dry_run

    if args.sheet_id:
        cfg["spreadsheet_id"] = args.sheet_id
    if args.sheet_name:
        cfg["sheet_name"] = args.sheet_name
    if args.no_csv:
        cfg["save_csv"] = False
    if args.no_excel:
        cfg["save_excel"] = False
    if args.incremental_hours:
        cfg["incremental_hours"] = args.incremental_hours

    # ── guard: must have a real spreadsheet ID ────────────────────────────────
    if cfg["spreadsheet_id"] == "YOUR_SPREADSHEET_ID_HERE":
        logger.error(
            "❌ SPREADSHEET_ID not configured!\n"
            "   Set it in .env:  SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjg…\n"
            "   Or pass it at runtime: python main.py --sheet-id <ID>"
        )
        sys.exit(1)

    # ── run ───────────────────────────────────────────────────────────────────
    success = run_pipeline(cfg)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()