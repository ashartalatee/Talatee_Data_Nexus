"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — CSV Storage                       ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : storage/csv_storage.py
Purpose : Save, append, and manage form data in CSV format.

          Features:
          1. Save DataFrame → CSV (overwrite or append mode)
          2. Incremental append — avoid duplicates on re-run
          3. Auto-create output directory
          4. Versioned backup before overwrite
          5. Schema enforcement — ensure consistent column order
          6. Encoding safe (utf-8-sig for Excel compatibility)
          7. File rotation — split large files by date or row limit
          8. Read back CSV → DataFrame with type hints

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_ENCODING  = "utf-8-sig"   # BOM → Excel opens correctly without garbled chars
DEFAULT_SEPARATOR = ","
MAX_ROWS_DEFAULT  = 100_000       # rotate file after this many rows


# ─────────────────────────────────────────────────────────────────────────────
# RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CSVStorageResult:
    """
    Result of a CSV storage operation.

    Attributes:
        path          : Path to the saved file.
        rows_written  : Number of rows written in this operation.
        total_rows    : Total rows now in the file (including existing).
        mode          : "write" or "append".
        backup_path   : Path to backup file (if backup was created).
        success       : True if operation completed without error.
        error         : Error message if success=False.
    """
    path         : Path
    rows_written : int
    total_rows   : int
    mode         : str
    backup_path  : Optional[Path] = None
    success      : bool           = True
    error        : str            = ""

    @property
    def summary(self) -> dict:
        return {
            "path"        : str(self.path),
            "rows_written": self.rows_written,
            "total_rows"  : self.total_rows,
            "mode"        : self.mode,
            "backup"      : str(self.backup_path) if self.backup_path else None,
            "success"     : self.success,
        }

    def __repr__(self) -> str:
        return (
            f"<CSVStorageResult path='{self.path.name}' "
            f"rows_written={self.rows_written} total={self.total_rows} "
            f"mode={self.mode} success={self.success}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE CSV STORAGE
# ─────────────────────────────────────────────────────────────────────────────

class CSVStorage:
    """
    Production-grade CSV storage handler for form data.

    Args:
        output_path      : Path to the target CSV file.
        encoding         : File encoding. Default "utf-8-sig" (Excel-safe).
        separator        : CSV delimiter. Default ",".
        backup_on_write  : Create a timestamped backup before overwriting. Default True.
        dedup_col        : Column used to detect duplicates on append.
                           If set, rows already in the file won't be appended again.
        max_rows         : Auto-rotate file when row count exceeds this. Default None.
        schema           : Expected column order. If set, output always follows this order.
    """

    def __init__(
        self,
        output_path    : str | Path,
        encoding       : str           = DEFAULT_ENCODING,
        separator      : str           = DEFAULT_SEPARATOR,
        backup_on_write: bool          = True,
        dedup_col      : Optional[str] = None,
        max_rows       : Optional[int] = None,
        schema         : Optional[list[str]] = None,
    ) -> None:
        self.output_path     = Path(output_path)
        self.encoding        = encoding
        self.separator       = separator
        self.backup_on_write = backup_on_write
        self.dedup_col       = dedup_col
        self.max_rows        = max_rows
        self.schema          = schema

        # ensure parent directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    # ── private ───────────────────────────────────────────────────────────────

    def _backup(self) -> Optional[Path]:
        """Create a timestamped backup of the existing file."""
        if not self.output_path.exists():
            return None
        ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.output_path.with_suffix(f".backup_{ts}.csv")
        shutil.copy2(self.output_path, backup_path)
        logger.debug(f"  📦 Backup created → {backup_path.name}")
        return backup_path

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder and fill missing columns per schema."""
        if not self.schema:
            return df
        for col in self.schema:
            if col not in df.columns:
                df[col] = ""
        # reorder: schema columns first, then any extra columns
        extra = [c for c in df.columns if c not in self.schema]
        return df[self.schema + extra]

    def _rotate_if_needed(self) -> None:
        """Rename the current file with a timestamp suffix if it's too large."""
        if not self.max_rows or not self.output_path.exists():
            return
        try:
            existing = pd.read_csv(self.output_path, encoding=self.encoding, nrows=0)
            row_count = sum(1 for _ in open(self.output_path, encoding=self.encoding)) - 1
            if row_count >= self.max_rows:
                ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
                rotated   = self.output_path.with_suffix(f".{ts}.csv")
                shutil.move(str(self.output_path), str(rotated))
                logger.info(f"  🔄 File rotated → {rotated.name} ({row_count} rows)")
        except Exception as exc:
            logger.warning(f"⚠️  Rotation check failed: {exc}")

    def _read_existing(self) -> pd.DataFrame:
        """Read existing CSV safely. Returns empty DataFrame if file doesn't exist."""
        if not self.output_path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(
                self.output_path,
                encoding=self.encoding,
                sep=self.separator,
                dtype=str,        # read everything as string to avoid type conflicts
            )
        except Exception as exc:
            logger.warning(f"⚠️  Could not read existing CSV: {exc}")
            return pd.DataFrame()

    # ── public: save (overwrite) ──────────────────────────────────────────────

    def save(self, df: pd.DataFrame) -> CSVStorageResult:
        """
        Save DataFrame to CSV — overwrites existing file.

        Args:
            df : DataFrame to save.

        Returns:
            CSVStorageResult
        """
        if df.empty:
            logger.warning("⚠️  CSVStorage.save() called with empty DataFrame.")
            return CSVStorageResult(
                path=self.output_path, rows_written=0, total_rows=0,
                mode="write", success=True,
            )

        backup_path = None
        try:
            df = self._enforce_schema(df.copy())

            if self.backup_on_write:
                backup_path = self._backup()

            df.to_csv(
                self.output_path,
                index=False,
                encoding=self.encoding,
                sep=self.separator,
            )

            logger.info(f"💾 CSV saved → {self.output_path} ({len(df)} rows)")

            return CSVStorageResult(
                path=self.output_path, rows_written=len(df),
                total_rows=len(df), mode="write",
                backup_path=backup_path, success=True,
            )

        except Exception as exc:
            logger.error(f"❌ CSV save failed: {exc}")
            return CSVStorageResult(
                path=self.output_path, rows_written=0, total_rows=0,
                mode="write", success=False, error=str(exc),
            )

    # ── public: append ────────────────────────────────────────────────────────

    def append(self, df: pd.DataFrame) -> CSVStorageResult:
        """
        Append new rows to existing CSV.
        If dedup_col is set, skips rows already in the file.

        Args:
            df : DataFrame with new rows to append.

        Returns:
            CSVStorageResult
        """
        if df.empty:
            logger.warning("⚠️  CSVStorage.append() called with empty DataFrame.")
            return CSVStorageResult(
                path=self.output_path, rows_written=0, total_rows=0,
                mode="append", success=True,
            )

        try:
            self._rotate_if_needed()
            df          = self._enforce_schema(df.copy())
            existing_df = self._read_existing()
            new_rows    = df

            # ── deduplication ─────────────────────────────────────────────────
            if self.dedup_col and not existing_df.empty and self.dedup_col in existing_df.columns:
                existing_keys = set(existing_df[self.dedup_col].astype(str).str.lower())
                before        = len(new_rows)
                new_rows      = new_rows[
                    ~new_rows[self.dedup_col].astype(str).str.lower().isin(existing_keys)
                ]
                skipped = before - len(new_rows)
                if skipped:
                    logger.info(f"  ⏭️  Skipped {skipped} duplicate row(s) (dedup_col='{self.dedup_col}')")

            if new_rows.empty:
                logger.info("  ✅ No new rows to append — all already exist.")
                total = len(existing_df)
                return CSVStorageResult(
                    path=self.output_path, rows_written=0,
                    total_rows=total, mode="append", success=True,
                )

            # ── write header only if file doesn't exist ───────────────────────
            write_header = not self.output_path.exists()
            new_rows.to_csv(
                self.output_path,
                mode="a",
                header=write_header,
                index=False,
                encoding=self.encoding,
                sep=self.separator,
            )

            total = len(existing_df) + len(new_rows)
            logger.info(
                f"💾 CSV appended → {self.output_path} "
                f"(+{len(new_rows)} rows | total={total})"
            )

            return CSVStorageResult(
                path=self.output_path, rows_written=len(new_rows),
                total_rows=total, mode="append", success=True,
            )

        except Exception as exc:
            logger.error(f"❌ CSV append failed: {exc}")
            return CSVStorageResult(
                path=self.output_path, rows_written=0, total_rows=0,
                mode="append", success=False, error=str(exc),
            )

    # ── public: read ──────────────────────────────────────────────────────────

    def read(self, dtype: Optional[dict] = None) -> pd.DataFrame:
        """
        Read CSV back into a DataFrame.

        Args:
            dtype : Optional column type mapping for pd.read_csv.

        Returns:
            DataFrame (empty if file doesn't exist).
        """
        if not self.output_path.exists():
            logger.warning(f"⚠️  File not found: {self.output_path}")
            return pd.DataFrame()

        df = pd.read_csv(
            self.output_path,
            encoding=self.encoding,
            sep=self.separator,
            dtype=dtype,
        )
        logger.info(f"📂 CSV loaded ← {self.output_path} ({len(df)} rows)")
        return df

    # ── public: info ──────────────────────────────────────────────────────────

    def info(self) -> dict:
        """Return metadata about the current CSV file."""
        if not self.output_path.exists():
            return {"exists": False, "path": str(self.output_path)}

        stat = self.output_path.stat()
        df   = self._read_existing()
        return {
            "exists"      : True,
            "path"        : str(self.output_path),
            "rows"        : len(df),
            "columns"     : list(df.columns),
            "size_kb"     : round(stat.st_size / 1024, 2),
            "modified"    : datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def save_csv(
    df         : pd.DataFrame,
    path       : str | Path,
    mode       : str           = "write",
    dedup_col  : Optional[str] = None,
    schema     : Optional[list[str]] = None,
) -> CSVStorageResult:
    """
    One-liner CSV save. Used by main.py and data_router handlers.

    Args:
        df        : DataFrame to save.
        path      : Output file path.
        mode      : "write" (overwrite) or "append".
        dedup_col : Column for dedup on append.
        schema    : Column order to enforce.

    Example:
        from storage.csv_storage import save_csv
        save_csv(df, "outputs/form_data.csv", mode="append", dedup_col="email")
    """
    storage = CSVStorage(path, dedup_col=dedup_col, schema=schema)
    return storage.save(df) if mode == "write" else storage.append(df)


def read_csv(path: str | Path, dtype: Optional[dict] = None) -> pd.DataFrame:
    """One-liner CSV read."""
    return CSVStorage(path).read(dtype=dtype)


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile, os

    print("=" * 65)
    print("  CSV Storage — Quick Test")
    print("=" * 65)

    # ── sample data ───────────────────────────────────────────────────────────
    df1 = pd.DataFrame({
        "email"  : ["budi@gmail.com", "siti@yahoo.com"],
        "nama"   : ["Budi Santoso", "Siti Rahayu"],
        "kota"   : ["Surabaya", "Jakarta"],
        "total"  : [1_500_000, 2_500_000],
    })

    df2 = pd.DataFrame({
        "email"  : ["siti@yahoo.com", "andi@outlook.com"],   # siti is duplicate
        "nama"   : ["Siti Rahayu", "Andi Wijaya"],
        "kota"   : ["Jakarta", "Bandung"],
        "total"  : [2_500_000, 750_000],
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test_form_data.csv"
        storage = CSVStorage(path, dedup_col="email", backup_on_write=True)

        # ── TEST 1: Save ──────────────────────────────────────────────────────
        print("\n  TEST 1: save() — overwrite")
        r1 = storage.save(df1)
        print(f"  Result : {r1}")
        print(f"  Content:\n{storage.read().to_string(index=False)}")

        # ── TEST 2: Append with dedup ─────────────────────────────────────────
        print("\n  TEST 2: append() — dedup on email")
        r2 = storage.append(df2)
        print(f"  Result : {r2}")
        print(f"  Content:\n{storage.read().to_string(index=False)}")
        assert r2.rows_written == 1, "Should only append 1 new row (siti is duplicate)"

        # ── TEST 3: Info ──────────────────────────────────────────────────────
        print("\n  TEST 3: info()")
        info = storage.info()
        for k, v in info.items():
            if k != "columns":
                print(f"  {k:<15}: {v}")
        print(f"  {'columns':<15}: {info['columns']}")

        # ── TEST 4: Convenience function ──────────────────────────────────────
        print("\n  TEST 4: save_csv() one-liner")
        path2 = Path(tmpdir) / "quick_save.csv"
        r4    = save_csv(df1, path2)
        print(f"  Result : {r4}")
        assert r4.success is True

        # ── TEST 5: Read back ─────────────────────────────────────────────────
        print("\n  TEST 5: read_csv() one-liner")
        df_back = read_csv(path2)
        assert len(df_back) == len(df1)
        print(f"  Rows read back: {len(df_back)} ✅")

    print("\n✅ All CSV storage tests passed!")