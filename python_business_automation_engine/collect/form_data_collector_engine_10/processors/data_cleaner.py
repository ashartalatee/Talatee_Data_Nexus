"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Data Cleaner                      ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : processors/data_cleaner.py
Purpose : Clean raw form data before transformation and storage.

          Cleaning operations:
          1. Strip whitespace (leading/trailing)
          2. Normalize encoding (fix mojibake, special chars)
          3. Standardize case (title, upper, lower per column type)
          4. Remove special characters / injection patterns
          5. Normalize whitespace (double spaces → single)
          6. Fix common typos in categorical fields
          7. Drop fully empty rows/columns
          8. Trim text to max length
          9. Normalize boolean-like fields (ya/tidak, yes/no → True/False)
         10. Add cleaning audit columns (_cleaned, _cleaning_notes)

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────────────────────
CaseMode  = Literal["title", "upper", "lower", "none"]
BoolStyle = Literal["bool", "int", "yes_no", "ya_tidak"]


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Characters that should never appear in form data (injection / XSS patterns)
_DANGEROUS_PATTERNS = re.compile(
    r"(<script.*?>.*?</script>|<.*?>|--|;DROP|;SELECT|UNION SELECT|"
    r"javascript:|vbscript:|onload=|onerror=)",
    re.IGNORECASE | re.DOTALL,
)

# Multiple whitespace → single space
_MULTI_SPACE = re.compile(r"\s{2,}")

# Common boolean truthy/falsy values
_BOOL_TRUE  = {"ya", "yes", "true", "1", "benar", "iya", "ok", "aktif", "active"}
_BOOL_FALSE = {"tidak", "no", "false", "0", "salah", "bukan", "nonaktif", "inactive"}

# Common categorical typo corrections (can be extended per project)
_TYPO_MAP: dict[str, str] = {
    "jakarta selatan" : "Jakarta Selatan",
    "jkt"             : "Jakarta",
    "sby"             : "Surabaya",
    "bdg"             : "Bandung",
    "yog"             : "Yogyakarta",
    "jogja"           : "Yogyakarta",
    "bali"            : "Bali",
    "indo"            : "Indonesia",
    "id"              : "Indonesia",
    "wni"             : "WNI",
    "wna"             : "WNA",
}


# ─────────────────────────────────────────────────────────────────────────────
# RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CleaningResult:
    """
    Result of a cleaning operation on a DataFrame.

    Attributes:
        dataframe       : Cleaned DataFrame.
        original_rows   : Row count before cleaning.
        original_cols   : Column count before cleaning.
        rows_dropped    : Rows removed (fully empty).
        cols_dropped    : Columns removed (fully empty).
        cells_modified  : Estimated number of cells that changed.
        operations      : List of operations applied.
    """
    dataframe      : pd.DataFrame
    original_rows  : int
    original_cols  : int
    rows_dropped   : int
    cols_dropped   : int
    cells_modified : int
    operations     : list[str] = field(default_factory=list)

    @property
    def summary(self) -> dict:
        return {
            "original_rows"  : self.original_rows,
            "original_cols"  : self.original_cols,
            "rows_dropped"   : self.rows_dropped,
            "cols_dropped"   : self.cols_dropped,
            "final_rows"     : len(self.dataframe),
            "final_cols"     : len(self.dataframe.columns),
            "cells_modified" : self.cells_modified,
            "operations"     : self.operations,
        }

    def __repr__(self) -> str:
        return (
            f"<CleaningResult rows={len(self.dataframe)} "
            f"dropped={self.rows_dropped} modified≈{self.cells_modified}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE CLEANER
# ─────────────────────────────────────────────────────────────────────────────

class DataCleaner:
    """
    Comprehensive data cleaner for raw form responses.

    Args:
        strip_whitespace    : Remove leading/trailing spaces. Default True.
        normalize_unicode   : Fix encoding issues (mojibake). Default True.
        remove_dangerous    : Strip HTML/SQL injection patterns. Default True.
        normalize_spaces    : Collapse multiple spaces to one. Default True.
        drop_empty_rows     : Remove rows where ALL values are empty. Default True.
        drop_empty_cols     : Remove columns where ALL values are empty. Default False.
        max_text_length     : Truncate text fields to this length. None = no limit.
        case_config         : Dict mapping column name → CaseMode.
                              e.g. {"Nama": "title", "Email": "lower"}
        bool_columns        : List of columns to normalize as boolean.
        bool_style          : Output format for bool columns.
        fix_typos           : Apply typo correction map. Default True.
        add_audit_cols      : Add _cleaned_at metadata column. Default True.
    """

    def __init__(
        self,
        strip_whitespace : bool              = True,
        normalize_unicode: bool              = True,
        remove_dangerous : bool              = True,
        normalize_spaces : bool              = True,
        drop_empty_rows  : bool              = True,
        drop_empty_cols  : bool              = False,
        max_text_length  : Optional[int]     = None,
        case_config      : Optional[dict[str, CaseMode]] = None,
        bool_columns     : Optional[list[str]]           = None,
        bool_style       : BoolStyle         = "ya_tidak",
        fix_typos        : bool              = True,
        add_audit_cols   : bool              = True,
    ) -> None:
        self.strip_whitespace  = strip_whitespace
        self.normalize_unicode = normalize_unicode
        self.remove_dangerous  = remove_dangerous
        self.normalize_spaces  = normalize_spaces
        self.drop_empty_rows   = drop_empty_rows
        self.drop_empty_cols   = drop_empty_cols
        self.max_text_length   = max_text_length
        self.case_config       = case_config or {}
        self.bool_columns      = bool_columns or []
        self.bool_style        = bool_style
        self.fix_typos         = fix_typos
        self.add_audit_cols    = add_audit_cols

    # ── private: single cell operations ──────────────────────────────────────

    def _clean_str(self, value: Any) -> str:
        """Apply all string-level cleaning to a single cell value."""
        if pd.isna(value) or value is None:
            return ""
        s = str(value)

        if self.normalize_unicode:
            # NFC normalization → fix mojibake / composed characters
            s = unicodedata.normalize("NFC", s)

        if self.strip_whitespace:
            s = s.strip()

        if self.remove_dangerous:
            s = _DANGEROUS_PATTERNS.sub("", s)

        if self.normalize_spaces:
            s = _MULTI_SPACE.sub(" ", s).strip()

        if self.max_text_length and len(s) > self.max_text_length:
            s = s[: self.max_text_length].rstrip()

        return s

    def _apply_case(self, s: str, mode: CaseMode) -> str:
        if not s or mode == "none":
            return s
        if mode == "title":
            return s.title()
        if mode == "upper":
            return s.upper()
        if mode == "lower":
            return s.lower()
        return s

    def _normalize_bool(self, value: Any) -> Any:
        """Normalize boolean-like strings to configured output format."""
        if pd.isna(value) or value is None:
            return value
        s = str(value).strip().lower()

        is_true  = s in _BOOL_TRUE
        is_false = s in _BOOL_FALSE

        if not is_true and not is_false:
            return value   # unknown — leave as-is

        if self.bool_style == "bool":
            return is_true
        if self.bool_style == "int":
            return 1 if is_true else 0
        if self.bool_style == "yes_no":
            return "Yes" if is_true else "No"
        if self.bool_style == "ya_tidak":
            return "Ya" if is_true else "Tidak"
        return value

    def _fix_typo(self, s: str) -> str:
        """Apply typo correction map (case-insensitive lookup)."""
        key = s.strip().lower()
        return _TYPO_MAP.get(key, s)

    # ── public: clean column names ────────────────────────────────────────────

    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names:
        - Strip whitespace
        - Collapse multiple spaces
        - Keep original casing (Google Forms uses natural language headers)
        """
        df.columns = [
            _MULTI_SPACE.sub(" ", col.strip())
            for col in df.columns
        ]
        return df

    # ── public: main clean ────────────────────────────────────────────────────

    def clean(self, df: pd.DataFrame) -> CleaningResult:
        """
        Apply full cleaning pipeline to a DataFrame.

        Args:
            df : Raw input DataFrame from collector.

        Returns:
            CleaningResult with cleaned DataFrame and audit info.
        """
        if df.empty:
            logger.warning("⚠️  Empty DataFrame passed to DataCleaner.")
            return CleaningResult(
                dataframe=df, original_rows=0, original_cols=0,
                rows_dropped=0, cols_dropped=0, cells_modified=0,
            )

        logger.info(f"🧹 Starting data cleaning | {len(df)} rows × {len(df.columns)} cols")
        df = df.copy()
        original_rows = len(df)
        original_cols = len(df.columns)
        operations: list[str] = []
        cells_modified = 0

        # ── 1. Clean column names ─────────────────────────────────────────────
        df = self.clean_column_names(df)
        operations.append("clean_column_names")

        # ── 2. Drop fully empty rows ──────────────────────────────────────────
        rows_before = len(df)
        if self.drop_empty_rows:
            # exclude metadata columns from empty check
            data_cols = [c for c in df.columns if not c.startswith("_")]
            df.replace("", pd.NA, inplace=True)
            df.dropna(subset=data_cols, how="all", inplace=True)
            df.reset_index(drop=True, inplace=True)
            rows_dropped = rows_before - len(df)
            if rows_dropped:
                logger.info(f"  🗑️  Dropped {rows_dropped} fully empty row(s)")
            operations.append(f"drop_empty_rows (removed {rows_before - len(df)})")
        rows_dropped = rows_before - len(df)

        # ── 3. Drop fully empty columns ───────────────────────────────────────
        cols_before = len(df.columns)
        if self.drop_empty_cols:
            df.dropna(axis=1, how="all", inplace=True)
            cols_dropped_n = cols_before - len(df.columns)
            if cols_dropped_n:
                logger.info(f"  🗑️  Dropped {cols_dropped_n} fully empty column(s)")
            operations.append(f"drop_empty_cols (removed {cols_dropped_n})")
        cols_dropped = cols_before - len(df.columns)

        # ── 4. String cleaning per cell ───────────────────────────────────────
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            if col.startswith("_"):   # skip metadata cols
                continue
            original = df[col].copy()
            df[col]  = df[col].apply(self._clean_str)
            changed  = (df[col] != original.fillna("")).sum()
            cells_modified += int(changed)

        operations.append("string_cleaning (strip, unicode, dangerous, spaces)")

        # ── 5. Case normalization ─────────────────────────────────────────────
        for col, mode in self.case_config.items():
            if col in df.columns and mode != "none":
                df[col] = df[col].apply(lambda s: self._apply_case(str(s), mode))
                operations.append(f"case_{mode}: {col}")

        # ── 6. Boolean normalization ──────────────────────────────────────────
        for col in self.bool_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._normalize_bool)
                operations.append(f"normalize_bool: {col}")

        # ── 7. Typo correction ────────────────────────────────────────────────
        if self.fix_typos:
            for col in str_cols:
                if col.startswith("_"):
                    continue
                df[col] = df[col].apply(
                    lambda s: self._fix_typo(s) if isinstance(s, str) else s
                )
            operations.append("fix_typos")

        # ── 8. Audit column ───────────────────────────────────────────────────
        if self.add_audit_cols:
            from datetime import datetime
            df["_cleaned_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            operations.append("add_audit_col (_cleaned_at)")

        logger.info(
            f"✅ Cleaning complete | "
            f"{len(df)} rows remaining | "
            f"~{cells_modified} cells modified | "
            f"{len(operations)} operations"
        )

        return CleaningResult(
            dataframe     = df,
            original_rows = original_rows,
            original_cols = original_cols,
            rows_dropped  = rows_dropped,
            cols_dropped  = cols_dropped,
            cells_modified= cells_modified,
            operations    = operations,
        )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def clean_dataframe(
    df              : pd.DataFrame,
    case_config     : Optional[dict[str, CaseMode]] = None,
    bool_columns    : Optional[list[str]]           = None,
    max_text_length : Optional[int]                 = None,
) -> pd.DataFrame:
    """
    Quick one-liner cleaning. Returns cleaned DataFrame directly.

    Used in main.py or notebooks:
        from processors.data_cleaner import clean_dataframe
        df_clean = clean_dataframe(df, case_config={"Nama": "title", "Email": "lower"})
    """
    cleaner = DataCleaner(
        case_config=case_config,
        bool_columns=bool_columns,
        max_text_length=max_text_length,
    )
    return cleaner.clean(df).dataframe


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Data Cleaner — Quick Test")
    print("=" * 65)

    raw = pd.DataFrame({
        "Timestamp"    : ["2024-06-01 08:00", "2024-06-01 09:00", "", "2024-06-01 11:00"],
        "Nama Lengkap" : ["  budi santoso  ", "SITI RAHAYU", "", "  andi   wijaya  "],
        "Email Address": ["Budi@Gmail.COM ", " siti@YAHOO.com", "", "andi@outlook.com"],
        "Kota"         : ["sby", "jkt", "", "bdg"],
        "Aktif"        : ["Ya", "tidak", "", "YES"],
        "Catatan"      : ["<script>alert(1)</script>normal text", "  ok  ", "", "biasa aja"],
        "_source"      : ["google_form", "google_form", "google_form", "google_form"],
    })

    print("\n📋 Raw DataFrame:")
    print(raw.to_string(index=True))

    cleaner = DataCleaner(
        case_config  = {"Nama Lengkap": "title", "Email Address": "lower", "Kota": "title"},
        bool_columns = ["Aktif"],
        bool_style   = "ya_tidak",
        drop_empty_rows = True,
        fix_typos    = True,
    )

    result = cleaner.clean(raw)

    print("\n✅ Cleaned DataFrame:")
    print(result.dataframe.to_string(index=True))

    print("\n📊 Cleaning Summary:")
    for k, v in result.summary.items():
        if k != "operations":
            print(f"  {k:<20}: {v}")
    print(f"  {'operations':<20}:")
    for op in result.summary["operations"]:
        print(f"    • {op}")