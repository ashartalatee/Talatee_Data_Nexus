"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Duplicate Checker                 ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : validators/duplicate_checker.py
Purpose : Detect duplicate rows in collected form data.

          Supports 3 strategies:
          1. EXACT     — exact match on one or more key columns
          2. FUZZY     — similarity-based match (catch typos like
                         "budi@gmail.com" vs "bud1@gmail.com")
          3. COMPOSITE — combine multiple columns as a composite key
                         e.g. (name + phone) to catch same person diff email

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal, Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────────────────────
Strategy = Literal["exact", "fuzzy", "composite"]


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DuplicateCheckResult:
    """
    Result of a duplicate check on a DataFrame.

    Attributes:
        total_rows      : Total rows input.
        duplicate_count : Number of duplicate rows found.
        unique_count    : Rows that are unique.
        duplicate_mask  : Boolean Series (True = duplicate).
        duplicate_df    : Sub-DataFrame of duplicate rows only.
        strategy        : Strategy used.
        key_columns     : Columns used as duplicate key.
        report          : Human-readable summary dict.
    """
    total_rows      : int
    duplicate_count : int
    unique_count    : int
    duplicate_mask  : pd.Series
    duplicate_df    : pd.DataFrame
    strategy        : str
    key_columns     : list[str]
    report          : dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.report = {
            "strategy"       : self.strategy,
            "key_columns"    : self.key_columns,
            "total_rows"     : self.total_rows,
            "unique_rows"    : self.unique_count,
            "duplicate_rows" : self.duplicate_count,
            "duplicate_pct"  : round(self.duplicate_count / max(self.total_rows, 1) * 100, 2),
        }

    @property
    def has_duplicates(self) -> bool:
        return self.duplicate_count > 0

    def __repr__(self) -> str:
        return (
            f"<DuplicateCheckResult strategy={self.strategy!r} "
            f"duplicates={self.duplicate_count}/{self.total_rows}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE CHECKER CLASS
# ─────────────────────────────────────────────────────────────────────────────

class DuplicateChecker:
    """
    Multi-strategy duplicate checker for form data.

    Args:
        strategy     : "exact" | "fuzzy" | "composite". Default "exact".
        key_col      : Primary column to check (email, phone, etc.).
        extra_cols   : Additional columns for "composite" strategy.
        keep         : Which duplicate to KEEP — "first" | "last" | False.
                       "first" = keep first occurrence, mark rest as dupe.
                       "last"  = keep last occurrence, mark rest as dupe.
                       False   = mark ALL occurrences as dupe.
        fuzzy_threshold: Similarity ratio 0-100 for fuzzy strategy.
                         Default 90 (90% similar = duplicate).
        normalize    : Normalize values before comparison
                       (lowercase, strip spaces). Default True.
    """

    def __init__(
        self,
        strategy         : Strategy = "exact",
        key_col          : str      = "Email",
        extra_cols       : Optional[list[str]] = None,
        keep             : Literal["first", "last"] | bool = "first",
        fuzzy_threshold  : int  = 90,
        normalize        : bool = True,
    ) -> None:
        self.strategy        = strategy
        self.key_col         = key_col
        self.extra_cols      = extra_cols or []
        self.keep            = keep
        self.fuzzy_threshold = fuzzy_threshold
        self.normalize       = normalize

    # ── private helpers ───────────────────────────────────────────────────────

    def _norm(self, series: pd.Series) -> pd.Series:
        """Normalize: lowercase + strip whitespace."""
        return series.astype(str).str.lower().str.strip()

    def _composite_key(self, df: pd.DataFrame, cols: list[str]) -> pd.Series:
        """Build a single composite key column from multiple columns."""
        combined = df[cols].astype(str).agg("|".join, axis=1)
        if self.normalize:
            combined = combined.str.lower().str.strip()
        # hash it so it's always a fixed-length string
        return combined.apply(
            lambda v: hashlib.md5(v.encode()).hexdigest()
        )

    def _fuzzy_duplicates(self, series: pd.Series) -> pd.Series:
        """
        Detect fuzzy duplicates using SequenceMatcher.
        O(n²) — only use on small datasets (<5000 rows).
        Returns a boolean mask (True = duplicate).
        """
        try:
            from difflib import SequenceMatcher
        except ImportError:
            logger.warning("difflib not available — falling back to exact match.")
            return series.duplicated(keep=self.keep)  # type: ignore

        values = series.tolist()
        n      = len(values)
        is_dup = [False] * n
        seen   : list[int] = []   # indices of "first seen" values

        for i in range(n):
            matched = False
            for j in seen:
                ratio = SequenceMatcher(
                    None, str(values[i]), str(values[j])
                ).ratio() * 100
                if ratio >= self.fuzzy_threshold:
                    matched = True
                    break
            if matched:
                is_dup[i] = True
            else:
                seen.append(i)

        return pd.Series(is_dup, index=series.index)

    # ── private: strategy dispatchers ────────────────────────────────────────

    def _check_exact(self, df: pd.DataFrame) -> pd.Series:
        col = self.key_col
        if col not in df.columns:
            logger.warning(f"⚠️  Column '{col}' not found — skipping duplicate check.")
            return pd.Series([False] * len(df), index=df.index)

        series = self._norm(df[col]) if self.normalize else df[col]
        return series.duplicated(keep=self.keep)  # type: ignore

    def _check_fuzzy(self, df: pd.DataFrame) -> pd.Series:
        col = self.key_col
        if col not in df.columns:
            logger.warning(f"⚠️  Column '{col}' not found — skipping duplicate check.")
            return pd.Series([False] * len(df), index=df.index)

        if len(df) > 5000:
            logger.warning(
                f"⚠️  Fuzzy check on {len(df)} rows is slow (O(n²)). "
                "Consider using 'exact' strategy for large datasets."
            )

        series = self._norm(df[col]) if self.normalize else df[col]
        return self._fuzzy_duplicates(series)

    def _check_composite(self, df: pd.DataFrame) -> pd.Series:
        all_cols = [self.key_col] + self.extra_cols
        missing  = [c for c in all_cols if c not in df.columns]
        if missing:
            logger.warning(f"⚠️  Missing columns for composite key: {missing}")
            all_cols = [c for c in all_cols if c in df.columns]
        if not all_cols:
            return pd.Series([False] * len(df), index=df.index)

        composite = self._composite_key(df, all_cols)
        return composite.duplicated(keep=self.keep)  # type: ignore

    # ── public: check ─────────────────────────────────────────────────────────

    def check(self, df: pd.DataFrame) -> DuplicateCheckResult:
        """
        Run duplicate detection on a DataFrame.

        Args:
            df : Input DataFrame (not modified in-place).

        Returns:
            DuplicateCheckResult with mask, duplicate rows, and report.
        """
        if df.empty:
            logger.warning("⚠️  Empty DataFrame — skipping duplicate check.")
            empty_mask = pd.Series(dtype=bool)
            return DuplicateCheckResult(
                total_rows=0, duplicate_count=0, unique_count=0,
                duplicate_mask=empty_mask, duplicate_df=pd.DataFrame(),
                strategy=self.strategy, key_columns=[self.key_col],
            )

        logger.info(
            f"🔍 Duplicate check | strategy={self.strategy} | "
            f"key='{self.key_col}' | rows={len(df)}"
        )

        dispatch = {
            "exact"    : self._check_exact,
            "fuzzy"    : self._check_fuzzy,
            "composite": self._check_composite,
        }

        if self.strategy not in dispatch:
            raise ValueError(
                f"Unknown strategy '{self.strategy}'. "
                f"Choose from: {list(dispatch.keys())}"
            )

        mask    = dispatch[self.strategy](df)
        dup_df  = df[mask].copy()
        n_dups  = mask.sum()
        n_uniq  = len(df) - n_dups

        key_cols = (
            [self.key_col] + self.extra_cols
            if self.strategy == "composite"
            else [self.key_col]
        )

        result = DuplicateCheckResult(
            total_rows      = len(df),
            duplicate_count = int(n_dups),
            unique_count    = int(n_uniq),
            duplicate_mask  = mask,
            duplicate_df    = dup_df,
            strategy        = self.strategy,
            key_columns     = key_cols,
        )

        if result.has_duplicates:
            logger.warning(
                f"🔁 Found {n_dups} duplicate(s) "
                f"({result.report['duplicate_pct']}%) "
                f"out of {len(df)} rows"
            )
        else:
            logger.info(f"✅ No duplicates found in {len(df)} rows")

        return result

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convenience method — return DataFrame with duplicates removed.

        Args:
            df : Input DataFrame.

        Returns:
            Clean DataFrame without duplicate rows.
        """
        result = self.check(df)
        clean  = df[~result.duplicate_mask].reset_index(drop=True)
        logger.info(
            f"🧹 Removed {result.duplicate_count} duplicate(s) → "
            f"{len(clean)} rows remaining"
        )
        return clean


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION  (used by main.py step_validate)
# ─────────────────────────────────────────────────────────────────────────────

def find_duplicates(
    df      : pd.DataFrame,
    key_col : str      = "Email",
    strategy: Strategy = "exact",
    keep    : Literal["first", "last"] | bool = "first",
) -> pd.Series:
    """
    Return a boolean mask where True = duplicate row.

    Used directly by main.py:
        from validators.duplicate_checker import find_duplicates
        dup_mask = find_duplicates(df, key_col="Email")
        df.loc[dup_mask, "_invalid_reason"] += "duplicate;"

    Args:
        df       : DataFrame to check.
        key_col  : Column to use as duplicate key.
        strategy : "exact" | "fuzzy" | "composite".
        keep     : Which occurrence to keep ("first" | "last").

    Returns:
        pd.Series of bool (same index as df).
    """
    checker = DuplicateChecker(strategy=strategy, key_col=key_col, keep=keep)
    return checker.check(df).duplicate_mask


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Duplicate Checker — Quick Test")
    print("=" * 60)

    sample_data = {
        "Timestamp": [
            "2024-06-01 08:00", "2024-06-01 09:00", "2024-06-01 10:00",
            "2024-06-01 11:00", "2024-06-01 12:00", "2024-06-01 13:00",
        ],
        "Email": [
            "budi@gmail.com",
            "siti@yahoo.com",
            "budi@gmail.com",       # ← exact duplicate of row 0
            "BUDI@GMAIL.COM",       # ← case-insensitive duplicate of row 0
            "andi@outlook.com",
            "siti@yahoo.com",       # ← exact duplicate of row 1
        ],
        "Name": ["Budi", "Siti", "Budi", "Budi", "Andi", "Siti"],
        "Phone": [
            "08123456789", "08556789012", "08123456789",
            "08123456789", "08778901234", "08556789012",
        ],
    }
    df = pd.DataFrame(sample_data)

    print("\n📋 Input DataFrame:")
    print(df.to_string(index=True))

    # ── Test 1: Exact ─────────────────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("  TEST 1: Exact Strategy (key=Email)")
    checker = DuplicateChecker(strategy="exact", key_col="Email")
    result  = checker.check(df)
    print(f"  Result: {result}")
    print(f"  Report: {result.report}")
    print(f"\n  Duplicate rows:\n{result.duplicate_df[['Email','Name']].to_string()}")

    # ── Test 2: Composite ─────────────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("  TEST 2: Composite Strategy (key=Name + Phone)")
    checker2 = DuplicateChecker(
        strategy="composite", key_col="Name", extra_cols=["Phone"]
    )
    result2 = checker2.check(df)
    print(f"  Result: {result2}")

    # ── Test 3: Fuzzy ─────────────────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("  TEST 3: Fuzzy Strategy (threshold=90)")
    fuzzy_data = {
        "Email": [
            "budi@gmail.com",
            "budi@gmai1.com",   # typo — 1 instead of l
            "siti@yahoo.com",
            "s1ti@yahoo.com",   # typo
        ]
    }
    df_fuzzy = pd.DataFrame(fuzzy_data)
    checker3 = DuplicateChecker(strategy="fuzzy", key_col="Email", fuzzy_threshold=90)
    result3  = checker3.check(df_fuzzy)
    print(f"  Result: {result3}")
    print(f"  Duplicates found: {result3.duplicate_df['Email'].tolist()}")

    # ── Test 4: remove_duplicates ─────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("  TEST 4: remove_duplicates()")
    clean_df = DuplicateChecker(strategy="exact", key_col="Email").remove_duplicates(df)
    print(f"  Before: {len(df)} rows → After: {len(clean_df)} rows")
    print(clean_df[["Email", "Name"]].to_string(index=False))