"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Data Transformer                  ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : processors/data_transformer.py
Purpose : Transform cleaned form data into analysis-ready format.

          Transformations:
          1. Field mapping    — rename columns to standard schema names
          2. Type casting     — string → int/float/datetime/bool
          3. Value mapping    — categorical recoding (e.g. "S1" → "Bachelor")
          4. Column splitting — "Nama Lengkap" → "Nama Depan" + "Nama Belakang"
          5. Column merging   — combine multiple fields into one
          6. Derived columns  — compute new columns from existing ones
          7. Pivot/unpivot    — reshape multi-answer columns
          8. Schema enforce   — reorder + select only needed columns

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TransformResult:
    """
    Result of a transformation pipeline.

    Attributes:
        dataframe    : Transformed DataFrame.
        original_cols: Column names before transformation.
        final_cols   : Column names after transformation.
        operations   : Ordered list of operations applied.
        warnings     : Non-fatal issues encountered.
    """
    dataframe    : pd.DataFrame
    original_cols: list[str]
    final_cols   : list[str]
    operations   : list[str] = field(default_factory=list)
    warnings     : list[str] = field(default_factory=list)

    @property
    def summary(self) -> dict:
        return {
            "original_cols": len(self.original_cols),
            "final_cols"   : len(self.final_cols),
            "final_rows"   : len(self.dataframe),
            "operations"   : self.operations,
            "warnings"     : self.warnings,
        }

    def __repr__(self) -> str:
        return (
            f"<TransformResult rows={len(self.dataframe)} "
            f"cols={len(self.final_cols)} ops={len(self.operations)}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE TRANSFORMER
# ─────────────────────────────────────────────────────────────────────────────

class DataTransformer:
    """
    Chainable data transformer for form responses.

    Each method returns self for fluent/method-chaining API:
        result = (
            DataTransformer(df)
            .rename_columns({"Email Address": "email", "Nama Lengkap": "nama"})
            .cast_types({"umur": "int", "tanggal": "datetime"})
            .map_values({"pendidikan": {"S1": "Bachelor", "S2": "Master"}})
            .split_column("nama", ["nama_depan", "nama_belakang"], sep=" ", maxsplit=1)
            .add_derived("nama_upper", lambda row: row["nama"].upper())
            .select_columns(["nama", "email", "umur"])
            .build()
        )
        df_clean = result.dataframe

    Args:
        df : Cleaned DataFrame from DataCleaner.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df         = df.copy()
        self._operations : list[str] = []
        self._warnings   : list[str] = []
        self._original   = list(df.columns)

    # ── 1. Field Mapping (rename) ─────────────────────────────────────────────

    def rename_columns(self, mapping: dict[str, str]) -> "DataTransformer":
        """
        Rename columns using a mapping dict.

        Args:
            mapping : {old_name: new_name}

        Example:
            .rename_columns({
                "Email Address"  : "email",
                "Nama Lengkap"   : "nama",
                "Nomor HP"       : "phone",
                "Tanggal Lahir"  : "birth_date",
            })
        """
        # only rename columns that exist
        valid_map = {k: v for k, v in mapping.items() if k in self._df.columns}
        missing   = [k for k in mapping if k not in self._df.columns]

        if missing:
            self._warnings.append(f"rename_columns: columns not found → {missing}")
            logger.warning(f"⚠️  rename_columns: not found → {missing}")

        if valid_map:
            self._df.rename(columns=valid_map, inplace=True)
            self._operations.append(f"rename_columns ({len(valid_map)} columns)")
            logger.info(f"  🔄 Renamed {len(valid_map)} column(s): {valid_map}")

        return self

    # ── 2. Type Casting ───────────────────────────────────────────────────────

    def cast_types(self, type_map: dict[str, str]) -> "DataTransformer":
        """
        Cast columns to specified types.

        Supported types:
            "int"      → pd.Int64Dtype() (nullable integer)
            "float"    → float64
            "str"      → string
            "bool"     → boolean
            "datetime" → datetime64 (auto-detect format)
            "date"     → date string normalized to YYYY-MM-DD
            "category" → pandas Categorical (memory efficient)

        Args:
            type_map : {column_name: type_string}

        Example:
            .cast_types({
                "umur"    : "int",
                "harga"   : "float",
                "aktif"   : "bool",
                "tanggal" : "datetime",
            })
        """
        for col, dtype in type_map.items():
            if col not in self._df.columns:
                self._warnings.append(f"cast_types: '{col}' not found")
                continue
            try:
                if dtype == "int":
                    self._df[col] = pd.to_numeric(
                        self._df[col].replace("", pd.NA), errors="coerce"
                    ).astype(pd.Int64Dtype())

                elif dtype == "float":
                    # handle European decimal format
                    self._df[col] = self._df[col].apply(
                        lambda v: str(v).replace(".", "").replace(",", ".")
                        if isinstance(v, str) and "," in str(v) and "." in str(v)
                        else v
                    )
                    self._df[col] = pd.to_numeric(self._df[col], errors="coerce")

                elif dtype == "str":
                    self._df[col] = self._df[col].astype(str).replace("nan", "")

                elif dtype == "bool":
                    true_vals  = {"ya", "yes", "true", "1", "benar", "aktif"}
                    self._df[col] = self._df[col].apply(
                        lambda v: str(v).strip().lower() in true_vals
                        if pd.notna(v) else pd.NA
                    )

                elif dtype == "datetime":
                    self._df[col] = pd.to_datetime(
                        self._df[col], errors="coerce", dayfirst=True
                    )

                elif dtype == "date":
                    parsed = pd.to_datetime(
                        self._df[col], errors="coerce", dayfirst=True
                    )
                    self._df[col] = parsed.dt.strftime("%Y-%m-%d")

                elif dtype == "category":
                    self._df[col] = self._df[col].astype("category")

                self._operations.append(f"cast_types: {col} → {dtype}")
                logger.debug(f"  🔢 Cast '{col}' → {dtype}")

            except Exception as exc:
                self._warnings.append(f"cast_types: '{col}' failed → {exc}")
                logger.warning(f"⚠️  cast_types '{col}': {exc}")

        return self

    # ── 3. Value Mapping ──────────────────────────────────────────────────────

    def map_values(
        self,
        column_maps    : dict[str, dict[str, Any]],
        case_insensitive: bool = True,
    ) -> "DataTransformer":
        """
        Recode categorical values.

        Args:
            column_maps      : {column: {old_value: new_value}}
            case_insensitive : Match regardless of case. Default True.

        Example:
            .map_values({
                "pendidikan": {
                    "SD": "Elementary", "SMP": "Junior High",
                    "SMA": "High School", "S1": "Bachelor",
                    "S2": "Master", "S3": "Doctorate",
                },
                "gender": {"L": "Male", "P": "Female"},
            })
        """
        for col, vmap in column_maps.items():
            if col not in self._df.columns:
                self._warnings.append(f"map_values: '{col}' not found")
                continue

            if case_insensitive:
                lower_map = {k.lower(): v for k, v in vmap.items()}
                self._df[col] = self._df[col].apply(
                    lambda v: lower_map.get(str(v).strip().lower(), v)
                    if pd.notna(v) else v
                )
            else:
                self._df[col] = self._df[col].map(
                    lambda v: vmap.get(str(v).strip(), v) if pd.notna(v) else v
                )

            self._operations.append(f"map_values: {col} ({len(vmap)} mappings)")
            logger.debug(f"  🗺️  Mapped values in '{col}'")

        return self

    # ── 4. Split Column ───────────────────────────────────────────────────────

    def split_column(
        self,
        source_col  : str,
        new_cols    : list[str],
        sep         : str = " ",
        maxsplit    : int = 1,
        drop_source : bool = False,
    ) -> "DataTransformer":
        """
        Split one column into multiple columns.

        Args:
            source_col  : Column to split.
            new_cols    : Names for the resulting columns.
            sep         : Separator string. Default " " (space).
            maxsplit    : Max number of splits. Default 1.
            drop_source : Drop original column after split. Default False.

        Example:
            .split_column(
                "nama", ["nama_depan", "nama_belakang"],
                sep=" ", maxsplit=1
            )
            # "Budi Santoso" → nama_depan="Budi", nama_belakang="Santoso"
        """
        if source_col not in self._df.columns:
            self._warnings.append(f"split_column: '{source_col}' not found")
            return self

        split_df = self._df[source_col].astype(str).str.split(
            sep, n=maxsplit, expand=True
        )

        for i, col_name in enumerate(new_cols):
            self._df[col_name] = split_df[i] if i in split_df.columns else ""

        if drop_source:
            self._df.drop(columns=[source_col], inplace=True)

        self._operations.append(
            f"split_column: '{source_col}' → {new_cols}"
        )
        logger.info(f"  ✂️  Split '{source_col}' → {new_cols}")
        return self

    # ── 5. Merge Columns ─────────────────────────────────────────────────────

    def merge_columns(
        self,
        source_cols : list[str],
        new_col     : str,
        sep         : str = " ",
        drop_source : bool = False,
    ) -> "DataTransformer":
        """
        Merge multiple columns into one.

        Args:
            source_cols : Columns to merge.
            new_col     : Name of the resulting merged column.
            sep         : Separator between values. Default " ".
            drop_source : Drop source columns after merge. Default False.

        Example:
            .merge_columns(
                ["Jalan", "Kelurahan", "Kecamatan", "Kota"],
                new_col="Alamat Lengkap",
                sep=", "
            )
        """
        existing = [c for c in source_cols if c in self._df.columns]
        missing  = [c for c in source_cols if c not in self._df.columns]

        if missing:
            self._warnings.append(f"merge_columns: not found → {missing}")

        if not existing:
            return self

        self._df[new_col] = (
            self._df[existing]
            .fillna("")
            .astype(str)
            .apply(lambda row: sep.join(v for v in row if v.strip()), axis=1)
        )

        if drop_source:
            self._df.drop(columns=existing, inplace=True)

        self._operations.append(f"merge_columns: {existing} → '{new_col}'")
        logger.info(f"  🔗 Merged {existing} → '{new_col}'")
        return self

    # ── 6. Derived Columns ────────────────────────────────────────────────────

    def add_derived(
        self,
        new_col  : str,
        fn       : Callable[[pd.Series], Any],
        axis     : int = 1,
    ) -> "DataTransformer":
        """
        Add a new column computed from existing columns.

        Args:
            new_col : Name of the new column.
            fn      : Function applied row-wise (axis=1) or series-wise (axis=0).
            axis    : 1 = apply per row (receives full row Series), 0 = per column.

        Examples:
            # Full name from parts
            .add_derived("nama_lengkap",
                lambda row: f"{row['nama_depan']} {row['nama_belakang']}")

            # Age from birth year
            .add_derived("umur",
                lambda row: 2024 - int(row["tahun_lahir"])
                if str(row["tahun_lahir"]).isdigit() else None)

            # Tier classification
            .add_derived("tier",
                lambda row: "Gold" if float(row.get("total_purchase", 0)) > 5_000_000
                else "Silver" if float(row.get("total_purchase", 0)) > 1_000_000
                else "Bronze")
        """
        try:
            if axis == 1:
                self._df[new_col] = self._df.apply(fn, axis=1)
            else:
                self._df[new_col] = fn(self._df)

            self._operations.append(f"add_derived: '{new_col}'")
            logger.debug(f"  ➕ Added derived column '{new_col}'")
        except Exception as exc:
            self._warnings.append(f"add_derived '{new_col}': {exc}")
            logger.warning(f"⚠️  add_derived '{new_col}': {exc}")

        return self

    # ── 7. Select / Reorder Columns ───────────────────────────────────────────

    def select_columns(
        self,
        columns     : list[str],
        keep_meta   : bool = True,
    ) -> "DataTransformer":
        """
        Select and reorder columns for final output.

        Args:
            columns   : Ordered list of columns to keep.
            keep_meta : Also keep columns starting with '_' (audit/metadata). Default True.

        Example:
            .select_columns(["nama", "email", "phone", "kota", "tanggal"])
        """
        existing = [c for c in columns if c in self._df.columns]
        missing  = [c for c in columns if c not in self._df.columns]

        if missing:
            self._warnings.append(f"select_columns: not found → {missing}")

        if keep_meta:
            meta_cols = [c for c in self._df.columns if c.startswith("_") and c not in existing]
            existing  = existing + meta_cols

        self._df = self._df[existing]
        self._operations.append(f"select_columns ({len(existing)} kept)")
        logger.info(f"  📋 Selected {len(existing)} columns")
        return self

    # ── 8. Enforce Schema ────────────────────────────────────────────────────

    def enforce_schema(
        self,
        schema      : list[str],
        fill_value  : Any = None,
    ) -> "DataTransformer":
        """
        Ensure final DataFrame has exactly the columns in schema (in order).
        Missing columns are added with fill_value. Extra columns are dropped.

        Args:
            schema     : Ordered list of expected column names.
            fill_value : Value for missing columns. Default None.

        Example:
            .enforce_schema(["nama", "email", "phone", "kota", "tanggal", "sumber"])
        """
        for col in schema:
            if col not in self._df.columns:
                self._df[col] = fill_value
                self._warnings.append(f"enforce_schema: added missing column '{col}'")

        self._df = self._df[schema]
        self._operations.append(f"enforce_schema ({len(schema)} columns)")
        logger.info(f"  📐 Schema enforced: {len(schema)} columns")
        return self

    # ── 9. Normalize Phone ────────────────────────────────────────────────────

    def normalize_phones(self, columns: list[str]) -> "DataTransformer":
        """
        Convenience: normalize phone columns to E.164 format (+62XXXXXXXXXX).
        Requires validators/phone_validator.py to be present.

        Example:
            .normalize_phones(["phone", "phone_alt"])
        """
        try:
            from validators.phone_validator import normalize_phone
            for col in columns:
                if col in self._df.columns:
                    self._df[col] = self._df[col].apply(
                        lambda v: normalize_phone(str(v)) if pd.notna(v) else v
                    )
                    self._operations.append(f"normalize_phones: {col}")
        except ImportError:
            self._warnings.append("normalize_phones: phone_validator not available")
        return self

    # ── 10. Normalize Emails ─────────────────────────────────────────────────

    def normalize_emails(self, columns: list[str]) -> "DataTransformer":
        """
        Lowercase and strip all email columns.

        Example:
            .normalize_emails(["email", "email_alt"])
        """
        for col in columns:
            if col in self._df.columns:
                self._df[col] = (
                    self._df[col].astype(str)
                    .str.lower()
                    .str.strip()
                    .replace("nan", "")
                )
                self._operations.append(f"normalize_emails: {col}")
        return self

    # ── BUILD ─────────────────────────────────────────────────────────────────

    def build(self) -> TransformResult:
        """
        Finalize and return the TransformResult.

        Returns:
            TransformResult with transformed DataFrame, operations log, warnings.
        """
        logger.info(
            f"✅ Transform complete | "
            f"{len(self._df)} rows | "
            f"{len(self._df.columns)} cols | "
            f"{len(self._operations)} operations | "
            f"{len(self._warnings)} warnings"
        )
        if self._warnings:
            for w in self._warnings:
                logger.warning(f"  ⚠️  {w}")

        return TransformResult(
            dataframe    = self._df.reset_index(drop=True),
            original_cols= self._original,
            final_cols   = list(self._df.columns),
            operations   = self._operations,
            warnings     = self._warnings,
        )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def transform_form_data(
    df         : pd.DataFrame,
    rename_map : Optional[dict[str, str]]       = None,
    type_map   : Optional[dict[str, str]]       = None,
    value_maps : Optional[dict[str, dict]]      = None,
    output_cols: Optional[list[str]]            = None,
) -> pd.DataFrame:
    """
    Quick transform pipeline. Returns transformed DataFrame directly.

    Used in main.py:
        from processors.data_transformer import transform_form_data
        df_out = transform_form_data(
            df,
            rename_map={"Email Address": "email", "Nama Lengkap": "nama"},
            type_map={"umur": "int"},
        )
    """
    t = DataTransformer(df)

    if rename_map:
        t.rename_columns(rename_map)
    if type_map:
        t.cast_types(type_map)
    if value_maps:
        t.map_values(value_maps)
    if output_cols:
        t.select_columns(output_cols)

    return t.build().dataframe


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Data Transformer — Quick Test")
    print("=" * 65)

    raw = pd.DataFrame({
        "Timestamp"      : ["2024-06-01 08:00", "2024-06-01 09:00", "2024-06-01 10:00"],
        "Nama Lengkap"   : ["Budi Santoso", "Siti Rahayu", "Andi Wijaya"],
        "Email Address"  : ["Budi@Gmail.com", "siti@Yahoo.com", "andi@outlook.com"],
        "Nomor HP"       : ["08123456789", "08556789012", "08778901234"],
        "Kota"           : ["Surabaya", "Jakarta", "Bandung"],
        "Pendidikan"     : ["S1", "S2", "SMA"],
        "Total Pembelian": ["1500000", "2500000", "750000"],
        "Aktif"          : ["Ya", "Ya", "Tidak"],
        "_source"        : ["google_form", "google_form", "google_form"],
    })

    print("\n📋 Raw DataFrame:")
    print(raw.to_string(index=False))

    result = (
        DataTransformer(raw)
        .rename_columns({
            "Nama Lengkap"   : "nama",
            "Email Address"  : "email",
            "Nomor HP"       : "phone",
            "Total Pembelian": "total",
            "Pendidikan"     : "edu",
        })
        .cast_types({"total": "float", "Aktif": "bool", "Timestamp": "datetime"})
        .map_values({"edu": {"S1": "Bachelor", "S2": "Master", "SMA": "High School"}})
        .split_column("nama", ["nama_depan", "nama_belakang"], sep=" ", maxsplit=1)
        .normalize_phones(["phone"])
        .normalize_emails(["email"])
        .add_derived(
            "tier",
            lambda row: "Gold"   if (row.get("total") or 0) >= 2_000_000
                   else "Silver" if (row.get("total") or 0) >= 1_000_000
                   else "Bronze"
        )
        .select_columns(["nama_depan", "nama_belakang", "email", "phone", "Kota", "edu", "total", "tier"])
        .build()
    )

    print("\n✅ Transformed DataFrame:")
    print(result.dataframe.to_string(index=False))

    print("\n📊 Transform Summary:")
    for k, v in result.summary.items():
        if k != "operations" and k != "warnings":
            print(f"  {k:<18}: {v}")
    print(f"  operations:")
    for op in result.operations:
        print(f"    • {op}")
    if result.warnings:
        print(f"  warnings:")
        for w in result.warnings:
            print(f"    ⚠️  {w}")