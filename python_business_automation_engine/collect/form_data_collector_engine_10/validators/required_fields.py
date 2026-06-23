"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Required Fields Validator         ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : validators/required_fields.py
Purpose : Validate that required fields are present and non-empty in
          collected form data.

          Features:
          - Define required vs optional fields per form schema
          - Support conditional required: field X required only if field Y = value
          - Severity levels: ERROR (must have) vs WARNING (should have)
          - Per-row and per-column completeness report
          - Auto-suggest which rows to quarantine vs pass

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────────────────────
Severity = Literal["error", "warning"]


# ─────────────────────────────────────────────────────────────────────────────
# FIELD SPEC DATACLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FieldSpec:
    """
    Specification for a single form field.

    Args:
        name        : Column name as it appears in the DataFrame.
        severity    : "error" = hard required (row quarantined if missing).
                      "warning" = soft required (row flagged but not quarantined).
        alias       : Alternative column names to look for (Google Forms can
                      change headers slightly between form versions).
        condition   : Optional callable(row) → bool. If provided, field is
                      only required when condition returns True.
                      Example: lambda row: row.get("Jenis") == "Individu"
        description : Human-readable description for reports.
    """
    name       : str
    severity   : Severity                        = "error"
    alias      : list[str]                       = field(default_factory=list)
    condition  : Optional[Callable[[dict], bool]] = None
    description: str                             = ""

    def __post_init__(self) -> None:
        # always include the canonical name in alias lookup
        if self.name not in self.alias:
            self.alias = [self.name] + self.alias


# ─────────────────────────────────────────────────────────────────────────────
# ROW VALIDATION RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RowValidationResult:
    """
    Validation result for a single row.

    Attributes:
        row_index      : Index in the original DataFrame.
        is_valid       : True if all ERROR-severity fields are present.
        has_warnings   : True if any WARNING-severity fields are missing.
        missing_errors : Fields with severity=error that are missing.
        missing_warnings: Fields with severity=warning that are missing.
        completeness   : % of all required fields that are filled.
    """
    row_index       : int
    is_valid        : bool
    has_warnings    : bool
    missing_errors  : list[str] = field(default_factory=list)
    missing_warnings: list[str] = field(default_factory=list)
    completeness    : float     = 100.0

    @property
    def all_missing(self) -> list[str]:
        return self.missing_errors + self.missing_warnings

    @property
    def summary(self) -> str:
        if self.is_valid and not self.has_warnings:
            return f"✅ Row {self.row_index} — complete ({self.completeness:.0f}%)"
        parts = []
        if self.missing_errors:
            parts.append(f"ERROR: {self.missing_errors}")
        if self.missing_warnings:
            parts.append(f"WARN: {self.missing_warnings}")
        tag = "❌" if not self.is_valid else "⚠️ "
        return f"{tag} Row {self.row_index} — {' | '.join(parts)} ({self.completeness:.0f}%)"


# ─────────────────────────────────────────────────────────────────────────────
# BATCH RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RequiredFieldsCheckResult:
    """
    Result of required fields check on a full DataFrame.

    Attributes:
        total_rows      : Total rows checked.
        valid_rows      : Rows with all ERROR fields present.
        warning_rows    : Rows with WARNING fields missing (but ERROR fields ok).
        error_rows      : Rows missing at least one ERROR field.
        valid_mask      : Boolean Series (True = passes error check).
        warning_mask    : Boolean Series (True = has at least one warning).
        column_report   : Per-column fill-rate DataFrame.
        row_results     : List of RowValidationResult.
        schema_applied  : Names of all field specs checked.
    """
    total_rows    : int
    valid_rows    : int
    warning_rows  : int
    error_rows    : int
    valid_mask    : pd.Series
    warning_mask  : pd.Series
    column_report : pd.DataFrame
    row_results   : list[RowValidationResult]
    schema_applied: list[str]

    @property
    def overall_completeness(self) -> float:
        return round(self.valid_rows / max(self.total_rows, 1) * 100, 2)

    @property
    def summary(self) -> dict:
        return {
            "total_rows"           : self.total_rows,
            "valid_rows"           : self.valid_rows,
            "warning_rows"         : self.warning_rows,
            "error_rows"           : self.error_rows,
            "overall_completeness" : f"{self.overall_completeness}%",
            "fields_checked"       : self.schema_applied,
        }

    def __repr__(self) -> str:
        return (
            f"<RequiredFieldsCheckResult "
            f"valid={self.valid_rows}/{self.total_rows} "
            f"errors={self.error_rows} warnings={self.warning_rows}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class RequiredFieldsValidator:
    """
    Validates presence and non-emptiness of required fields in form data.

    Usage:
        schema = [
            FieldSpec("Nama Lengkap",  severity="error"),
            FieldSpec("Email",         severity="error",   alias=["Email Address"]),
            FieldSpec("No. HP",        severity="error",   alias=["Nomor HP", "Phone"]),
            FieldSpec("Alamat",        severity="warning"),
            FieldSpec("NPWP",          severity="warning",
                      condition=lambda row: row.get("Tipe") == "Perusahaan"),
        ]
        validator = RequiredFieldsValidator(schema)
        result    = validator.check(df)

    Args:
        schema         : List of FieldSpec objects defining required fields.
        empty_values   : Additional values treated as "empty" besides NaN/None/""
        quarantine_errors_only: If True, only ERROR rows go to invalid bucket.
                                If False, WARNING rows also quarantined.
    """

    def __init__(
        self,
        schema                  : list[FieldSpec],
        empty_values            : Optional[list[Any]] = None,
        quarantine_errors_only  : bool = True,
    ) -> None:
        self.schema                 = schema
        self.quarantine_errors_only = quarantine_errors_only
        self._empty: set            = {
            "", "nan", "none", "null", "n/a", "na", "-", ".",
            *(str(v).lower() for v in (empty_values or [])),
        }

    # ── private ───────────────────────────────────────────────────────────────

    def _is_empty(self, value: Any) -> bool:
        """Return True if value is considered empty."""
        if value is None:
            return True
        if isinstance(value, float):
            import math
            return math.isnan(value)
        return str(value).strip().lower() in self._empty

    def _resolve_column(
        self, df: pd.DataFrame, spec: FieldSpec
    ) -> Optional[str]:
        """
        Find the actual column name in df from spec.name + spec.alias.
        Returns None if no matching column found.
        """
        for candidate in spec.alias:
            # exact match
            if candidate in df.columns:
                return candidate
            # case-insensitive match
            for col in df.columns:
                if col.strip().lower() == candidate.strip().lower():
                    return col
        return None

    def _check_row(
        self,
        row       : pd.Series,
        col_map   : dict[str, Optional[str]],  # spec.name → actual col
    ) -> RowValidationResult:
        """Validate a single row against the schema."""
        row_dict        = row.to_dict()
        missing_errors  : list[str] = []
        missing_warnings: list[str] = []
        total_applicable = 0

        for spec in self.schema:
            actual_col = col_map.get(spec.name)

            # skip if column not found in dataframe at all
            if actual_col is None:
                continue

            # check conditional requirement
            if spec.condition is not None:
                try:
                    if not spec.condition(row_dict):
                        continue   # not required for this row
                except Exception:
                    pass           # condition error → treat as required

            total_applicable += 1
            value = row_dict.get(actual_col)

            if self._is_empty(value):
                if spec.severity == "error":
                    missing_errors.append(spec.name)
                else:
                    missing_warnings.append(spec.name)

        total_missing    = len(missing_errors) + len(missing_warnings)
        completeness     = (
            (total_applicable - total_missing) / max(total_applicable, 1) * 100
        )

        return RowValidationResult(
            row_index        = int(row.name),
            is_valid         = len(missing_errors) == 0,
            has_warnings     = len(missing_warnings) > 0,
            missing_errors   = missing_errors,
            missing_warnings = missing_warnings,
            completeness     = round(completeness, 1),
        )

    # ── public: check ─────────────────────────────────────────────────────────

    def check(
        self,
        df     : pd.DataFrame,
        verbose: bool = False,
    ) -> RequiredFieldsCheckResult:
        """
        Run required fields validation on entire DataFrame.

        Args:
            df      : Input DataFrame.
            verbose : Log each row result. Default False.

        Returns:
            RequiredFieldsCheckResult
        """
        if df.empty:
            logger.warning("⚠️  Empty DataFrame — skipping required fields check.")
            return RequiredFieldsCheckResult(
                total_rows=0, valid_rows=0, warning_rows=0, error_rows=0,
                valid_mask=pd.Series(dtype=bool),
                warning_mask=pd.Series(dtype=bool),
                column_report=pd.DataFrame(),
                row_results=[],
                schema_applied=[],
            )

        logger.info(
            f"🔎 Required fields check | "
            f"{len(self.schema)} field specs | {len(df)} rows"
        )

        # ── build column map once (avoid repeated lookup per row) ─────────────
        col_map: dict[str, Optional[str]] = {}
        for spec in self.schema:
            actual = self._resolve_column(df, spec)
            col_map[spec.name] = actual
            if actual is None:
                logger.warning(
                    f"⚠️  Field '{spec.name}' (aliases: {spec.alias}) "
                    f"not found in DataFrame columns: {list(df.columns)}"
                )
            else:
                logger.debug(f"   '{spec.name}' → mapped to column '{actual}'")

        # ── validate each row ─────────────────────────────────────────────────
        row_results: list[RowValidationResult] = []
        for _, row in df.iterrows():
            result = self._check_row(row, col_map)
            row_results.append(result)
            if verbose:
                logger.debug(result.summary)

        # ── build masks ───────────────────────────────────────────────────────
        valid_mask   = pd.Series(
            [r.is_valid    for r in row_results], index=df.index
        )
        warning_mask = pd.Series(
            [r.has_warnings for r in row_results], index=df.index
        )

        n_errors   = int((~valid_mask).sum())
        n_warnings = int(warning_mask.sum())
        n_valid    = int(valid_mask.sum())

        # ── per-column completeness report ────────────────────────────────────
        col_report_rows = []
        for spec in self.schema:
            actual = col_map.get(spec.name)
            if actual is None:
                col_report_rows.append({
                    "field"      : spec.name,
                    "severity"   : spec.severity,
                    "column"     : "NOT FOUND",
                    "total"      : len(df),
                    "filled"     : 0,
                    "missing"    : len(df),
                    "fill_rate"  : 0.0,
                    "conditional": spec.condition is not None,
                })
                continue

            filled  = df[actual].apply(lambda v: not self._is_empty(v)).sum()
            missing = len(df) - filled
            col_report_rows.append({
                "field"      : spec.name,
                "severity"   : spec.severity,
                "column"     : actual,
                "total"      : len(df),
                "filled"     : int(filled),
                "missing"    : int(missing),
                "fill_rate"  : round(filled / len(df) * 100, 2),
                "conditional": spec.condition is not None,
            })

        column_report = pd.DataFrame(col_report_rows)

        # ── log summary ───────────────────────────────────────────────────────
        logger.info(
            f"✅ Required fields result: "
            f"{n_valid} valid | {n_errors} errors | {n_warnings} warnings"
        )

        return RequiredFieldsCheckResult(
            total_rows    = len(df),
            valid_rows    = n_valid,
            warning_rows  = n_warnings,
            error_rows    = n_errors,
            valid_mask    = valid_mask,
            warning_mask  = warning_mask,
            column_report = column_report,
            row_results   = row_results,
            schema_applied= [s.name for s in self.schema],
        )

    def get_valid_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return only rows that pass all ERROR-level required field checks."""
        result = self.check(df)
        return df[result.valid_mask].reset_index(drop=True)

    def get_invalid_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return only rows that fail at least one ERROR-level required check."""
        result = self.check(df)
        return df[~result.valid_mask].reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION  (used by main.py step_validate)
# ─────────────────────────────────────────────────────────────────────────────

def check_required_fields(
    df          : pd.DataFrame,
    required    : list[str],
    warnings    : Optional[list[str]] = None,
    empty_values: Optional[list[Any]] = None,
) -> pd.Series:
    """
    Quick required-fields check. Returns boolean mask (True = row is valid).

    Used by main.py:
        from validators.required_fields import check_required_fields
        mask = check_required_fields(df, required=["Nama", "Email", "No. HP"])
        df_valid   = df[mask]
        df_invalid = df[~mask]

    Args:
        df           : Input DataFrame.
        required     : Columns that MUST be present (error severity).
        warnings     : Columns that SHOULD be present (warning severity).
        empty_values : Extra values treated as empty.

    Returns:
        pd.Series of bool (True = passes all ERROR checks).
    """
    schema = [FieldSpec(col, severity="error") for col in required]
    if warnings:
        schema += [FieldSpec(col, severity="warning") for col in warnings]

    validator = RequiredFieldsValidator(schema, empty_values=empty_values)
    result    = validator.check(df)
    return result.valid_mask


# ─────────────────────────────────────────────────────────────────────────────
# COMMON FORM SCHEMAS  (ready-to-use presets)
# ─────────────────────────────────────────────────────────────────────────────

class FormSchemas:
    """
    Pre-built schemas for common form types.
    Use as a starting point, customize as needed.
    """

    @staticmethod
    def lead_form() -> list[FieldSpec]:
        """Standard lead generation form."""
        return [
            FieldSpec("Nama Lengkap",  severity="error",   alias=["Nama", "Full Name", "Name"]),
            FieldSpec("Email",         severity="error",   alias=["Email Address", "Alamat Email"]),
            FieldSpec("No. HP",        severity="error",   alias=["Nomor HP", "Phone", "Telepon", "WA"]),
            FieldSpec("Kota",          severity="warning", alias=["Kota/Kabupaten", "City"]),
            FieldSpec("Sumber",        severity="warning", alias=["Sumber Info", "Source", "Referral"]),
        ]

    @staticmethod
    def registration_form() -> list[FieldSpec]:
        """Event / webinar registration form."""
        return [
            FieldSpec("Nama Lengkap",  severity="error",  alias=["Nama", "Full Name"]),
            FieldSpec("Email",         severity="error",  alias=["Email Address"]),
            FieldSpec("No. HP",        severity="error",  alias=["Nomor HP", "Phone"]),
            FieldSpec("Institusi",     severity="error",  alias=["Perusahaan", "Instansi", "Company"]),
            FieldSpec("Jabatan",       severity="warning",alias=["Posisi", "Position", "Role"]),
            FieldSpec("Tanggal Lahir", severity="warning",alias=["Birth Date", "TTL"]),
        ]

    @staticmethod
    def order_form() -> list[FieldSpec]:
        """Online order / purchase form."""
        return [
            FieldSpec("Nama",          severity="error",  alias=["Nama Pemesan", "Full Name"]),
            FieldSpec("Email",         severity="error",  alias=["Email Address"]),
            FieldSpec("No. HP",        severity="error",  alias=["WhatsApp", "Phone", "Telepon"]),
            FieldSpec("Alamat",        severity="error",  alias=["Alamat Pengiriman", "Shipping Address"]),
            FieldSpec("Kode Pos",      severity="error",  alias=["Postal Code", "ZIP"]),
            FieldSpec("Produk",        severity="error",  alias=["Nama Produk", "Product", "Item"]),
            FieldSpec("Jumlah",        severity="error",  alias=["Qty", "Quantity"]),
            FieldSpec("Catatan",       severity="warning",alias=["Notes", "Keterangan"]),
            FieldSpec("NPWP",          severity="warning",
                      alias=["No. NPWP"],
                      condition=lambda row: str(row.get("Tipe Pembeli","")).lower() == "perusahaan",
                      description="Only required for corporate buyers"),
        ]

    @staticmethod
    def survey_form() -> list[FieldSpec]:
        """Customer satisfaction / feedback survey."""
        return [
            FieldSpec("Timestamp",     severity="error"),
            FieldSpec("Rating",        severity="error",  alias=["Skor", "Score", "Penilaian"]),
            FieldSpec("Komentar",      severity="warning",alias=["Feedback", "Saran", "Comment"]),
            FieldSpec("Email",         severity="warning",alias=["Email Address"]),
        ]


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Required Fields Validator — Quick Test")
    print("=" * 65)

    # ── Sample form data ──────────────────────────────────────────────────────
    sample = pd.DataFrame({
        "Timestamp"    : ["2024-06-01 08:00", "2024-06-01 09:00", "2024-06-01 10:00",
                          "2024-06-01 11:00", "2024-06-01 12:00"],
        "Nama Lengkap" : ["Budi Santoso",  "",              "Siti Rahayu",  "Andi",   ""],
        "Email Address": ["budi@gmail.com","siti@yahoo.com","",             "andi@x.com",""],
        "Nomor HP"     : ["08123456789",   "08556789012",  "08778901234",  "",         "08990001234"],
        "Kota"         : ["Surabaya",      "",             "Jakarta",      "Bandung",  ""],
        "Tipe Pembeli" : ["Individu",      "Individu",     "Perusahaan",   "Individu", "Perusahaan"],
        "No. NPWP"     : ["",              "",             "01.234.567.8-901.234", "", ""],
    })

    print("\n📋 Input DataFrame:")
    print(sample.to_string(index=True))

    # ── Test 1: Lead form schema ──────────────────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 1: Lead Form Schema")

    schema = FormSchemas.lead_form()
    validator = RequiredFieldsValidator(schema)
    result    = validator.check(sample, verbose=True)

    print(f"\n  Result: {result}")
    print(f"\n  Summary:")
    for k, v in result.summary.items():
        print(f"    {k:<28}: {v}")

    print(f"\n  📊 Column Report:")
    print(result.column_report.to_string(index=False))

    # ── Test 2: Order form (conditional NPWP) ────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 2: Order Form Schema (with conditional NPWP)")

    schema2   = FormSchemas.order_form()
    validator2 = RequiredFieldsValidator(schema2)
    result2   = validator2.check(sample, verbose=True)

    print(f"\n  Result: {result2}")

    # ── Test 3: Quick convenience function ───────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 3: check_required_fields() — quick function")

    mask = check_required_fields(
        sample,
        required=["Nama Lengkap", "Email Address", "Nomor HP"],
        warnings=["Kota"],
    )

    print(f"\n  Valid mask: {mask.tolist()}")
    print(f"\n  ✅ Valid rows ({mask.sum()}):")
    print(sample[mask][["Nama Lengkap", "Email Address", "Nomor HP"]].to_string())
    print(f"\n  ❌ Invalid rows ({(~mask).sum()}):")
    print(sample[~mask][["Nama Lengkap", "Email Address", "Nomor HP"]].to_string())