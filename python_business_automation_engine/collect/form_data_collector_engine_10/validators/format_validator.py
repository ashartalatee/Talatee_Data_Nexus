"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Format Validator                  ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : validators/format_validator.py
Purpose : Validate data format correctness for various field types:
          - Date / DateTime
          - Numeric (integer, float, range)
          - URL / Website
          - Indonesian NIK (Nomor Induk Kependudukan, 16 digit)
          - Indonesian NPWP (Nomor Pokok Wajib Pajak)
          - Postal Code (Indonesia & international)
          - Free text (min/max length, pattern)
          - Gender field normalization
          - Currency / money values

          Supports both single-value and full-DataFrame batch validation.

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Literal, Optional, Union

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# TYPE ALIASES
# ─────────────────────────────────────────────────────────────────────────────
FieldType = Literal[
    "date", "datetime", "integer", "float",
    "url", "nik", "npwp", "postal_code",
    "text", "gender", "currency",
]


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FormatValidationResult:
    """
    Result of a single field format validation.

    Attributes:
        value      : Original input value.
        field_type : Type that was validated against.
        is_valid   : True if format is correct.
        reason     : Failure reason code (empty if valid).
        normalized : Cleaned / normalized value (e.g. date → ISO 8601).
        detail     : Optional extra info (e.g. parsed date components).
    """
    value      : Any
    field_type : str
    is_valid   : bool
    reason     : str  = ""
    normalized : str  = ""
    detail     : dict = field(default_factory=dict)

    @property
    def summary(self) -> str:
        if self.is_valid:
            norm = f" → {self.normalized}" if self.normalized else ""
            return f"✅ {self.value!r}{norm}"
        return f"❌ {self.value!r} [{self.field_type}] → {self.reason}"


# ─────────────────────────────────────────────────────────────────────────────
# REGEX PATTERNS
# ─────────────────────────────────────────────────────────────────────────────

# URL (http / https / ftp)
_URL_RE = re.compile(
    r"^(https?|ftp)://"
    r"(?:(?:[A-Z0-9](?:[A-Z0-9\-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,10})"
    r"(?::\d+)?(?:/[^\s]*)?$",
    re.IGNORECASE,
)

# NIK Indonesia: 16 digit, starts with province code 11-99
_NIK_RE = re.compile(r"^[1-9][0-9]{15}$")

# NPWP Indonesia: XX.XXX.XXX.X-XXX.XXX  or 15 digits
_NPWP_FORMATTED_RE = re.compile(
    r"^\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{3}$"
)
_NPWP_DIGITS_RE = re.compile(r"^\d{15}$")

# Indonesia postal code: 5 digits, starts with 1-9
_POSTAL_ID_RE = re.compile(r"^[1-9]\d{4}$")

# International postal code: 3-10 alphanumeric chars (covers most countries)
_POSTAL_INTL_RE = re.compile(r"^[A-Z0-9]{3,10}$", re.IGNORECASE)

# Currency: optional Rp/$ prefix, digits with optional thousand-sep & decimals
_CURRENCY_RE = re.compile(
    r"^(?:Rp\.?\s*|IDR\s*|\$\s*|USD\s*)?[\d]{1,15}(?:[.,]\d{1,3})*(?:[.,]\d{2})?$",
    re.IGNORECASE,
)

# Common date separators
_DATE_FORMATS = [
    "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
    "%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d",
    "%d/%m/%y", "%d-%m-%y",
    "%d %B %Y", "%d %b %Y",             # "23 June 2024", "23 Jun 2024"
    "%B %d, %Y", "%b %d, %Y",           # "June 23, 2024"
]
_DATETIME_FORMATS = [
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
]

# Gender normalization map
_GENDER_MAP: dict[str, str] = {
    # Male
    "l": "L", "laki": "L", "laki-laki": "L", "pria": "L",
    "male": "L", "m": "L", "man": "L", "men": "L", "boy": "L",
    # Female
    "p": "P", "perempuan": "P", "wanita": "P",
    "female": "P", "f": "P", "woman": "P", "women": "P", "girl": "P",
}


# ─────────────────────────────────────────────────────────────────────────────
# INDIVIDUAL VALIDATORS (pure functions)
# ─────────────────────────────────────────────────────────────────────────────

def validate_date(value: Any) -> FormatValidationResult:
    """Validate and normalize a date string to ISO 8601 (YYYY-MM-DD)."""
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "date", False, "empty_value")

    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return FormatValidationResult(
                value=value, field_type="date", is_valid=True,
                normalized=dt.strftime("%Y-%m-%d"),
                detail={"day": dt.day, "month": dt.month, "year": dt.year},
            )
        except ValueError:
            continue

    return FormatValidationResult(
        value=value, field_type="date", is_valid=False,
        reason=f"unrecognized_date_format | tried {len(_DATE_FORMATS)} formats",
    )


def validate_datetime(value: Any) -> FormatValidationResult:
    """Validate and normalize a datetime string to ISO 8601."""
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "datetime", False, "empty_value")

    # try datetime formats first
    for fmt in _DATETIME_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return FormatValidationResult(
                value=value, field_type="datetime", is_valid=True,
                normalized=dt.strftime("%Y-%m-%dT%H:%M:%S"),
                detail={"timestamp": dt.isoformat()},
            )
        except ValueError:
            continue

    # fallback: try date-only formats
    date_result = validate_date(value)
    if date_result.is_valid:
        return FormatValidationResult(
            value=value, field_type="datetime", is_valid=True,
            normalized=date_result.normalized + "T00:00:00",
            detail=date_result.detail,
        )

    return FormatValidationResult(
        value=value, field_type="datetime", is_valid=False,
        reason="unrecognized_datetime_format",
    )


def validate_integer(
    value: Any,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> FormatValidationResult:
    """Validate integer value with optional range check."""
    raw = str(value).strip().replace(",", "").replace(".", "")
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "integer", False, "empty_value")
    try:
        num = int(raw)
    except ValueError:
        return FormatValidationResult(value, "integer", False, "not_an_integer")

    if min_val is not None and num < min_val:
        return FormatValidationResult(
            value, "integer", False,
            reason=f"below_minimum_{min_val}",
            normalized=str(num),
        )
    if max_val is not None and num > max_val:
        return FormatValidationResult(
            value, "integer", False,
            reason=f"above_maximum_{max_val}",
            normalized=str(num),
        )

    return FormatValidationResult(
        value=value, field_type="integer", is_valid=True,
        normalized=str(num), detail={"parsed": num},
    )


def validate_float(
    value: Any,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    decimal_sep: str = ".",
) -> FormatValidationResult:
    """Validate float value. Handles both '1.500,75' and '1500.75' formats."""
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "float", False, "empty_value")

    # normalize thousand separator vs decimal separator
    cleaned = raw.replace("Rp", "").replace("IDR", "").replace("$", "").strip()
    # detect comma-as-decimal (European/Indonesian style: 1.500,75)
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        num = float(cleaned)
    except ValueError:
        return FormatValidationResult(value, "float", False, "not_a_number")

    if min_val is not None and num < min_val:
        return FormatValidationResult(
            value, "float", False, f"below_minimum_{min_val}", str(num)
        )
    if max_val is not None and num > max_val:
        return FormatValidationResult(
            value, "float", False, f"above_maximum_{max_val}", str(num)
        )

    return FormatValidationResult(
        value=value, field_type="float", is_valid=True,
        normalized=str(num), detail={"parsed": num},
    )


def validate_url(value: Any) -> FormatValidationResult:
    """Validate URL format (http/https/ftp)."""
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "url", False, "empty_value")

    # auto-prepend https:// if missing scheme
    normalized = raw if re.match(r"^https?://|^ftp://", raw, re.I) else "https://" + raw

    if _URL_RE.match(normalized):
        return FormatValidationResult(
            value=value, field_type="url", is_valid=True, normalized=normalized
        )
    return FormatValidationResult(value=value, field_type="url", is_valid=False, reason="invalid_url_format")


def validate_nik(value: Any) -> FormatValidationResult:
    """
    Validate Indonesian NIK (Nomor Induk Kependudukan).
    Rules:
    - Exactly 16 digits
    - First 2 digits = province code (11-99)
    - Digits 3-4 = city/regency code (01-99)
    - Digits 5-6 = district code (01-99)
    - Digits 7-8 = birth date day (01-31, or 41-71 for female = day+40)
    - Digits 9-10 = birth month (01-12)
    - Digits 11-12 = birth year (00-99)
    """
    raw = re.sub(r"\D", "", str(value)) if value is not None else ""
    if not raw:
        return FormatValidationResult(value, "nik", False, "empty_value")
    if not _NIK_RE.match(raw):
        return FormatValidationResult(
            value=value, field_type="nik", is_valid=False,
            reason=f"must_be_16_digits_got_{len(raw)}",
        )

    province = raw[:2]
    city     = raw[2:4]
    district = raw[4:6]
    day_raw  = int(raw[6:8])
    month    = int(raw[8:10])
    year     = raw[10:12]

    is_female = day_raw > 40
    day       = day_raw - 40 if is_female else day_raw

    if not (1 <= day <= 31):
        return FormatValidationResult(value, "nik", False, "invalid_birth_day")
    if not (1 <= month <= 12):
        return FormatValidationResult(value, "nik", False, "invalid_birth_month")

    gender = "P (Perempuan)" if is_female else "L (Laki-laki)"

    return FormatValidationResult(
        value=value, field_type="nik", is_valid=True,
        normalized=raw,
        detail={
            "province_code": province,
            "city_code"    : city,
            "district_code": district,
            "birth_day"    : day,
            "birth_month"  : month,
            "birth_year"   : f"xx{year}",
            "gender"       : gender,
        },
    )


def validate_npwp(value: Any) -> FormatValidationResult:
    """
    Validate Indonesian NPWP (Nomor Pokok Wajib Pajak).
    Accepts: '01.234.567.8-901.234' or '012345678901234' (15 digits)
    """
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "npwp", False, "empty_value")

    if _NPWP_FORMATTED_RE.match(raw):
        digits = re.sub(r"\D", "", raw)
        return FormatValidationResult(
            value=value, field_type="npwp", is_valid=True,
            normalized=raw, detail={"digits": digits},
        )

    digits = re.sub(r"\D", "", raw)
    if _NPWP_DIGITS_RE.match(digits):
        formatted = (
            f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}."
            f"{digits[8]}-{digits[9:12]}.{digits[12:15]}"
        )
        return FormatValidationResult(
            value=value, field_type="npwp", is_valid=True,
            normalized=formatted, detail={"digits": digits},
        )

    return FormatValidationResult(
        value=value, field_type="npwp", is_valid=False,
        reason="invalid_npwp_format | expected XX.XXX.XXX.X-XXX.XXX or 15 digits",
    )


def validate_postal_code(
    value: Any,
    country: str = "ID",
) -> FormatValidationResult:
    """Validate postal code. Default: Indonesian 5-digit format."""
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "postal_code", False, "empty_value")

    if country.upper() == "ID":
        if _POSTAL_ID_RE.match(raw):
            return FormatValidationResult(
                value=value, field_type="postal_code", is_valid=True, normalized=raw
            )
        return FormatValidationResult(
            value=value, field_type="postal_code", is_valid=False,
            reason="invalid_indonesian_postal_code | must be 5 digits starting 1-9",
        )

    cleaned = raw.upper().replace(" ", "").replace("-", "")
    if _POSTAL_INTL_RE.match(cleaned):
        return FormatValidationResult(
            value=value, field_type="postal_code", is_valid=True, normalized=cleaned
        )
    return FormatValidationResult(
        value=value, field_type="postal_code", is_valid=False,
        reason="invalid_international_postal_code",
    )


def validate_text(
    value    : Any,
    min_len  : int = 1,
    max_len  : int = 500,
    pattern  : Optional[str] = None,
    allow_empty: bool = False,
) -> FormatValidationResult:
    """Validate free text with optional length and regex pattern constraints."""
    raw = str(value).strip() if value is not None else ""
    if not raw:
        if allow_empty:
            return FormatValidationResult(value, "text", True, normalized="")
        return FormatValidationResult(value, "text", False, "empty_value")

    if len(raw) < min_len:
        return FormatValidationResult(
            value, "text", False,
            reason=f"too_short | min={min_len} got={len(raw)}"
        )
    if len(raw) > max_len:
        return FormatValidationResult(
            value, "text", False,
            reason=f"too_long | max={max_len} got={len(raw)}"
        )
    if pattern and not re.match(pattern, raw):
        return FormatValidationResult(
            value, "text", False,
            reason=f"pattern_mismatch | pattern={pattern}"
        )

    return FormatValidationResult(
        value=value, field_type="text", is_valid=True,
        normalized=raw, detail={"length": len(raw)},
    )


def validate_gender(value: Any) -> FormatValidationResult:
    """
    Validate and normalize gender field.
    Accepts many formats → normalizes to 'L' (Laki-laki) or 'P' (Perempuan).
    """
    raw = str(value).strip() if value is not None else ""
    if not raw or raw.lower() in ("nan", "none", ""):
        return FormatValidationResult(value, "gender", False, "empty_value")

    normalized = _GENDER_MAP.get(raw.lower())
    if normalized:
        return FormatValidationResult(
            value=value, field_type="gender", is_valid=True,
            normalized=normalized,
            detail={"label": "Laki-laki" if normalized == "L" else "Perempuan"},
        )

    return FormatValidationResult(
        value=value, field_type="gender", is_valid=False,
        reason=f"unrecognized_gender_value | got='{raw}'",
    )


def validate_currency(value: Any, min_val: float = 0) -> FormatValidationResult:
    """Validate currency / money value. Strips Rp, IDR, $ prefixes."""
    result = validate_float(value, min_val=min_val)
    result.field_type = "currency"
    return result


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATOR DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

_VALIDATORS: dict[str, Callable] = {
    "date"       : validate_date,
    "datetime"   : validate_datetime,
    "integer"    : validate_integer,
    "float"      : validate_float,
    "url"        : validate_url,
    "nik"        : validate_nik,
    "npwp"       : validate_npwp,
    "postal_code": validate_postal_code,
    "text"       : validate_text,
    "gender"     : validate_gender,
    "currency"   : validate_currency,
}


def validate_format(value: Any, field_type: FieldType, **kwargs) -> FormatValidationResult:
    """
    Generic format validator — dispatches to the right validator.

    Args:
        value      : Value to validate.
        field_type : One of the supported FieldType literals.
        **kwargs   : Extra args forwarded to the specific validator
                     (e.g. min_val, max_val, pattern, country).

    Returns:
        FormatValidationResult
    """
    validator = _VALIDATORS.get(field_type)
    if not validator:
        raise ValueError(
            f"Unknown field_type '{field_type}'. "
            f"Supported: {list(_VALIDATORS.keys())}"
        )
    return validator(value, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# BATCH DATAFRAME VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class FormatValidator:
    """
    Batch format validator for DataFrames.

    Define a schema: column name → field type (+ options).
    Run validate_dataframe() → get back annotated DataFrame + report.

    Example schema:
        schema = {
            "Tanggal Lahir" : {"type": "date"},
            "NIK"           : {"type": "nik"},
            "Email"         : {"type": "text", "min_len": 5, "max_len": 100},
            "Harga"         : {"type": "currency", "min_val": 0},
            "Website"       : {"type": "url"},
            "Kode Pos"      : {"type": "postal_code", "country": "ID"},
        }
    """

    def __init__(self, schema: dict[str, dict]) -> None:
        """
        Args:
            schema : Dict mapping column name → {"type": FieldType, **options}.
        """
        self.schema = schema

    def validate_dataframe(
        self,
        df      : pd.DataFrame,
        verbose : bool = False,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate all schema columns in the DataFrame.

        Returns:
            (annotated_df, report_df)
            - annotated_df : Original df + _fmt_valid + _fmt_reason columns
            - report_df    : Per-column validation stats
        """
        df = df.copy()
        report_rows = []

        for col, options in self.schema.items():
            field_type = options.get("type", "text")
            opts       = {k: v for k, v in options.items() if k != "type"}

            if col not in df.columns:
                logger.warning(f"⚠️  Schema column '{col}' not in DataFrame — skipping.")
                continue

            results = df[col].apply(
                lambda v, ft=field_type, o=opts: validate_format(v, ft, **o)
            )

            valid_mask    = results.apply(lambda r: r.is_valid)
            invalid_count = (~valid_mask).sum()

            # annotate in-place
            fmt_col    = f"_fmt_{col}"
            reason_col = f"_fmt_reason_{col}"
            df[fmt_col]    = results.apply(lambda r: r.normalized if r.is_valid else "")
            df[reason_col] = results.apply(lambda r: "" if r.is_valid else r.reason)

            report_rows.append({
                "column"       : col,
                "field_type"   : field_type,
                "total"        : len(df),
                "valid"        : int(valid_mask.sum()),
                "invalid"      : int(invalid_count),
                "valid_pct"    : round(valid_mask.mean() * 100, 2),
            })

            if verbose or invalid_count > 0:
                logger.info(
                    f"  📋 [{field_type}] '{col}': "
                    f"{valid_mask.sum()}/{len(df)} valid | {invalid_count} invalid"
                )

        report_df = pd.DataFrame(report_rows)
        return df, report_df


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_format(value: Any, field_type: FieldType, **kwargs) -> bool:
    """Quick True/False check for a single value."""
    return validate_format(value, field_type, **kwargs).is_valid


def normalize_value(value: Any, field_type: FieldType, **kwargs) -> str:
    """Return normalized string for a valid value, empty string if invalid."""
    result = validate_format(value, field_type, **kwargs)
    return result.normalized if result.is_valid else ""


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Format Validator — Quick Test")
    print("=" * 65)

    tests: list[tuple[Any, FieldType, dict]] = [
        # Date
        ("23/06/2024",           "date",        {}),
        ("2024-06-23",           "date",        {}),
        ("23 June 2024",         "date",        {}),
        ("32/13/2024",           "date",        {}),   # ❌
        # DateTime
        ("23/06/2024 14:30:00",  "datetime",    {}),
        ("2024-06-23T08:00:00Z", "datetime",    {}),
        # Integer
        ("42",                   "integer",     {}),
        ("99",                   "integer",     {"max_val": 50}),   # ❌
        # Float
        ("1.500,75",             "float",       {}),   # European format
        ("15000.50",             "float",       {}),
        # URL
        ("https://talatee.com",  "url",         {}),
        ("talatee.com",          "url",         {}),   # auto-prepend https
        ("not a url!!",          "url",         {}),   # ❌
        # NIK
        ("3578012306900001",     "nik",         {}),
        ("123456789",            "nik",         {}),   # ❌ not 16 digits
        # NPWP
        ("01.234.567.8-901.234", "npwp",        {}),
        ("012345678901234",      "npwp",        {}),   # 15 digits raw
        # Postal Code
        ("60111",                "postal_code", {"country": "ID"}),
        ("00000",                "postal_code", {"country": "ID"}),  # ❌
        # Gender
        ("Laki-laki",            "gender",      {}),
        ("female",               "gender",      {}),
        ("X",                    "gender",      {}),   # ❌
        # Currency
        ("Rp 150.000",           "currency",    {}),
        ("$ 99.99",              "currency",    {}),
    ]

    print(f"\n{'VALUE':<26} {'TYPE':<12} {'OK':<5} {'NORMALIZED':<25} {'REASON'}")
    print("─" * 95)
    for value, ftype, opts in tests:
        r = validate_format(value, ftype, **opts)
        ok   = "✅" if r.is_valid else "❌"
        norm = r.normalized[:23] if r.normalized else ""
        rsn  = r.reason[:30] if r.reason else ""
        print(f"{str(value):<26} {ftype:<12} {ok:<5} {norm:<25} {rsn}")

    # ── Batch / DataFrame test ─────────────────────────────────────────────
    print("\n" + "─" * 65)
    print("  BATCH VALIDATION — DataFrame Test")
    print("─" * 65)

    sample = pd.DataFrame({
        "NIK"           : ["3578012306900001", "123", "3578016508950002"],
        "Tanggal Lahir" : ["23/06/1990", "99/99/2024", "01-01-2000"],
        "Kode Pos"      : ["60111", "00000", "40111"],
        "Gender"        : ["Laki-laki", "X", "Perempuan"],
    })

    schema = {
        "NIK"           : {"type": "nik"},
        "Tanggal Lahir" : {"type": "date"},
        "Kode Pos"      : {"type": "postal_code", "country": "ID"},
        "Gender"        : {"type": "gender"},
    }

    validator = FormatValidator(schema)
    annotated_df, report_df = validator.validate_dataframe(sample, verbose=True)

    print("\n📊 Validation Report:")
    print(report_df.to_string(index=False))