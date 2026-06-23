"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Phone Validator                   ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : validators/phone_validator.py
Purpose : Validate & normalize phone numbers.
          Focused on Indonesian numbers (62xxx) with international fallback.

          Normalization output always: +62XXXXXXXXXX (E.164 format)

          Examples:
            08123456789   → +628123456789  ✅
            8123456789    → +628123456789  ✅
            628123456789  → +628123456789  ✅
            +628123456789 → +628123456789  ✅
            0812-3456-789 → +628123456789  ✅

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS — Indonesian Operators
# ─────────────────────────────────────────────────────────────────────────────

# Prefix valid operator Indonesia (setelah kode negara 62)
_ID_OPERATOR_PREFIXES: dict[str, list[str]] = {
    "Telkomsel"  : ["811","812","813","821","822","823","851","852","853"],
    "Indosat"    : ["814","815","816","855","856","857","858"],
    "XL"         : ["817","818","819","859","877","878"],
    "Axis"       : ["831","832","833","838"],
    "Tri (3)"    : ["895","896","897","898","899"],
    "Smartfren"  : ["881","882","883","884","885","886","887","888","889"],
    "Telkom"     : ["800","801","802","803","804","805","806","807","808","809"],
    "By.U"       : ["851"],
}

# Flat set untuk lookup cepat
_ALL_ID_PREFIXES: set[str] = {
    p for prefixes in _ID_OPERATOR_PREFIXES.values() for p in prefixes
}

# Panjang nomor Indonesia setelah kode negara (62):
# Min 9 digit, Max 13 digit (total dengan 62: 11-15)
_ID_MIN_LEN = 9
_ID_MAX_LEN = 13

# Strip semua karakter non-digit kecuali leading +
_STRIP_PATTERN = re.compile(r"(?<!^\+)[^\d]")


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PhoneValidationResult:
    """
    Result of a single phone number validation.

    Attributes:
        raw          : Original input.
        is_valid     : True if number passes all checks.
        reason       : Failure reason code (empty if valid).
        normalized   : E.164 format (+62XXXXXXXXXX) if valid, else empty.
        country_code : Detected country code string ("62", etc.).
        local_number : Number without country code.
        operator     : Detected Indonesian operator name (if identifiable).
        is_indonesian: True if number is Indonesian (+62).
    """
    raw          : str
    is_valid     : bool
    reason       : str  = ""
    normalized   : str  = ""
    country_code : str  = ""
    local_number : str  = ""
    operator     : str  = "Unknown"
    is_indonesian: bool = False

    @property
    def summary(self) -> str:
        if self.is_valid:
            op = f" [{self.operator}]" if self.operator != "Unknown" else ""
            return f"✅ {self.raw} → {self.normalized}{op}"
        return f"❌ {self.raw} → {self.reason}"


# ─────────────────────────────────────────────────────────────────────────────
# CORE VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class PhoneValidator:
    """
    Phone number validator with Indonesian focus.

    Args:
        default_country : ISO country code for number without country prefix.
                          Default "ID" (Indonesia).
        strict_operator : If True, reject Indonesian numbers with unknown
                          operator prefix. Default False.
    """

    def __init__(
        self,
        default_country : str  = "ID",
        strict_operator : bool = False,
    ) -> None:
        self.default_country  = default_country.upper()
        self.strict_operator  = strict_operator

    # ── private ───────────────────────────────────────────────────────────────

    def _clean(self, raw: str) -> str:
        """Remove formatting chars: spaces, dashes, dots, parentheses."""
        cleaned = raw.strip()
        # preserve leading + for international format
        has_plus = cleaned.startswith("+")
        digits   = re.sub(r"\D", "", cleaned)
        return ("+" + digits) if has_plus else digits

    def _normalize_indonesian(self, digits: str) -> Optional[str]:
        """
        Normalize various Indonesian formats to E.164 (+62XXXXXXXXXX).

        Accepted input (digits only):
            08XXXXXXXXX   → 0 prefix (local format)
            8XXXXXXXXX    → without prefix
            628XXXXXXXXX  → with country code, no +
            628XXXXXXXXX  → already E.164 (after stripping +)
        """
        if digits.startswith("0"):
            digits = digits[1:]         # strip leading 0 → 8XXXXXXXXX

        if digits.startswith("62"):
            digits = digits[2:]         # strip 62 → 8XXXXXXXXX

        # now digits should be 8XXXXXXXXX
        if not digits.startswith("8"):
            return None

        length = len(digits)
        if length < _ID_MIN_LEN or length > _ID_MAX_LEN:
            return None

        return "+62" + digits

    def _detect_operator(self, local: str) -> str:
        """Detect operator from first 3 digits of local number."""
        prefix3 = local[:3] if len(local) >= 3 else ""
        for operator, prefixes in _ID_OPERATOR_PREFIXES.items():
            if prefix3 in prefixes:
                return operator
        return "Unknown"

    # ── public: single validate ───────────────────────────────────────────────

    def validate(self, phone: str) -> PhoneValidationResult:
        """
        Validate and normalize a single phone number.

        Returns:
            PhoneValidationResult with full detail.
        """
        if not phone or not isinstance(phone, str):
            return PhoneValidationResult(raw=str(phone), is_valid=False, reason="empty_or_null")

        cleaned = self._clean(phone)

        if not cleaned or not re.search(r"\d", cleaned):
            return PhoneValidationResult(raw=phone, is_valid=False, reason="no_digits")

        digits = cleaned.lstrip("+")

        # ── Indonesian number detection ───────────────────────────────────────
        is_indonesian = (
            digits.startswith("62")
            or digits.startswith("08")
            or digits.startswith("8")
            or cleaned.startswith("+62")
        )

        if is_indonesian or self.default_country == "ID":
            normalized = self._normalize_indonesian(digits)

            if normalized is None:
                return PhoneValidationResult(
                    raw=phone, is_valid=False,
                    reason="invalid_indonesian_format",
                    is_indonesian=True,
                )

            local    = normalized[3:]   # strip +62
            operator = self._detect_operator(local)

            if self.strict_operator and operator == "Unknown":
                return PhoneValidationResult(
                    raw=phone, is_valid=False,
                    reason="unknown_indonesian_operator",
                    normalized=normalized, country_code="62",
                    local_number=local, operator=operator,
                    is_indonesian=True,
                )

            return PhoneValidationResult(
                raw=phone, is_valid=True,
                normalized=normalized, country_code="62",
                local_number=local, operator=operator,
                is_indonesian=True,
            )

        # ── International fallback (basic length check) ───────────────────────
        # E.164: max 15 digits total
        if len(digits) < 7 or len(digits) > 15:
            return PhoneValidationResult(
                raw=phone, is_valid=False,
                reason="invalid_length_international",
            )

        return PhoneValidationResult(
            raw=phone, is_valid=True,
            normalized="+" + digits,
            country_code=digits[:2],
            local_number=digits[2:],
        )

    # ── public: batch validate ────────────────────────────────────────────────

    def validate_batch(
        self,
        phones  : list[str],
        verbose : bool = False,
    ) -> pd.DataFrame:
        """
        Validate a list of phone numbers. Returns summary DataFrame.

        Returns DataFrame with columns:
            raw, is_valid, reason, normalized, country_code,
            local_number, operator, is_indonesian
        """
        results = []
        for phone in phones:
            r = self.validate(phone)
            if verbose:
                logger.debug(r.summary)
            results.append({
                "raw"          : r.raw,
                "is_valid"     : r.is_valid,
                "reason"       : r.reason,
                "normalized"   : r.normalized,
                "country_code" : r.country_code,
                "local_number" : r.local_number,
                "operator"     : r.operator,
                "is_indonesian": r.is_indonesian,
            })

        df    = pd.DataFrame(results)
        valid = df["is_valid"].sum()
        total = len(df)
        logger.info(
            f"📱 Phone batch validation: {valid}/{total} valid "
            f"| {total - valid} invalid"
        )
        return df


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION  (used by main.py step_validate)
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_phone(
    phone          : str,
    default_country: str  = "ID",
    strict_operator: bool = False,
) -> bool:
    """
    Quick single-phone check. Returns True/False.

    Used directly by main.py:
        from validators.phone_validator import is_valid_phone
        mask = df["Phone"].apply(is_valid_phone)
    """
    validator = PhoneValidator(
        default_country=default_country,
        strict_operator=strict_operator,
    )
    return validator.validate(phone).is_valid


def normalize_phone(phone: str, default_country: str = "ID") -> str:
    """
    Normalize a phone number to E.164 format.
    Returns empty string if invalid.

    Useful for storage/dedup:
        df["phone_normalized"] = df["Phone"].apply(normalize_phone)
    """
    result = PhoneValidator(default_country=default_country).validate(phone)
    return result.normalized if result.is_valid else ""


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Phone Validator — Quick Test (Indonesia Focus)")
    print("=" * 60)

    test_phones = [
        "08123456789",          # ✅ Telkomsel (local 0 prefix)
        "8123456789",           # ✅ Telkomsel (no prefix)
        "628123456789",         # ✅ Telkomsel (with 62)
        "+628123456789",        # ✅ Telkomsel (E.164)
        "0812-3456-789",        # ✅ Telkomsel (formatted)
        "0812 3456 789",        # ✅ Telkomsel (spaces)
        "0855-1234-5678",       # ✅ Indosat
        "0878-9999-0000",       # ✅ XL
        "0895-6789-0123",       # ✅ Tri
        "0812",                 # ❌ terlalu pendek
        "123",                  # ❌ terlalu pendek
        "",                     # ❌ kosong
        "abcdefgh",             # ❌ bukan angka
        "+14155552671",         # ✅ US number (international)
    ]

    validator = PhoneValidator()
    df = validator.validate_batch(test_phones, verbose=True)

    print("\n📊 Batch Validation Result:")
    print(df[["raw","is_valid","normalized","operator","reason"]].to_string(index=False))
    print(f"\n✅ Valid  : {df['is_valid'].sum()}")
    print(f"❌ Invalid: {(~df['is_valid']).sum()}")

    print("\n📋 Operator Distribution (valid only):")
    print(df[df["is_valid"]]["operator"].value_counts().to_string())