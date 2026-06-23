"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Email Validator                   ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : validators/email_validator.py
Purpose : Validate email addresses with multi-level checks:
          Level 1 → Format / Regex
          Level 2 → Domain structure (MX-like check)
          Level 3 → Disposable email detection
          Level 4 → Batch validation with detailed report

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# RFC 5322 compliant regex (practical version)
_EMAIL_REGEX = re.compile(
    r"^(?![.\-_])"                   # tidak boleh mulai dengan . - _
    r"[a-zA-Z0-9._%+\-]{1,64}"      # local part maks 64 char
    r"@"
    r"(?:[a-zA-Z0-9\-]{1,63}\.)"    # domain label
    r"+[a-zA-Z]{2,10}$",            # TLD 2-10 char
    re.IGNORECASE,
)

# Disposable / temporary email domains yang umum dipakai
_DISPOSABLE_DOMAINS: set[str] = {
    "mailinator.com", "guerrillamail.com", "tempmail.com", "throwam.com",
    "yopmail.com", "sharklasers.com", "guerrillamailblock.com", "grr.la",
    "guerrillamail.info", "spam4.me", "trashmail.com", "dispostable.com",
    "maildrop.cc", "discard.email", "fakeinbox.com", "tempr.email",
    "getnada.com", "mailnull.com", "spamgourmet.com", "trashmail.at",
    "trashmail.io", "spamhereplease.com", "10minutemail.com",
    "emailondeck.com", "temp-mail.org", "throwaway.email",
}

# Domain populer yang pasti valid (skip DNS check)
_TRUSTED_DOMAINS: set[str] = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "live.com", "msn.com", "protonmail.com", "zoho.com", "yandex.com",
    # Indonesia
    "yahoo.co.id", "gmail.co.id",
}


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EmailValidationResult:
    """
    Detailed result of a single email validation.

    Attributes:
        email       : Original input string.
        is_valid    : True if email passes all enabled checks.
        reason      : Failure reason code (empty string if valid).
        normalized  : Lowercased, stripped email (or empty if invalid).
        domain      : Extracted domain (or empty if unparseable).
        is_disposable: True if domain is in the disposable blocklist.
        dns_checked : True if DNS/socket lookup was attempted.
        dns_reachable: True if DNS resolved successfully.
    """
    email         : str
    is_valid      : bool
    reason        : str  = ""
    normalized    : str  = ""
    domain        : str  = ""
    is_disposable : bool = False
    dns_checked   : bool = False
    dns_reachable : bool = False

    @property
    def summary(self) -> str:
        if self.is_valid:
            return f"✅ {self.email} → valid"
        return f"❌ {self.email} → {self.reason}"


# ─────────────────────────────────────────────────────────────────────────────
# CORE VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class EmailValidator:
    """
    Multi-level email validator.

    Args:
        check_dns        : Attempt DNS lookup to verify domain exists.
                           Adds ~1-3s per unique domain. Default False
                           (safe for batch processing).
        block_disposable : Reject known disposable email domains. Default True.
        dns_timeout      : Seconds to wait for DNS lookup. Default 3.
    """

    def __init__(
        self,
        check_dns        : bool = False,
        block_disposable : bool = True,
        dns_timeout      : int  = 3,
    ) -> None:
        self.check_dns        = check_dns
        self.block_disposable = block_disposable
        self.dns_timeout      = dns_timeout
        self._dns_cache: dict[str, bool] = {}   # cache per session

    # ── private ───────────────────────────────────────────────────────────────

    def _extract_domain(self, email: str) -> Optional[str]:
        parts = email.rsplit("@", 1)
        return parts[1].lower().strip() if len(parts) == 2 else None

    def _check_format(self, email: str) -> bool:
        return bool(_EMAIL_REGEX.match(email))

    def _check_disposable(self, domain: str) -> bool:
        return domain in _DISPOSABLE_DOMAINS

    def _check_dns(self, domain: str) -> bool:
        """Return True if domain resolves (has A/MX record reachable)."""
        if domain in _TRUSTED_DOMAINS:
            return True
        if domain in self._dns_cache:
            return self._dns_cache[domain]
        try:
            socket.setdefaulttimeout(self.dns_timeout)
            socket.getaddrinfo(domain, None)
            self._dns_cache[domain] = True
            return True
        except (socket.gaierror, socket.timeout, OSError):
            self._dns_cache[domain] = False
            return False

    # ── public: single validate ───────────────────────────────────────────────

    def validate(self, email: str) -> EmailValidationResult:
        """
        Validate a single email address.

        Returns:
            EmailValidationResult with full detail.
        """
        if not email or not isinstance(email, str):
            return EmailValidationResult(
                email=str(email), is_valid=False, reason="empty_or_null"
            )

        normalized = email.strip().lower()

        # ── Level 1: Format ───────────────────────────────────────────────────
        if not self._check_format(normalized):
            return EmailValidationResult(
                email=email, is_valid=False,
                reason="invalid_format", normalized=normalized,
            )

        domain = self._extract_domain(normalized)
        if not domain:
            return EmailValidationResult(
                email=email, is_valid=False,
                reason="no_domain", normalized=normalized,
            )

        # ── Level 2: Disposable ───────────────────────────────────────────────
        is_disposable = self._check_disposable(domain)
        if self.block_disposable and is_disposable:
            return EmailValidationResult(
                email=email, is_valid=False,
                reason="disposable_domain", normalized=normalized,
                domain=domain, is_disposable=True,
            )

        # ── Level 3: DNS ──────────────────────────────────────────────────────
        dns_checked   = False
        dns_reachable = False
        if self.check_dns:
            dns_checked   = True
            dns_reachable = self._check_dns(domain)
            if not dns_reachable:
                return EmailValidationResult(
                    email=email, is_valid=False,
                    reason="dns_not_found", normalized=normalized,
                    domain=domain, is_disposable=is_disposable,
                    dns_checked=True, dns_reachable=False,
                )

        return EmailValidationResult(
            email=email, is_valid=True,
            normalized=normalized, domain=domain,
            is_disposable=is_disposable,
            dns_checked=dns_checked, dns_reachable=dns_reachable,
        )

    # ── public: batch validate ────────────────────────────────────────────────

    def validate_batch(
        self,
        emails: list[str],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Validate a list of emails and return a summary DataFrame.

        Args:
            emails  : List of email strings.
            verbose : If True, log each result. Default False.

        Returns:
            DataFrame with columns:
                email, is_valid, reason, normalized, domain, is_disposable
        """
        results = []
        for email in emails:
            r = self.validate(email)
            if verbose:
                logger.debug(r.summary)
            results.append({
                "email"        : r.email,
                "is_valid"     : r.is_valid,
                "reason"       : r.reason,
                "normalized"   : r.normalized,
                "domain"       : r.domain,
                "is_disposable": r.is_disposable,
            })

        df    = pd.DataFrame(results)
        valid = df["is_valid"].sum()
        total = len(df)
        logger.info(
            f"📧 Email batch validation: {valid}/{total} valid "
            f"| {total - valid} invalid"
        )
        return df


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTION  (used by main.py step_validate)
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_email(
    email: str,
    check_dns: bool = False,
    block_disposable: bool = True,
) -> bool:
    """
    Quick single-email check. Returns True/False.

    Used directly by main.py:
        from validators.email_validator import is_valid_email
        mask = df["Email"].apply(is_valid_email)

    Args:
        email            : Email string to check.
        check_dns        : Run DNS lookup (slower). Default False.
        block_disposable : Block throwaway domains. Default True.
    """
    validator = EmailValidator(
        check_dns=check_dns,
        block_disposable=block_disposable,
    )
    return validator.validate(email).is_valid


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Email Validator — Quick Test")
    print("=" * 60)

    test_emails = [
        "user@gmail.com",           # ✅ valid
        "user.name+tag@outlook.com",# ✅ valid
        "invalid-email",            # ❌ format
        "@nodomain.com",            # ❌ format
        "user@.com",                # ❌ format
        "user@mailinator.com",      # ❌ disposable
        "user@tempmail.com",        # ❌ disposable
        "",                         # ❌ empty
        "valid@yahoo.co.id",        # ✅ valid (Indonesia)
        "double@@domain.com",       # ❌ format
    ]

    validator = EmailValidator(check_dns=False, block_disposable=True)
    df = validator.validate_batch(test_emails, verbose=True)

    print("\n📊 Batch Validation Result:")
    print(df.to_string(index=False))
    print(f"\n✅ Valid  : {df['is_valid'].sum()}")
    print(f"❌ Invalid: {(~df['is_valid']).sum()}")