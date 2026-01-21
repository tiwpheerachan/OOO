from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

# -------------------------
# Regex helpers
# -------------------------
RE_YYYYMMDD = re.compile(r"^\d{8}$")
RE_BRANCH5 = re.compile(r"^\d{5}$")
RE_TAX13 = re.compile(r"^\d{13}$")

# Extract digits from messy OCR values
RE_DIGITS = re.compile(r"\d+")
RE_DATE_ANY = re.compile(r"(\d{4})\D?(\d{2})\D?(\d{2})")  # 2025-12-03 / 20251203 / 2025/12/03

# VAT tokens
RE_VAT_NO = re.compile(r"\b(NO\s*VAT|VAT\s*EXEMPT|REVERSE\s*CHARGE|NO)\b", re.IGNORECASE)
RE_VAT_7 = re.compile(r"\b7\s*%?\b", re.IGNORECASE)

PRICE_TYPES = {"", "1", "2", "3"}  # 1=VAT separated, 2=included, 3=no vat


def _s(v: Optional[str]) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _digits_only(v: str) -> str:
    return "".join(ch for ch in v if ch.isdigit())


# -------------------------
# Sanitizers (recommended to use before validate)
# -------------------------
def sanitize_yyyymmdd(v: str) -> str:
    """
    Accepts messy date strings and tries to return YYYYMMDD.
    - keeps empty as ""
    - accepts YYYYMMDD already
    - accepts YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
    """
    s = _s(v)
    if not s:
        return ""
    s2 = s.replace("\n", " ").strip()

    if RE_YYYYMMDD.fullmatch(s2):
        return s2

    m = RE_DATE_ANY.search(s2)
    if not m:
        return ""

    y, mo, d = m.group(1), m.group(2), m.group(3)
    candidate = f"{y}{mo}{d}"
    return candidate if validate_yyyymmdd(candidate) else ""


def sanitize_branch5(v: str) -> str:
    """
    Branch code should be 5 digits. OCR might produce: '00000-', '00000.', 'สาขา 00000'
    - returns "00000" if digits exist but not 5? (only when clearly 0/blank)
    - otherwise returns "" if cannot sanitize safely
    """
    s = _s(v)
    if not s:
        return ""

    digits = _digits_only(s)
    if not digits:
        return ""

    # If digits longer than 5, take last 5 (often OCR includes prefixes like "สาขา 00000")
    if len(digits) > 5:
        digits = digits[-5:]

    if len(digits) < 5:
        # If it's all zeros (e.g., "0" or "00") -> treat as "00000"
        if set(digits) <= {"0"}:
            return "00000"
        return ""

    return digits if RE_BRANCH5.fullmatch(digits) else ""


def sanitize_tax13(v: str) -> str:
    """
    Vendor Tax ID must be exactly 13 digits.
    - strips anything not digit
    - if >13 digits, takes first 13 (OCR often appends noise)
    """
    s = _s(v)
    if not s:
        return ""

    digits = _digits_only(s)
    if len(digits) < 13:
        return ""
    digits = digits[:13]
    return digits if RE_TAX13.fullmatch(digits) else ""


def sanitize_price_type(v: str) -> str:
    """
    Only allow "", "1", "2", "3".
    Accepts int-like strings.
    """
    s = _s(v)
    if not s:
        return ""
    s2 = _digits_only(s)[:1]  # "1", "2", "3"
    return s2 if s2 in PRICE_TYPES else ""


def sanitize_vat_rate(v: str) -> str:
    """
    Project rule: VAT rate must be "7%" or "NO" (or empty).
    Accepts messy OCR like: "7", "7 %", "VAT 7%", "No VAT", "reverse charge"
    """
    s = _s(v)
    if not s:
        return ""

    s2 = s.replace("\n", " ").strip()

    if RE_VAT_NO.search(s2):
        return "NO"
    if RE_VAT_7.search(s2):
        return "7%"

    # If someone passed "7%" already (or "7")
    if s2 == "7":
        return "7%"
    if s2.upper() == "NO":
        return "NO"

    return ""


# -------------------------
# Validators (backward compatible)
# -------------------------
def validate_yyyymmdd(v: str) -> bool:
    """
    Return True if:
    - empty (allowed)
    - valid YYYYMMDD date
    """
    s = _s(v)
    if not s:
        return True
    if not RE_YYYYMMDD.fullmatch(s):
        return False
    try:
        datetime.strptime(s, "%Y%m%d")
        return True
    except Exception:
        return False


def validate_branch5(v: str) -> bool:
    """
    Allow empty. Otherwise must be exactly 5 digits.
    Also accepts messy input by sanitizing first.
    """
    s = _s(v)
    if not s:
        return True
    s2 = sanitize_branch5(s)
    return bool(s2) and RE_BRANCH5.fullmatch(s2) is not None


def validate_tax13(v: str) -> bool:
    """
    Allow empty. Otherwise must be exactly 13 digits.
    Also accepts messy input by sanitizing first.
    """
    s = _s(v)
    if not s:
        return True
    s2 = sanitize_tax13(s)
    return bool(s2) and RE_TAX13.fullmatch(s2) is not None


def validate_price_type(v: str) -> bool:
    """
    Allow "", "1", "2", "3". Also accepts messy input by sanitizing first.
    """
    s2 = sanitize_price_type(v)
    return s2 in PRICE_TYPES


def validate_vat_rate(v: str) -> bool:
    """
    Allow empty, or "7%" or "NO".
    (This matches your pipeline rules and prevents garbage like "7" or "VAT7" leaking.)
    """
    s = _s(v)
    if not s:
        return True
    s2 = sanitize_vat_rate(s)
    return s2 in {"7%", "NO", ""}


__all__ = [
    "sanitize_yyyymmdd",
    "sanitize_branch5",
    "sanitize_tax13",
    "sanitize_price_type",
    "sanitize_vat_rate",
    "validate_yyyymmdd",
    "validate_branch5",
    "validate_tax13",
    "validate_price_type",
    "validate_vat_rate",
]
