# backend/app/utils/text_utils.py
"""
Text utilities for extractors (SunEtLune / PEAK A–U)

Goals:
- Normalize text safely for extraction (keep newlines!)
- Convert Thai digits to Arabic
- Normalize Unicode & punctuation variants
- Reduce OCR noise without destroying important tokens (invoice/ref/doc ids)
- Provide helper utilities used across extract/export:
  - compact_no_ws (for C_reference / G_invoice_no)
  - clean_number_string (more robust)
  - normalize_filename_token / extract_doc_ref_from_filename
  - fix_ocr_digits_in_numeric_context (O->0, I/l->1 when surrounded by digits)
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional, Tuple

# -------------------------
# Thai digit mapping
# -------------------------
THAI_DIGITS = "๐๑๒๓๔๕๖๗๘๙"
ARABIC_DIGITS = "0123456789"
THAI_TO_ARABIC = str.maketrans(THAI_DIGITS, ARABIC_DIGITS)

# -------------------------
# Common punctuation variants
# -------------------------
# dash variants: hyphen, en-dash, em-dash, minus, Thai dash, fullwidth hyphen
_DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2212\uFE63\uFF0D\u0E3F"
RE_DASHES = re.compile(rf"[{_DASH_CHARS}]+")
RE_MULTI_SPACE = re.compile(r"[ \t]+")
RE_ALL_WS = re.compile(r"\s+")

# Zero-width / control chars that often appear from PDF extract / OCR
RE_CONTROL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
RE_ZW = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060]")  # ZWSP + bidi marks etc.

# For Thai checks
RE_THAI = re.compile(r"[\u0E00-\u0E7F]")

# Numeric cleaning
RE_AMOUNT_JUNK = re.compile(r"[,\s]|฿|THB|บาท|Baht", re.IGNORECASE)

# Shopee doc/ref tokens often look like:
# TRSPEMKP00-00000-251203-0012589
# Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf
RE_SHOPEE_DOCREF = re.compile(
    r"\b([A-Z]{2,12}[A-Z0-9]{2,20}-\d{5}-\d{6}-\d{7})\b", re.IGNORECASE
)


def _nfc(s: str) -> str:
    try:
        return unicodedata.normalize("NFC", s)
    except Exception:
        return s


def _normalize_punct(s: str) -> str:
    """
    Normalize punctuation variants:
    - Convert dash variants to '-'
    - Convert fullwidth chars to normal via NFKC then back to NFC
    """
    try:
        # NFKC can normalize fullwidth letters/numbers/punct
        s2 = unicodedata.normalize("NFKC", s)
    except Exception:
        s2 = s
    s2 = RE_DASHES.sub("-", s2)
    return _nfc(s2)


def normalize_text(text: Optional[str]) -> str:
    """
    Normalize text for extraction:
    - Convert Thai digits -> Arabic
    - Unicode normalize
    - Remove control & zero-width chars
    - Normalize dash variants
    - Collapse multiple spaces but KEEP newlines
    - Keep original structure as much as possible (do NOT drop lines)
      (Dropping empty lines can break multi-line patterns in OCR/PDF)

    IMPORTANT:
    - Do NOT remove Thai 'ๆ' (it can be part of names/addresses; removing may harm extraction)
    """
    if not text:
        return ""

    s = str(text)

    # Thai digits -> Arabic
    s = s.translate(THAI_TO_ARABIC)

    # Normalize punct / width / unicode
    s = _normalize_punct(s)

    # Remove control chars + zero-width chars
    s = RE_CONTROL.sub("", s)
    s = RE_ZW.sub("", s)

    # Normalize whitespace per-line, but keep newlines
    lines = s.splitlines()
    out_lines = []
    for line in lines:
        line = line.replace("\r", "")
        line = RE_MULTI_SPACE.sub(" ", line).strip()
        # Keep line even if empty? We keep single empty lines by default to preserve blocks.
        # But we avoid producing huge consecutive empty blocks.
        out_lines.append(line)

    # Reduce multiple empty lines to max 1
    normalized = "\n".join(out_lines)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()

    return normalized


def compact_no_ws(text: Optional[str]) -> str:
    """
    Remove ALL whitespace (spaces/newlines/tabs) and normalize punctuation.
    Used for IDs / doc refs: C_reference, G_invoice_no.

    Example:
      "TRSPEMKP00-00000-251203-0012589" -> same
      "TRSPEMKP00 - 00000 - 251203 - 0012589" -> "TRSPEMKP00-00000-251203-0012589"
    """
    if not text:
        return ""
    s = _normalize_punct(str(text))
    s = RE_ALL_WS.sub("", s)
    return s.strip()


def clean_number_string(s: str) -> str:
    """
    Clean number string: remove commas, currency, spaces.
    Keep digits and at most one decimal point.
    Also normalizes Thai digits and dash variants.
    """
    if not s:
        return ""
    x = normalize_text(s)
    x = RE_AMOUNT_JUNK.sub("", x).strip()

    # keep only digits and dots; but allow leading '-'? (most expenses are positive; keep minus if present)
    neg = x.startswith("-")
    x = re.sub(r"[^\d.]", "", x)

    if x.count(".") > 1:
        # keep first dot only
        parts = x.split(".")
        x = parts[0] + "." + "".join(parts[1:])

    if neg and x and not x.startswith("-"):
        x = "-" + x
    return x


def extract_thai_text(text: str) -> str:
    """
    Extract only Thai characters (and spaces) from text.
    """
    if not text:
        return ""
    thai_chars = re.findall(r"[\u0E00-\u0E7F\s]+", str(text))
    return " ".join([t.strip() for t in thai_chars if t.strip()]).strip()


def is_thai_text(text: str, threshold: float = 0.3) -> bool:
    """
    Check if text contains significant Thai content.
    """
    if not text:
        return False
    s = str(text).strip()
    if len(s) < 3:
        return False

    thai_count = len(RE_THAI.findall(s))
    total_chars = len([c for c in s if not c.isspace()])

    if total_chars <= 0:
        return False
    return (thai_count / total_chars) >= threshold


def fix_ocr_digits_in_numeric_context(text: str) -> str:
    """
    Fix common OCR confusions ONLY when near digits:
    - 'O'/'o' -> '0'
    - 'I'/'l' -> '1'
    This is intentionally conservative to avoid damaging names.

    Example:
      "Seller ID 16464655O5" -> "Seller ID 1646465505"
    """
    if not text:
        return ""
    s = str(text)

    # O->0 when surrounded by digits
    s = re.sub(r"(?<=\d)[Oo](?=\d)", "0", s)
    # I/l->1 when surrounded by digits
    s = re.sub(r"(?<=\d)[Il](?=\d)", "1", s)
    return s


def normalize_filename_token(filename: Optional[str]) -> str:
    """
    Normalize filename for matching:
    - basename only
    - normalize unicode & dash
    - keep original case but also useful for comparisons
    """
    if not filename:
        return ""
    s = str(filename).strip()
    # remove path parts if present
    s = s.replace("\\", "/")
    s = s.split("/")[-1]
    s = _normalize_punct(s)
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s


def extract_doc_ref_from_filename(filename: Optional[str]) -> str:
    """
    Extract Shopee-style doc reference token from filename.

    Examples:
      "Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf" -> "TRSPEMKP00-00000-251203-0012589"
      "TRSPEMKP00-00000-251203-0012589" -> same

    Returns empty string if not found.
    """
    fn = normalize_filename_token(filename)
    if not fn:
        return ""

    # remove extension for better match
    base = fn.rsplit(".", 1)[0]

    m = RE_SHOPEE_DOCREF.search(base)
    if not m:
        return ""

    ref = m.group(1)
    ref = ref.upper()
    ref = compact_no_ws(ref)
    return ref


def extract_seller_id_and_username(text: str) -> Tuple[str, str]:
    """
    Best-effort extraction for:
      Seller ID <digits>
      Username <word/slug>
    Used for L_description pattern building.

    Returns (seller_id, username) (may be empty strings).
    """
    if not text:
        return ("", "")

    s = normalize_text(text)
    s = fix_ocr_digits_in_numeric_context(s)

    # Seller ID patterns: "Seller ID 1646465545" or "SellerID: 1646465545"
    m = re.search(r"\bSeller\s*ID\s*[:#]?\s*(\d{5,})\b", s, re.IGNORECASE)
    seller_id = m.group(1) if m else ""

    # Username patterns often appear near seller id lines or "Username: xxx"
    u = ""
    m2 = re.search(r"\bUsername\s*[:#]?\s*([A-Za-z0-9._-]{2,64})\b", s, re.IGNORECASE)
    if m2:
        u = m2.group(1)

    # fallback: try "Shop name" key
    if not u:
        m3 = re.search(r"\bShop\s*Name\s*[:#]?\s*(.{2,80})$", s, re.IGNORECASE | re.MULTILINE)
        if m3:
            cand = m3.group(1).strip()
            cand = cand.split("  ")[0].strip()
            cand = re.sub(r"[^\w.\-]+", " ", cand).strip()
            if 2 <= len(cand) <= 64:
                u = cand

    return (seller_id, u)


__all__ = [
    "normalize_text",
    "compact_no_ws",
    "clean_number_string",
    "extract_thai_text",
    "is_thai_text",
    "fix_ocr_digits_in_numeric_context",
    "normalize_filename_token",
    "extract_doc_ref_from_filename",
    "extract_seller_id_and_username",
]
