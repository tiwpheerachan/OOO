# backend/app/extractors/wallet_mapping.py
from __future__ import annotations

"""
Wallet Mapping System — PEAK Importer (Q_payment_method)

Goal:
- Fill PEAK column Q_payment_method ("ชำระโดย") with wallet code EWLxxx
- Use our company (client_tax_id) + seller/shop identity to map reliably

Design:
- Primary key: seller_id (digits)
- Fallback: shop_name / label keywords (normalized string)
- Optional: extract seller_id from OCR text ("Seller ID: ...", "Shop ID=...")
- Robust normalization (Thai digits, whitespace, punctuation)

Behavior:
- Return "" if cannot resolve (caller should mark NEEDS_REVIEW)
- NEVER return platform name (Shopee/Lazada/etc.)
"""

from typing import Dict, Optional, Tuple, List
import re

# ============================================================
# Client Tax ID Constants (our companies)
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD    = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Wallet mappings by seller_id (digits only)
# ============================================================

# Rabbit wallets
RABBIT_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "0000000001": "EWL001",  # Shopee-70mai
    "0000000002": "EWL002",  # Shopee-ddpai
    "0000000003": "EWL003",  # Shopee-jimmy
    "0000000004": "EWL004",  # Shopee-mibro
    "0000000006": "EWL006",  # Shopee-toptoy
    "0000000007": "EWL007",  # Shopee-uwant
    "0000000008": "EWL008",  # Shopee-wanbo
    "0000000009": "EWL009",  # Shopee-zepp
    "142025022504068027": "EWL010",  # Rabbit (Rabbit)
}

# SHD wallets
SHD_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "628286975":  "EWL001",  # Shopee-ankerthailandstore
    "340395201":  "EWL002",  # Shopee-dreamofficial
    "383844799":  "EWL003",  # Shopee-levoitofficialstore
    "261472748":  "EWL004",  # Shopee-soundcoreofficialstore
    "517180669":  "EWL005",  # xiaomismartappliances  (คุณระบุว่าเป็น MOVA/SHD)
    "426162640":  "EWL006",  # Shopee-xiaomi.thailand
    "231427130":  "EWL007",  # xiaomi_home_appliances
    "1646465545": "EWL008",  # Shopee-nextgadget
}

# TopOne wallets
TOPONE_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "538498056": "EWL001",  # Shopee-Vinkothailandstore
}

# ============================================================
# Fallback mapping by shop name keywords (normalized lowercase)
# (Use when seller_id missing)
# ============================================================

RABBIT_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-70mai":  "EWL001",
    "shopee-ddpai":  "EWL002",
    "shopeejimmy":   "EWL003",
    "shopee-jimmy":  "EWL003",
    "shopee-mibro":  "EWL004",
    "shopee-toptoy": "EWL006",
    "shopee-uwant":  "EWL007",
    "shopee-wanbo":  "EWL008",
    "shopee-zepp":   "EWL009",
    "rabbit":        "EWL010",
}

SHD_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-ankerthailandstore":   "EWL001",
    "shopee-dreamofficial":        "EWL002",
    "shopee-levoitofficialstore":  "EWL003",
    "shopee-soundcoreofficialstore":"EWL004",
    "xiaomismartappliances":       "EWL005",
    "shopee-xiaomi.thailand":      "EWL006",
    "xiaomi_home_appliances":      "EWL007",
    "shopee-nextgadget":           "EWL008",
    "nextgadget":                  "EWL008",
}

TOPONE_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-vinkothailandstore": "EWL001",
    "vinkothailandstore":        "EWL001",
}

# ============================================================
# Regex for extracting seller/shop ids from OCR text
# ============================================================
SELLER_ID_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bseller(?:\s*id)?\s*[:#=]?\s*([0-9]{5,20})\b", re.IGNORECASE),
    re.compile(r"\bshop(?:\s*id)?\s*[:#=]?\s*([0-9]{5,20})\b", re.IGNORECASE),
    re.compile(r"\bmerchant(?:\s*id)?\s*[:#=]?\s*([0-9]{5,20})\b", re.IGNORECASE),
]

EWL_RE = re.compile(r"^EWL\d{3}$", re.IGNORECASE)

# ============================================================
# Normalization helpers
# ============================================================
def _thai_digits_to_arabic(s: str) -> str:
    # ๐๑๒๓๔๕๖๗๘๙ -> 0123456789
    th = "๐๑๒๓๔๕๖๗๘๙"
    ar = "0123456789"
    trans = str.maketrans({th[i]: ar[i] for i in range(10)})
    return s.translate(trans)

def _norm_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    # unify whitespace/newlines
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _digits_only(s: str) -> str:
    if not s:
        return ""
    s = _thai_digits_to_arabic(str(s))
    return "".join(ch for ch in s if ch.isdigit())

def _norm_seller_id(seller_id: str) -> str:
    """
    Normalize seller_id: digits only (remove comma/space/hyphen)
    """
    d = _digits_only(seller_id)
    return d

def _norm_shop_name(shop_name: str) -> str:
    """
    Normalize shop name for keyword matching:
    - lowercase
    - remove extra spaces
    """
    s = _norm_text(shop_name).lower()
    return s

def _extract_seller_id_from_text(text: str) -> str:
    """
    Extract seller_id from OCR/body text.
    """
    t = _norm_text(text).lower()
    if not t:
        return ""
    for rx in SELLER_ID_PATTERNS:
        m = rx.search(t)
        if m:
            return _norm_seller_id(m.group(1))
    return ""

def _is_valid_wallet(code: str) -> bool:
    return bool(code) and bool(EWL_RE.match(code.strip()))

def _client_bucket(client_tax_id: str) -> str:
    """
    Return: 'RABBIT' | 'SHD' | 'TOPONE' | ''
    """
    d = _digits_only(client_tax_id)
    if d == CLIENT_RABBIT:
        return "RABBIT"
    if d == CLIENT_SHD:
        return "SHD"
    if d == CLIENT_TOPONE:
        return "TOPONE"
    return ""

def _tables_for_client(client_bucket: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Return (by_seller_id, by_shop_keyword)
    """
    if client_bucket == "RABBIT":
        return (RABBIT_WALLET_BY_SELLER_ID, RABBIT_WALLET_BY_SHOP_KEYWORD)
    if client_bucket == "SHD":
        return (SHD_WALLET_BY_SELLER_ID, SHD_WALLET_BY_SHOP_KEYWORD)
    if client_bucket == "TOPONE":
        return (TOPONE_WALLET_BY_SELLER_ID, TOPONE_WALLET_BY_SHOP_KEYWORD)
    return ({}, {})

# ============================================================
# Public API
# ============================================================
def resolve_wallet_code(
    client_tax_id: str,
    *,
    seller_id: str = "",
    shop_name: str = "",
    text: str = "",
) -> str:
    """
    Resolve wallet code (EWLxxx) using:
      1) seller_id mapping
      2) extract seller_id from text (if not provided)
      3) shop_name keyword mapping

    Returns:
      - "EWLxxx" if resolved
      - "" if unknown (caller should mark NEEDS_REVIEW)
    """
    bucket = _client_bucket(client_tax_id)
    if not bucket:
        return ""

    by_sid, by_shop = _tables_for_client(bucket)

    sid = _norm_seller_id(seller_id)
    if not sid and text:
        sid = _extract_seller_id_from_text(text)

    if sid:
        code = by_sid.get(sid, "")
        if _is_valid_wallet(code):
            return code

    shop = _norm_shop_name(shop_name)
    if shop:
        # keyword contains match
        for key, code in by_shop.items():
            if not key:
                continue
            if key in shop and _is_valid_wallet(code):
                return code

    return ""

def extract_seller_id_best_effort(text: str) -> str:
    """
    Utility: extract seller_id from OCR text.
    """
    return _extract_seller_id_from_text(text)

__all__ = [
    "resolve_wallet_code",
    "extract_seller_id_best_effort",
    "CLIENT_RABBIT",
    "CLIENT_SHD",
    "CLIENT_TOPONE",
]
