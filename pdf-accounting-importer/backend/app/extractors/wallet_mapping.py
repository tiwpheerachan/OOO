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

from typing import Dict, Tuple, List
import re

# ============================================================
# Client Tax ID Constants (our companies)
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Wallet mappings by seller_id (digits only)
# ============================================================

# Rabbit wallets
RABBIT_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "253227155": "EWL001",          # Shopee-70mai
    "235607098": "EWL002",          # Shopee-ddpai
    "516516644": "EWL003",          # Shopee-jimmy
    "1443909809": "EWL004",         # Shopee-mibro
    "1232116856": "EWL005",         # Shopee-MOVA
    "1357179095": "EWL006",         # Shopee-toptoy
    "1416156484": "EWL007",         # Shopee-uwant
    "418530715": "EWL008",          # Shopee-wanbo
    "349400909": "EWL009",          # Shopee-zepp
    "142025022504068027": "EWL010", # Rabbit (Rabbit)
}

# SHD wallets
SHD_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "628286975": "EWL001",          # Shopee-ankerthailandstore
    "340395201": "EWL002",          # Shopee-dreamofficial
    "383844799": "EWL003",          # Shopee-levoitofficialstore
    "261472748": "EWL004",          # Shopee-soundcoreofficialstore
    "517180669": "EWL005",          # xiaomismartappliances
    "426162640": "EWL006",          # Shopee-xiaomi.thailand
    "231427130": "EWL007",          # xiaomi_home_appliances
    "1646465545": "EWL008",         # Shopee-nextgadget
}

# TopOne wallets
TOPONE_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "538498056": "EWL001",          # Shopee-Vinkothailandstore
}

# ============================================================
# Fallback mapping by shop name keywords (normalized lowercase)
# (Use when seller_id missing)
# ============================================================

RABBIT_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    # keep explicit/unique first; matching is "contains"
    "shopee-70mai": "EWL001",
    "70mai": "EWL001",
    "shopee-ddpai": "EWL002",
    "ddpai": "EWL002",
    "shopee-jimmy": "EWL003",
    "shopeejimmy": "EWL003",
    "jimmy": "EWL003",
    "shopee-mibro": "EWL004",
    "mibro": "EWL004",
    "shopee-mova": "EWL005",
    "mova": "EWL005",
    "shopee-toptoy": "EWL006",
    "toptoy": "EWL006",
    "shopee-uwant": "EWL007",
    "uwant": "EWL007",
    "shopee-wanbo": "EWL008",
    "wanbo": "EWL008",
    "shopee-zepp": "EWL009",
    "zepp": "EWL009",
    "rabbit": "EWL010",
}

SHD_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-ankerthailandstore": "EWL001",
    "ankerthailandstore": "EWL001",
    "anker": "EWL001",
    "shopee-dreamofficial": "EWL002",
    "dreamofficial": "EWL002",
    "dreame": "EWL002",
    "shopee-levoitofficialstore": "EWL003",
    "levoitofficialstore": "EWL003",
    "levoit": "EWL003",
    "shopee-soundcoreofficialstore": "EWL004",
    "soundcoreofficialstore": "EWL004",
    "soundcore": "EWL004",
    "xiaomismartappliances": "EWL005",
    "xiaomi smart appliances": "EWL005",
    "shopee-xiaomi.thailand": "EWL006",
    "xiaomi.thailand": "EWL006",
    "xiaomi thailand": "EWL006",
    "xiaomi_home_appliances": "EWL007",
    "xiaomi home appliances": "EWL007",
    "shopee-nextgadget": "EWL008",
    "nextgadget": "EWL008",
}

TOPONE_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-vinkothailandstore": "EWL001",
    "vinkothailandstore": "EWL001",
    "vinko": "EWL001",
}

# ============================================================
# Regex for extracting seller/shop ids from OCR text
# ============================================================

# NOTE: หลายเอกสารมักเขียนแบบ:
# - Seller ID: 123456
# - SellerId 123456
# - Shop ID=123456
# - ShopId# 123456
# - Merchant ID : 123456
# - shopid 123456
# และบางทีมี comma/space/dash คั่น
SELLER_ID_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bseller\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
    re.compile(r"\bshop\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
    re.compile(r"\bmerchant\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
    re.compile(r"\bstore\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
]

EWL_RE = re.compile(r"^EWL\d{3}$", re.IGNORECASE)

# ============================================================
# Normalization helpers
# ============================================================

_TH_DIGITS = "๐๑๒๓๔๕๖๗๘๙"
_AR_DIGITS = "0123456789"
_TH2AR = str.maketrans({ _TH_DIGITS[i]: _AR_DIGITS[i] for i in range(10) })


def _thai_digits_to_arabic(s: str) -> str:
    return (s or "").translate(_TH2AR)


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
    # digits only (remove comma/space/hyphen)
    return _digits_only(seller_id)


def _norm_shop_name(shop_name: str) -> str:
    # lower + strip + collapse spaces + remove some punctuation noise
    s = _norm_text(shop_name).lower()
    if not s:
        return ""
    # keep dots/underscores/hyphens because your keywords use them,
    # but remove brackets/quotes that often appear in OCR
    s = re.sub(r"[\"'`“”‘’\(\)\[\]\{\}<>]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_seller_id_from_text(text: str) -> str:
    t = _norm_text(text).lower()
    if not t:
        return ""
    for rx in SELLER_ID_PATTERNS:
        m = rx.search(t)
        if not m:
            continue
        raw = m.group(1) or ""
        sid = _norm_seller_id(raw)
        # sanity: seller_id usually >= 5 digits
        if len(sid) >= 5:
            return sid
    return ""


def _is_valid_wallet(code: str) -> bool:
    return bool(code) and bool(EWL_RE.match(code.strip()))


def _client_bucket(client_tax_id: str) -> str:
    d = _digits_only(client_tax_id)
    if d == CLIENT_RABBIT:
        return "RABBIT"
    if d == CLIENT_SHD:
        return "SHD"
    if d == CLIENT_TOPONE:
        return "TOPONE"
    return ""


def _tables_for_client(bucket: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    if bucket == "RABBIT":
        return (RABBIT_WALLET_BY_SELLER_ID, RABBIT_WALLET_BY_SHOP_KEYWORD)
    if bucket == "SHD":
        return (SHD_WALLET_BY_SELLER_ID, SHD_WALLET_BY_SHOP_KEYWORD)
    if bucket == "TOPONE":
        return (TOPONE_WALLET_BY_SELLER_ID, TOPONE_WALLET_BY_SHOP_KEYWORD)
    return ({}, {})


def _match_shop_keyword(shop_norm: str, by_shop: Dict[str, str]) -> str:
    """
    Match by 'contains' but do longest-key-first to prevent wrong early hits.
    """
    if not shop_norm or not by_shop:
        return ""

    # longest-first keys
    keys = sorted((k for k in by_shop.keys() if k), key=len, reverse=True)
    for k in keys:
        code = by_shop.get(k, "")
        if not _is_valid_wallet(code):
            continue
        if k in shop_norm:
            return code

    return ""


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

    # 1) direct seller_id
    sid = _norm_seller_id(seller_id)
    if sid:
        code = by_sid.get(sid, "")
        if _is_valid_wallet(code):
            return code

    # 2) extract seller_id from OCR/body text
    if not sid and text:
        sid = _extract_seller_id_from_text(text)
        if sid:
            code = by_sid.get(sid, "")
            if _is_valid_wallet(code):
                return code

    # 3) fallback by shop_name keywords
    shop_norm = _norm_shop_name(shop_name)
    if shop_norm:
        code = _match_shop_keyword(shop_norm, by_shop)
        if _is_valid_wallet(code):
            return code

    # 4) optional: sometimes shop label appears inside OCR text (not in shop_name field)
    #    we only use this as last fallback to avoid false positives
    if text:
        t_norm = _norm_shop_name(text)  # reuse same normalization for keyword contains
        code = _match_shop_keyword(t_norm, by_shop)
        if _is_valid_wallet(code):
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
