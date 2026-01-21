"""
Vendor + Wallet + Credit Mapping System (v3.5) — PEAK Importer

Source of Truth:
  client_tax_id (บริษัทเรา เช่น Rabbit/SHD/TopOne)
  vendor_tax_id (ผู้รับเงิน เช่น Shopee/SPX/Lazada/TikTok/Shopify/Marketplace)
    -> vendor_code (Cxxxxx)

Additional:
  wallet code mapping:
    client_tax_id + seller/shop -> wallet_code (EWLxxx)
    used for PEAK column Q_payment_method ("ชำระโดย")

  credit card mapping:
    client_tax_id + credit (last4/name) -> credit_id (ADVxxx)
    used for PEAK credit selection / Corporate Card mapping

Goals:
✅ get_vendor_code() คืน "Cxxxxx" เสมอเมื่อรู้ client + vendor
✅ fallback ห้ามคืนชื่อ platform (กัน D_vendor_code หลุดเป็น Shopee/Lazada)
✅ normalize tax id / name ให้ robust (รวมกรณีมีข้อความปน, OCR, Thai digit)
✅ get_wallet_code() คืน "EWLxxx" เมื่อรู้ seller_id/shop mapping
✅ get_credit_id() คืน "ADVxxx" จากเลขท้าย 4 หลัก/ชื่อบัตร (ตามตารางของคุณ)
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple, List
import re

# ============================================================
# Client Tax ID Constants
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD    = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Vendor Tax ID Constants (canonical)
# ============================================================
VENDOR_SHOPEE             = "0105558019581"   # Shopee (Thailand) Co., Ltd.
VENDOR_LAZADA             = "010556214176"    # Lazada E-Services (Thailand) Co., Ltd.
VENDOR_TIKTOK             = "0105555040244"   # TikTok
VENDOR_MARKETPLACE_OTHER  = "0105548000241"   # Marketplace/ตัวกลาง
VENDOR_SHOPIFY            = "0993000475879"   # Shopify Commerce Singapore
VENDOR_SPX                = "0105561164871"   # SPX Express (Thailand)

# ============================================================
# Source of Truth: Nested dict mapping
# client_tax_id -> vendor_tax_id -> vendor_code (Cxxxxx)
# ============================================================
VENDOR_CODE_BY_CLIENT: Dict[str, Dict[str, str]] = {
    CLIENT_RABBIT: {
        VENDOR_SHOPEE: "C00395",
        VENDOR_LAZADA: "C00411",
        VENDOR_TIKTOK: "C00562",
        VENDOR_MARKETPLACE_OTHER: "C01031",
        VENDOR_SHOPIFY: "C01143",
        VENDOR_SPX: "C00563",
    },
    CLIENT_SHD: {
        VENDOR_SHOPEE: "C00888",
        VENDOR_LAZADA: "C01132",
        VENDOR_TIKTOK: "C01246",
        VENDOR_MARKETPLACE_OTHER: "C01420",
        VENDOR_SHOPIFY: "C33491",
        VENDOR_SPX: "C01133",
    },
    CLIENT_TOPONE: {
        VENDOR_SHOPEE: "C00020",
        VENDOR_LAZADA: "C00025",
        VENDOR_TIKTOK: "C00051",
        VENDOR_MARKETPLACE_OTHER: "C00095",
        VENDOR_SPX: "C00038",
        # VENDOR_SHOPIFY: "Cxxxxx",
    },
}

# ============================================================
# Vendor Name -> Vendor Tax ID mapping (fallback by name)
# ============================================================
VENDOR_NAME_TO_TAX: Dict[str, str] = {
    # Shopee
    "shopee": VENDOR_SHOPEE,
    "ช้อปปี้": VENDOR_SHOPEE,
    "shopee (thailand)": VENDOR_SHOPEE,
    "shopee thailand": VENDOR_SHOPEE,
    "shopee.co.th": VENDOR_SHOPEE,

    # Lazada
    "lazada": VENDOR_LAZADA,
    "ลาซาด้า": VENDOR_LAZADA,
    "lazada e-services": VENDOR_LAZADA,
    "lazada e services": VENDOR_LAZADA,
    "lazada.co.th": VENDOR_LAZADA,

    # TikTok
    "tiktok": VENDOR_TIKTOK,
    "ติ๊กต๊อก": VENDOR_TIKTOK,
    "tiktok shop": VENDOR_TIKTOK,

    # SPX
    "spx": VENDOR_SPX,
    "spx express": VENDOR_SPX,
    "shopee express": VENDOR_SPX,

    # Shopify
    "shopify": VENDOR_SHOPIFY,
    "shopify commerce": VENDOR_SHOPIFY,

    # Marketplace / other
    "marketplace": VENDOR_MARKETPLACE_OTHER,
    "ตัวกลาง": VENDOR_MARKETPLACE_OTHER,
    "มาร์เก็ตเพลส": VENDOR_MARKETPLACE_OTHER,
    "better marketplace": VENDOR_MARKETPLACE_OTHER,
    "เบ็ตเตอร์": VENDOR_MARKETPLACE_OTHER,
}

# ============================================================
# Aliases for vendor tax IDs (OCR mistakes / variant formats)
# map alias -> canonical vendor tax id
# ============================================================
ALIAS_VENDOR_TAX_ID_MAP: Dict[str, str] = {
    # "010555801958I": VENDOR_SHOPEE,  # example OCR I/1
}

# ============================================================
# Wallet mapping (Q_payment_method) — EWLxxx
# ============================================================

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

TOPONE_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "538498056": "EWL001",          # Shopee-Vinkothailandstore
}

# keyword fallback (normalized lowercase) — contains-match (longest-first)
RABBIT_WALLET_BY_SHOP_NAME: Dict[str, str] = {
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

SHD_WALLET_BY_SHOP_NAME: Dict[str, str] = {
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

TOPONE_WALLET_BY_SHOP_NAME: Dict[str, str] = {
    "shopee-vinkothailandstore": "EWL001",
    "vinkothailandstore": "EWL001",
    "vinko": "EWL001",
}

# ============================================================
# Credit Card mapping — ADVxxx
# (จากตารางของคุณ: id_credit, credit_name, credit_iv)
# ============================================================

# Rabbit: match by last4 digits -> ADVxxx
# จากรูปที่คุณส่ง:
# - ADV011 Rabbit-Visa-Personal : ....1350, 2939, 8255, 3622
# - ADV013 Rabbit-Visa-(RB)4614 : ....4614
# - ADV014 Rabbit-Visa-(SHD)4622 : ....4622
RABBIT_CREDIT_BY_LAST4: Dict[str, str] = {
    "1350": "ADV011",
    "2939": "ADV011",
    "8255": "ADV011",
    "3622": "ADV011",
    "4614": "ADV013",
    "4622": "ADV014",
    # เพิ่มเติมได้เรื่อย ๆ
}

# ถ้ามีของ SHD/TopOne ในอนาคต ให้ใส่เพิ่มได้
SHD_CREDIT_BY_LAST4: Dict[str, str] = {
    # "xxxx": "ADVxxx",
}

TOPONE_CREDIT_BY_LAST4: Dict[str, str] = {
    # "xxxx": "ADVxxx",
}

# ============================================================
# Normalization helpers
# ============================================================
_TAX13_RE   = re.compile(r"\b\d{13}\b")
_CCODE_RE   = re.compile(r"^C\d{5}$", re.IGNORECASE)
_EWL_RE     = re.compile(r"^EWL\d{3}$", re.IGNORECASE)
_ADV_RE     = re.compile(r"^ADV\d{3}$", re.IGNORECASE)
_WS_RE      = re.compile(r"\s+")

_TH_DIGITS = "๐๑๒๓๔๕๖๗๘๙"
_AR_DIGITS = "0123456789"
_TH2AR = str.maketrans({ _TH_DIGITS[i]: _AR_DIGITS[i] for i in range(10) })

# tries to catch seller id patterns (OCR)
SELLER_ID_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bseller\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
    re.compile(r"\bshop\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
    re.compile(r"\bmerchant\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE),
]

# credit last4 patterns (OCR/name)
LAST4_PATTERNS: List[re.Pattern] = [
    re.compile(r"(?:visa|master|amex|american\s*express)\D{0,30}(\d{4})\b", re.IGNORECASE),
    re.compile(r"\bending\D{0,10}(\d{4})\b", re.IGNORECASE),
    re.compile(r"\bท้าย\D{0,10}(\d{4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{4})\b"),
]


def _thai_digits_to_arabic(s: str) -> str:
    return (s or "").translate(_TH2AR)


def _norm_name(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    s = _WS_RE.sub(" ", s)
    # remove noisy brackets/quotes often from OCR
    s = re.sub(r"[\"'`“”‘’\(\)\[\]\{\}<>]+", " ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _extract_13_digits(s: str) -> str:
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    m = _TAX13_RE.search(s)
    return m.group(0) if m else ""


def _norm_tax_id(tax_id: str) -> str:
    """
    Normalize tax id:
    - if embedded 13 digits -> take it
    - if not 13 digits -> return ""
    - apply alias map
    """
    s = (tax_id or "").strip()
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    d13 = _extract_13_digits(s)
    if not d13:
        return ""
    return ALIAS_VENDOR_TAX_ID_MAP.get(d13, d13)


def _is_known_client(client_tax_id: str) -> bool:
    c = _norm_tax_id(client_tax_id)
    return c in (CLIENT_RABBIT, CLIENT_SHD, CLIENT_TOPONE)


def _code_is_valid(code: str) -> bool:
    return bool(code) and bool(_CCODE_RE.match(code.strip()))


def _wallet_is_valid(code: str) -> bool:
    return bool(code) and bool(_EWL_RE.match(code.strip()))


def _adv_is_valid(code: str) -> bool:
    return bool(code) and bool(_ADV_RE.match(code.strip()))


def _digits_only(s: str) -> str:
    if not s:
        return ""
    s = _thai_digits_to_arabic(str(s))
    return "".join(ch for ch in s if ch.isdigit())


def _norm_seller_id(seller_id: str) -> str:
    """
    seller_id should be digits only.
    Accepts: ' 628,286,975 ' -> '628286975'
    """
    return _digits_only(seller_id)


def _extract_seller_id_from_text(text: str) -> str:
    t = _norm_name(text)
    if not t:
        return ""
    for rx in SELLER_ID_PATTERNS:
        m = rx.search(t)
        if not m:
            continue
        sid = _norm_seller_id(m.group(1))
        if len(sid) >= 5:
            return sid
    return ""


def _match_contains_longest_first(hay: str, mapping: Dict[str, str], *, validator) -> str:
    """
    contains-match with longest-first keys to avoid false early hits.
    """
    if not hay or not mapping:
        return ""
    keys = sorted((k for k in mapping.keys() if k), key=len, reverse=True)
    for k in keys:
        v = mapping.get(k, "")
        if v and validator(v) and (k in hay):
            return v
    return ""


def _extract_last4_best_effort(*parts: str) -> str:
    """
    Extract last4 digits from given strings (credit_iv/name/text).
    Returns "" if not found.
    """
    joined = " | ".join([_norm_name(p) for p in parts if p])
    if not joined:
        return ""

    # try patterns
    for rx in LAST4_PATTERNS:
        m = rx.search(joined)
        if m:
            last4 = _digits_only(m.group(1))
            if len(last4) == 4:
                return last4

    # fallback: take last 4 digits from any digits sequence
    digits = _digits_only(joined)
    if len(digits) >= 4:
        return digits[-4:]
    return ""


# ============================================================
# Public API: resolve vendor tax id from name
# ============================================================
def get_vendor_tax_id_from_name(vendor_name: str) -> str:
    """
    Best-effort resolve vendor_tax_id from vendor_name/platform string.
    """
    vn = _norm_name(vendor_name)
    if not vn:
        return ""
    # contains match
    for key, tax in VENDOR_NAME_TO_TAX.items():
        if key and key in vn:
            return tax
    return ""


# ============================================================
# Public API: main vendor mapping (HARDENED)
# ============================================================
def get_vendor_code(client_tax_id: str, vendor_tax_id: str = "", vendor_name: str = "") -> str:
    """
    Return vendor code (Cxxxxx) for PEAK "ผู้รับเงิน/คู่ค้า".

    Hardened behavior:
    - vendor_tax_id ถ้า caller ส่ง "Shopee" มา จะถูกมองว่าเป็นชื่อ ไม่ใช่ tax id
    - ถ้ารู้ client + vendor (จาก tax หรือ name) ต้องคืน Cxxxxx เสมอ
    - ห้ามคืนชื่อ platform เด็ดขาด
    """
    c = _norm_tax_id(client_tax_id)

    if not c or not _is_known_client(c):
        return "Unknown"

    # 1) try vendor tax id (13 digits only)
    v = _norm_tax_id(vendor_tax_id)
    if v:
        code = VENDOR_CODE_BY_CLIENT.get(c, {}).get(v, "")
        if _code_is_valid(code):
            return code

    # 2) treat vendor_tax_id as name hint too if not 13 digits
    name_hint = vendor_name or vendor_tax_id or ""
    v2 = get_vendor_tax_id_from_name(name_hint)
    if v2:
        code = VENDOR_CODE_BY_CLIENT.get(c, {}).get(v2, "")
        if _code_is_valid(code):
            return code

    return "Unknown"


# ============================================================
# Wallet mapping (Q_payment_method) — EWLxxx
# ============================================================
def get_wallet_code(
    client_tax_id: str,
    *,
    seller_id: str = "",
    shop_name: str = "",
    platform: str = "",
    text: str = "",
) -> str:
    """
    Return wallet code (EWLxxx) for PEAK column Q_payment_method ("ชำระโดย")

    Behavior:
    - Prefer seller_id exact mapping
    - Fallback by shop_name keywords (contains / longest-first)
    - Optional: try extract seller_id from text (OCR)
    - NEVER return platform name
    - If cannot map -> "" (let worker mark NEEDS_REVIEW)
    """
    c = _norm_tax_id(client_tax_id)
    if not c or not _is_known_client(c):
        return ""

    sid = _norm_seller_id(seller_id)
    if not sid and text:
        sid = _extract_seller_id_from_text(text)

    shop = _norm_name(shop_name)
    _ = _norm_name(platform)  # kept for future use

    if c == CLIENT_RABBIT:
        if sid:
            code = RABBIT_WALLET_BY_SELLER_ID.get(sid, "")
            if _wallet_is_valid(code):
                return code
        code = _match_contains_longest_first(shop, RABBIT_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        if _wallet_is_valid(code):
            return code
        # last fallback: sometimes shop label appears inside OCR text
        t = _norm_name(text)
        code = _match_contains_longest_first(t, RABBIT_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        return code if _wallet_is_valid(code) else ""

    if c == CLIENT_SHD:
        if sid:
            code = SHD_WALLET_BY_SELLER_ID.get(sid, "")
            if _wallet_is_valid(code):
                return code
        code = _match_contains_longest_first(shop, SHD_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        if _wallet_is_valid(code):
            return code
        t = _norm_name(text)
        code = _match_contains_longest_first(t, SHD_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        return code if _wallet_is_valid(code) else ""

    if c == CLIENT_TOPONE:
        if sid:
            code = TOPONE_WALLET_BY_SELLER_ID.get(sid, "")
            if _wallet_is_valid(code):
                return code
        code = _match_contains_longest_first(shop, TOPONE_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        if _wallet_is_valid(code):
            return code
        t = _norm_name(text)
        code = _match_contains_longest_first(t, TOPONE_WALLET_BY_SHOP_NAME, validator=_wallet_is_valid)
        return code if _wallet_is_valid(code) else ""

    return ""


# ============================================================
# Credit mapping — ADVxxx
# ============================================================
def get_credit_id(
    client_tax_id: str,
    *,
    credit_iv: str = "",
    credit_name: str = "",
    text: str = "",
) -> str:
    """
    Return credit id (ADVxxx) based on our client + last4 digits.

    Inputs:
      - credit_iv: column like "Visa .... 4614" or raw last4
      - credit_name: "Rabbit-Visa-(RB)4614" etc.
      - text: OCR text (fallback)

    Returns:
      - "ADVxxx" if resolved
      - "" if unknown (caller can mark NEEDS_REVIEW)
    """
    c = _norm_tax_id(client_tax_id)
    if not c or not _is_known_client(c):
        return ""

    last4 = _extract_last4_best_effort(credit_iv, credit_name, text)
    if not last4:
        return ""

    if c == CLIENT_RABBIT:
        adv = RABBIT_CREDIT_BY_LAST4.get(last4, "")
        return adv if _adv_is_valid(adv) else ""
    if c == CLIENT_SHD:
        adv = SHD_CREDIT_BY_LAST4.get(last4, "")
        return adv if _adv_is_valid(adv) else ""
    if c == CLIENT_TOPONE:
        adv = TOPONE_CREDIT_BY_LAST4.get(last4, "")
        return adv if _adv_is_valid(adv) else ""

    return ""


# ============================================================
# Optional: detect client by context (best effort)
# ============================================================
def detect_client_from_context(text: str) -> Optional[str]:
    """
    Robust detect:
    - check tax id presence
    - fallback keyword match
    """
    t = _norm_name(text)
    if not t:
        return None

    # tax id hit is strongest
    if CLIENT_RABBIT in t:
        return CLIENT_RABBIT
    if CLIENT_SHD in t:
        return CLIENT_SHD
    if CLIENT_TOPONE in t:
        return CLIENT_TOPONE

    # keyword fallback
    if "rabbit" in t:
        return CLIENT_RABBIT
    if "shd" in t:
        return CLIENT_SHD
    if "topone" in t or "top one" in t:
        return CLIENT_TOPONE

    return None


def get_client_name(client_tax_id: str) -> str:
    c = _norm_tax_id(client_tax_id)
    if c == CLIENT_RABBIT:
        return "RABBIT"
    if c == CLIENT_SHD:
        return "SHD"
    if c == CLIENT_TOPONE:
        return "TOPONE"
    return "UNKNOWN"


def get_all_vendor_codes_for_client(client_tax_id: str) -> Dict[str, str]:
    c = _norm_tax_id(client_tax_id)
    return dict(VENDOR_CODE_BY_CLIENT.get(c, {}))


# ============================================================
# Category mapping for description/group (kept as your version)
# ============================================================
def get_expense_category(description: str, platform: str = "") -> str:
    """
    Rules:
      - Lazada / Shopee / TikTok → Marketplace Expense
      - Commission → Selling Expense
      - Advertising → Advertising Expense
      - Goods → Inventory / COGS
      - Shipping/SPX → Shipping Expense
    """
    desc = _norm_name(description)
    plat = _norm_name(platform)

    if any(w in desc for w in ("shipping", "delivery", "ขนส่ง", "จัดส่ง", "spx")) or plat in ("spx", "spx express"):
        return "Shipping Expense"

    if any(w in desc for w in ("commission", "คอมมิชชั่น", "ค่าคอม")):
        return "Selling Expense"

    if any(w in desc for w in ("advertising", "โฆษณา", "ads", "sponsored")):
        return "Advertising Expense"

    if any(w in desc for w in ("goods", "สินค้า", "inventory", "cogs", "cost of goods")):
        return "Inventory / COGS"

    if plat in ("shopee", "lazada", "tiktok", "ช้อปปี้", "ลาซาด้า", "ติ๊กต๊อก"):
        return "Marketplace Expense"
    if any(w in desc for w in ("shopee", "lazada", "tiktok", "ช้อปปี้", "ลาซาด้า", "ติ๊กต๊อก")):
        return "Marketplace Expense"

    return "Marketplace Expense"


def format_short_description(platform: str, fee_type: str = "", seller_info: str = "") -> str:
    parts = []
    if platform:
        parts.append(platform.strip())
    if fee_type:
        parts.append(fee_type.strip())

    if seller_info:
        m = re.search(r"Seller(?:\s+ID)?:\s*([0-9A-Za-z_\-]+)", seller_info)
        if m:
            parts.append(f"Seller {m.group(1)}")

    return " - ".join(parts) if parts else "Marketplace Expense"


__all__ = [
    "get_vendor_code",
    "get_vendor_tax_id_from_name",
    "detect_client_from_context",
    "get_client_name",
    "get_all_vendor_codes_for_client",
    "get_expense_category",
    "format_short_description",
    "get_wallet_code",
    "get_credit_id",
    "CLIENT_RABBIT",
    "CLIENT_SHD",
    "CLIENT_TOPONE",
    "VENDOR_SHOPEE",
    "VENDOR_LAZADA",
    "VENDOR_TIKTOK",
    "VENDOR_SPX",
    "VENDOR_MARKETPLACE_OTHER",
    "VENDOR_SHOPIFY",
]
