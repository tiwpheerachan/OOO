# backend/app/services/classifier.py
from __future__ import annotations

import os
import re
import logging
from typing import Literal, Dict, Tuple, Optional

from ..utils.text_utils import normalize_text

logger = logging.getLogger(__name__)

# ✅ 8 platforms (aligned with export_service & ai_service)
PlatformLabel = Literal[
    "META",      # Meta/Facebook Ads
    "GOOGLE",    # Google Ads
    "SHOPEE",    # Shopee marketplace
    "LAZADA",    # Lazada marketplace
    "TIKTOK",    # TikTok Shop
    "SPX",       # Shopee Express
    "THAI_TAX",  # Thai Tax Invoice
    "UNKNOWN",   # Unknown/Other
]

# ---------------------------------------------------------------------
# Strong ID Regex (High Confidence)
# ---------------------------------------------------------------------

# Meta/Facebook Ads patterns
RE_META_RECEIPT = re.compile(r"\bRC\s*META\s*[A-Z0-9\-/]{6,}\b", re.IGNORECASE)
RE_META_IRELAND = re.compile(r"meta\s*platforms?\s*ireland", re.IGNORECASE)
RE_FACEBOOK = re.compile(r"\b(facebook|fb\s*ads|instagram\s*ads)\b", re.IGNORECASE)

# Google Ads patterns
RE_GOOGLE_PAYMENT = re.compile(r"\b[VW]\s*\d{15,20}\b", re.IGNORECASE)  # V0971174339667745
RE_GOOGLE_ASIA = re.compile(r"google\s*asia\s*pacific", re.IGNORECASE)
RE_GOOGLE_ADS = re.compile(r"\b(google\s*ad(?:s|words)?|google\s*advertising)\b", re.IGNORECASE)

# Thai Tax Invoice patterns
RE_THAI_TAX_INVOICE = re.compile(r"(ใบกำกับภาษี|ใบเสร็จรับเงิน|tax\s*invoice)", re.IGNORECASE)
RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")
RE_BRANCH_5 = re.compile(r"(?:branch|สาขา)\s*[:#]?\s*(\d{5})", re.IGNORECASE)

# SPX patterns (shipping docs)
RE_SPX_RCSPX = re.compile(r"\bRCS\s*PX\s*[A-Z0-9\-/]{6,}\b", re.IGNORECASE)
RE_SPX_RCS_ANY = re.compile(r"\bRCS\s*[A-Z0-9]{3,}\b", re.IGNORECASE)

# Lazada
RE_LAZADA_THMPTI = re.compile(r"\bTHMPTI\s*\d{10,20}\b", re.IGNORECASE)

# TikTok
RE_TIKTOK_TTSTH = re.compile(r"\bTTSTH[0-9A-Z\-/]*\b", re.IGNORECASE)
RE_TIKTOK_WORD = re.compile(r"\btiktok\b", re.IGNORECASE)

# Shopee
RE_SHOPEE_TIV = re.compile(r"\bTIV\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_TIR = re.compile(r"\bTIR\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_WORD = re.compile(r"\bshopee\b", re.IGNORECASE)
RE_SHOPEE_TRS = re.compile(r"\bTRS\b", re.IGNORECASE)  # weak; only with shopee context

# ---------------------------------------------------------------------
# Filename-only hints (สำคัญมากเวลาข้อความใน PDF สั้น)
# ---------------------------------------------------------------------
FILENAME_META_HINTS = (
    "meta", "facebook", "fb", "instagram", "ig", "meta-ads", "fb-ads", "facebook-ads",
)
FILENAME_GOOGLE_HINTS = (
    "google", "adwords", "googleads", "google-ads", "gads",
)
FILENAME_SPX_HINTS = (
    "spx", "rcspx", "shopee express", "shopee-express", "standard express", "waybill", "awb",
)
FILENAME_SHOPEE_HINTS = (
    "shopee", "tiv", "tir", "shopee-ti", "shopee-th", "shopee(thailand)",
)
FILENAME_LAZADA_HINTS = (
    "lazada", "lzd", "laz", "thmpti",
)
FILENAME_TIKTOK_HINTS = (
    "tiktok", "ttsth", "tt shop", "tiktokshop",
)

# ---------------------------------------------------------------------
# Signals (Soft Keywords)
# ---------------------------------------------------------------------
META_SIGS_STRONG = (
    "meta platforms ireland",
    "facebook ireland",
    "rcmeta",
    "meta ads receipt",
    "facebook ads receipt",
    "instagram ads",
)
META_SIGS_WEAK = (
    "meta", "facebook", "fb ads", "meta ads",
    "reverse charge", "vat reverse",
)

GOOGLE_SIGS_STRONG = (
    "google asia pacific",
    "google advertising",
    "google ads payment",
    "google adwords",
)
GOOGLE_SIGS_WEAK = (
    "google ads", "google payment", "adwords",
)

THAI_TAX_SIGS = (
    "ใบกำกับภาษี",
    "ใบเสร็จรับเงิน",
    "tax invoice",
    "เลขประจำตัวผู้เสียภาษี",
    "เลขทะเบียน",
    "สำนักงานใหญ่",
)

SHOPEE_SIGS = (
    "shopee", "shopee-ti", "shopee-tiv", "shopee-tir",
    "tiv-", "tir-", "ช้อปปี้", "shopee (thailand)",
)
LAZADA_SIGS = (
    "lazada", "lazada invoice", "lzd", "ลาซาด้า", "seller center",
)
TIKTOK_SIGS = (
    "tiktok", "tiktok shop", "tt shop", "tiktok commerce", "ติ๊กต็อก",
)
SPX_SIGS = (
    "spx", "spx express", "standard express", "rcs", "rcspx",
    "spx (thailand)", "spx express (thailand)", "shopee express",
)

INVOICE_SIGS = (
    "ใบกำกับภาษี", "tax invoice", "receipt", "ใบเสร็จ", "invoice",
)

# Known client tax IDs (exclude from vendor detection)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ---------------------------------------------------------------------
# NEW: Marketplace identity extraction (for description building)
# ---------------------------------------------------------------------
RE_SELLER_ID = re.compile(r"\bSeller\s*ID\b\s*[:#]?\s*([0-9]{6,20})\b", re.IGNORECASE)
RE_USERNAME = re.compile(r"\bUsername\b\s*[:#]?\s*([A-Za-z0-9_.\-]{2,64})\b", re.IGNORECASE)

# Sometimes appears in Shopee docs as: "Seller ID 1646465545  nextgadget"
RE_SELLER_ID_LOOSE = re.compile(r"\bseller\s*id\s*[:#]?\s*([0-9]{6,20})\b", re.IGNORECASE)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _norm(s: str) -> str:
    """Normalize + lower + trim; keep speed stable"""
    try:
        t = normalize_text(s or "").lower()
        # prevent mega text slowdown
        if len(t) > 160_000:
            t = t[:100_000] + "\n...\n" + t[-40_000:]
        return t
    except Exception as e:
        logger.warning("Normalization error: %s", e)
        return ""


def _contains_any(t: str, needles: tuple[str, ...]) -> bool:
    return any(n and (n in t) for n in needles)


def _count_contains(t: str, needles: tuple[str, ...]) -> int:
    hit = 0
    for n in needles:
        if n and (n in t):
            hit += 1
    return hit


def _regex_hit(t: str, rx: re.Pattern) -> bool:
    try:
        return rx.search(t) is not None
    except Exception:
        return False


def _has_vendor_tax_id(t: str) -> bool:
    """
    ✅ Check if text has 13-digit tax ID NOT in client list
    (Strong indicator for Thai Tax Invoice)
    """
    try:
        for m in RE_TAX_ID_13.finditer(t):
            tax = m.group(1)
            if tax and tax not in CLIENT_TAX_IDS:
                return True
        return False
    except Exception:
        return False


# --------------------------
# NEW: Filename helpers
# --------------------------
def extract_filename_stem(filename: str) -> str:
    """
    Return clean stem without path and extension.
    Example:
      'C:\\x\\Shopee-TIV-TRSPEMKP00-00000-251201-0013100.pdf'
      -> 'Shopee-TIV-TRSPEMKP00-00000-251201-0013100'
    """
    if not filename:
        return ""
    try:
        base = os.path.basename(filename).strip()
        stem, _ext = os.path.splitext(base)
        return stem.strip()
    except Exception:
        return ""


def extract_doc_ref_from_filename(filename: str) -> str:
    """
    ต้องการ: C_reference/G_invoice_no ให้ “เป็นชื่อไฟล์เต็มๆ”
    -> เอา stem แล้ว normalize เป็น no-space
    """
    stem = extract_filename_stem(filename)
    if not stem:
        return ""
    # no whitespace / no newlines
    return re.sub(r"\s+", "", stem)


def extract_marketplace_identity(text: str) -> Tuple[str, str]:
    """
    Try to extract (seller_id, username) from document text.
    Returns ("", "") if not found.
    """
    t = _norm(text)
    seller_id = ""
    username = ""

    try:
        m = RE_SELLER_ID.search(t)
        if m:
            seller_id = m.group(1).strip()

        u = RE_USERNAME.search(t)
        if u:
            username = u.group(1).strip()

        if not seller_id:
            m2 = RE_SELLER_ID_LOOSE.search(t)
            if m2:
                seller_id = m2.group(1).strip()

        return seller_id, username
    except Exception:
        return "", ""


def _filename_boost(score: Dict[str, int], fn: str) -> None:
    """Filename-only boosting (critical for short PDFs / image-based)"""
    if not fn:
        return

    # SPX highest among filename hints
    if _contains_any(fn, FILENAME_SPX_HINTS) or "rcspx" in fn:
        score["SPX"] += 55

    # META / GOOGLE
    if _contains_any(fn, FILENAME_META_HINTS):
        score["META"] += 40
    if _contains_any(fn, FILENAME_GOOGLE_HINTS):
        score["GOOGLE"] += 40

    # marketplaces
    if _contains_any(fn, FILENAME_LAZADA_HINTS):
        score["LAZADA"] += 30
    if _contains_any(fn, FILENAME_TIKTOK_HINTS):
        score["TIKTOK"] += 26
    if _contains_any(fn, FILENAME_SHOPEE_HINTS):
        score["SHOPEE"] += 24


def _weighted_score(t: str, filename: str) -> Dict[str, int]:
    """
    ✅ Weighted scoring using BOTH text and filename
    """
    fn = _norm(filename)
    tt = t

    score: Dict[str, int] = {
        "META": 0,
        "GOOGLE": 0,
        "SHOPEE": 0,
        "LAZADA": 0,
        "TIKTOK": 0,
        "SPX": 0,
        "THAI_TAX": 0,
    }

    # filename boost
    _filename_boost(score, fn)

    # META strong
    if _regex_hit(tt, RE_META_RECEIPT) or _regex_hit(fn, RE_META_RECEIPT):
        score["META"] += 170
    if _regex_hit(tt, RE_META_IRELAND) or _regex_hit(fn, RE_META_IRELAND):
        score["META"] += 165
    if _regex_hit(tt, RE_FACEBOOK) or _regex_hit(fn, RE_FACEBOOK):
        score["META"] += 90
    score["META"] += 16 * _count_contains(tt, META_SIGS_STRONG)
    score["META"] += 10 * _count_contains(tt, META_SIGS_WEAK)

    # GOOGLE strong
    if _regex_hit(tt, RE_GOOGLE_PAYMENT) or _regex_hit(fn, RE_GOOGLE_PAYMENT):
        score["GOOGLE"] += 170
    if _regex_hit(tt, RE_GOOGLE_ASIA) or _regex_hit(fn, RE_GOOGLE_ASIA):
        score["GOOGLE"] += 165
    if _regex_hit(tt, RE_GOOGLE_ADS) or _regex_hit(fn, RE_GOOGLE_ADS):
        score["GOOGLE"] += 90
    score["GOOGLE"] += 16 * _count_contains(tt, GOOGLE_SIGS_STRONG)
    score["GOOGLE"] += 10 * _count_contains(tt, GOOGLE_SIGS_WEAK)

    # SPX BEFORE Shopee
    if _regex_hit(tt, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX):
        score["SPX"] += 145
    if "rcspx" in tt or "rcspx" in fn:
        score["SPX"] += 145
    score["SPX"] += 10 * _count_contains(tt, SPX_SIGS)

    # LAZADA
    if _regex_hit(tt, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
        score["LAZADA"] += 120
    score["LAZADA"] += 10 * _count_contains(tt, LAZADA_SIGS)

    # TIKTOK
    if _regex_hit(tt, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH):
        score["TIKTOK"] += 120
    if _regex_hit(tt, RE_TIKTOK_WORD) or _regex_hit(fn, RE_TIKTOK_WORD):
        score["TIKTOK"] += 25
    score["TIKTOK"] += 10 * _count_contains(tt, TIKTOK_SIGS)

    # SHOPEE
    if _regex_hit(tt, RE_SHOPEE_TIV) or _regex_hit(fn, RE_SHOPEE_TIV):
        score["SHOPEE"] += 110
    if _regex_hit(tt, RE_SHOPEE_TIR) or _regex_hit(fn, RE_SHOPEE_TIR):
        score["SHOPEE"] += 110
    if _regex_hit(tt, RE_SHOPEE_WORD) or _regex_hit(fn, RE_SHOPEE_WORD):
        score["SHOPEE"] += 22
    score["SHOPEE"] += 10 * _count_contains(tt, SHOPEE_SIGS)

    # TRS weak: only with Shopee context
    trs = _regex_hit(tt, RE_SHOPEE_TRS) or ("trs" in tt)
    if trs:
        has_ctx = ("shopee" in tt) or ("tiv" in tt) or ("tir" in tt) or ("shopee" in fn)
        if has_ctx:
            score["SHOPEE"] += 18

    # THAI_TAX (conservative)
    if _regex_hit(tt, RE_THAI_TAX_INVOICE):
        score["THAI_TAX"] += 55
    if _has_vendor_tax_id(tt):
        score["THAI_TAX"] += 70
    if _regex_hit(tt, RE_BRANCH_5):
        score["THAI_TAX"] += 35
    score["THAI_TAX"] += 10 * _count_contains(tt, THAI_TAX_SIGS)

    # penalties if strong other platform exists
    if score["META"] >= 70 or score["GOOGLE"] >= 70 or score["SPX"] >= 70:
        score["THAI_TAX"] = int(score["THAI_TAX"] * 0.25)
    elif score["SHOPEE"] >= 55 or score["LAZADA"] >= 55 or score["TIKTOK"] >= 55:
        score["THAI_TAX"] = int(score["THAI_TAX"] * 0.45)

    return score


def classify_platform(text: str, filename: str = "", debug: bool = False) -> PlatformLabel:
    """
    ✅ Enhanced platform classifier for 8 platforms
    - MUST accept filename
    """
    if debug:
        logger.setLevel(logging.DEBUG)

    try:
        t = _norm(text)
        fn = _norm(filename)

        if not t and not fn:
            return "UNKNOWN"

        # --------------------------
        # Fast paths (strong ID)
        # --------------------------
        if (
            _regex_hit(t, RE_META_RECEIPT) or _regex_hit(fn, RE_META_RECEIPT) or
            _regex_hit(t, RE_META_IRELAND) or _regex_hit(fn, RE_META_IRELAND)
        ):
            return "META"

        if (
            _regex_hit(t, RE_GOOGLE_PAYMENT) or _regex_hit(fn, RE_GOOGLE_PAYMENT) or
            _regex_hit(t, RE_GOOGLE_ASIA) or _regex_hit(fn, RE_GOOGLE_ASIA)
        ):
            return "GOOGLE"

        # SPX ก่อน Shopee เสมอ
        if (
            _regex_hit(t, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX) or
            ("rcspx" in t) or ("rcspx" in fn)
        ):
            return "SPX"

        if _regex_hit(t, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
            return "LAZADA"

        if _regex_hit(t, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH):
            return "TIKTOK"

        # --------------------------
        # Weighted scoring
        # --------------------------
        score = _weighted_score(t, filename=filename)
        if debug:
            logger.debug("Scores: %s", score)

        best_label, best_score = max(score.items(), key=lambda kv: kv[1])

        # thresholds per priority
        if score["META"] >= 55:
            return "META"
        if score["GOOGLE"] >= 55:
            return "GOOGLE"
        if score["SPX"] >= 45:
            return "SPX"
        if score["LAZADA"] >= 42:
            return "LAZADA"
        if score["TIKTOK"] >= 34:
            return "TIKTOK"
        if score["SHOPEE"] >= 34:
            return "SHOPEE"
        if score["THAI_TAX"] >= 70:
            return "THAI_TAX"

        # modest fallback (only if reasonable)
        if best_score >= 28 and best_label in (
            "META", "GOOGLE", "SPX", "SHOPEE", "LAZADA", "TIKTOK", "THAI_TAX"
        ):
            return best_label  # type: ignore[return-value]

        # invoice + vendor tax -> thai tax
        if _contains_any(t, INVOICE_SIGS) and _has_vendor_tax_id(t):
            return "THAI_TAX"

        return "UNKNOWN"

    except Exception as e:
        logger.error("Classification error: %s", e, exc_info=True)
        return "UNKNOWN"


def get_classification_details(text: str, filename: str = "") -> Tuple[PlatformLabel, Dict[str, int]]:
    """
    ✅ Return (platform, scores) for debugging
    """
    try:
        t = _norm(text)
        score = _weighted_score(t, filename=filename)
        platform = classify_platform(text, filename, debug=False)
        return (platform, score)
    except Exception as e:
        logger.error("Error getting classification details: %s", e)
        return (
            "UNKNOWN",
            {"META": 0, "GOOGLE": 0, "SHOPEE": 0, "LAZADA": 0, "TIKTOK": 0, "SPX": 0, "THAI_TAX": 0},
        )


def get_platform_metadata(platform: PlatformLabel) -> Dict[str, str]:
    """
    ✅ Metadata for integration (optional helper)
    """
    metadata = {
        "META": {
            "vendor_code": "Meta Platforms Ireland",
            "vat_rate": "NO",
            "price_type": "3",
            "group": "Advertising Expense",
        },
        "GOOGLE": {
            "vendor_code": "Google Asia Pacific",
            "vat_rate": "NO",
            "price_type": "3",
            "group": "Advertising Expense",
        },
        "SHOPEE": {
            "vendor_code": "Shopee",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "LAZADA": {
            "vendor_code": "Lazada",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "TIKTOK": {
            "vendor_code": "TikTok",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Marketplace Expense",
        },
        "SPX": {
            "vendor_code": "Shopee Express",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Delivery/Logistics Expense",
        },
        "THAI_TAX": {
            "vendor_code": "",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "General Expense",
        },
        "UNKNOWN": {
            "vendor_code": "Other",
            "vat_rate": "7%",
            "price_type": "1",
            "group": "Other Expense",
        },
    }
    return metadata.get(platform, metadata["UNKNOWN"])


__all__ = [
    "PlatformLabel",
    "classify_platform",
    "get_classification_details",
    "get_platform_metadata",
    # new helpers (to be used by extract_service/post_process)
    "extract_filename_stem",
    "extract_doc_ref_from_filename",
    "extract_marketplace_identity",
]
