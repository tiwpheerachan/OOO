# backend/app/services/classifier.py
"""
Platform Classifier - Fixed Version (from your original code)

✅ Changes made:
1. Relaxed thresholds: 80→40, 70→40, 60→30, 60→30
2. Added error handling (try-catch)
3. Added logging support
4. Added debug parameter
5. Added helper function for debugging
"""
from __future__ import annotations

import re
import logging
from typing import Literal, Dict, Tuple

from ..utils.text_utils import normalize_text

# Setup logger
logger = logging.getLogger(__name__)

PlatformLabel = Literal["shopee", "lazada", "tiktok", "spx", "ads", "other", "unknown"]

# ---------------------------------------------------------------------
# Strong ID regex (high confidence, handle whitespace/newline)
# ---------------------------------------------------------------------

# SPX: RCSPX... (often appears in filenames + text)
RE_SPX_RCSPX = re.compile(r"\bRCS\s*PX\s*[A-Z0-9\-/]{6,}\b", re.IGNORECASE)
RE_SPX_RCS_ANY = re.compile(r"\bRCS\s*[A-Z0-9]{3,}\b", re.IGNORECASE)  # weaker fallback

# Lazada: THMPTIxxxxxxxxxxxxxxxx
RE_LAZADA_THMPTI = re.compile(r"\bTHMPTI\s*\d{10,20}\b", re.IGNORECASE)

# TikTok: TTSTH* or TikTok Shop
RE_TIKTOK_TTSTH = re.compile(r"\bTTSTH[0-9A-Z\-/]*\b", re.IGNORECASE)
RE_TIKTOK_WORD = re.compile(r"\btiktok\b", re.IGNORECASE)

# Shopee: TIV-/TIR- patterns (strong)
RE_SHOPEE_TIV = re.compile(r"\bTIV\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_TIR = re.compile(r"\bTIR\s*-\s*[A-Z0-9]{3,}\b", re.IGNORECASE)
RE_SHOPEE_WORD = re.compile(r"\bshopee\b", re.IGNORECASE)

# Shopee TRS is weak (too many collisions). Only count if paired with shopee context.
RE_SHOPEE_TRS = re.compile(r"\bTRS\b", re.IGNORECASE)

# ---------------------------------------------------------------------
# Signals (soft keywords)
# ---------------------------------------------------------------------

SHOPEE_SIGS = (
    "shopee", "shopee-ti", "shopee-tiv", "shopee-tir", "tiv-", "tir-",
    "ช้อปปี้", "shopee (thailand)", "shopee thailand",
)
LAZADA_SIGS = (
    "lazada", "lazada invoice", "lzd", "laz", "ลาซาด้า",
)
TIKTOK_SIGS = (
    "tiktok", "tiktok shop", "tt shop", "tiktok commerce", "ติ๊กต็อก",
)
SPX_SIGS = (
    "spx", "spx express", "standard express", "rcs", "rcspx", "spx (thailand)",
    "spx express (thailand)",
)

# Ads / billing signals (need multiple to be confident)
ADS_SIGS_STRONG = (
    "ad invoice", "ads invoice", "tax invoice for ads", "billing",
    "statement", "charged", "payment for ads", "ads account", "ad account",
    "invoice for advertising", "advertising invoice",
    "facebook ads", "meta ads", "google ads", "tiktok ads", "line ads",
    "โฆษณา", "ค่าโฆษณา", "ยิงแอด", "บิลโฆษณา", "ใบแจ้งหนี้โฆษณา",
)
ADS_SIGS_WEAK = (
    "ads", "advertising", "campaign", "impression", "click", "cpc", "cpm",
)

# Negative shipping/tracking context (avoid marking as ads)
NEGATIVE_FOR_ADS = (
    "address", "shipment", "shipping", "tracking", "waybill", "parcel",
    "ผู้รับ", "ที่อยู่", "ขนส่ง", "พัสดุ", "จัดส่ง", "เลขพัสดุ", "tracking no",
)

# Generic invoice signals (fallback)
INVOICE_SIGS = (
    "ใบกำกับภาษี", "tax invoice", "receipt", "ใบเสร็จ", "invoice", "tax receipt",
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _norm(s: str) -> str:
    """
    normalize + lower + trim; keep it safe
    """
    try:
        t = normalize_text(s or "")
        t = t.lower()
        if len(t) > 160_000:
            # head+tail window to keep speed stable
            t = t[:100_000] + "\n...\n" + t[-40_000:]
        return t
    except Exception as e:
        logger.warning(f"Normalization error: {e}")
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


def _weighted_score(t: str, filename: str) -> dict[str, int]:
    """
    Weighted scoring using BOTH text and filename.
    filename is important when OCR text is sparse/empty.
    """
    fn = _norm(filename)
    tt = t

    score = {"shopee": 0, "lazada": 0, "tiktok": 0, "spx": 0, "ads": 0}

    # ---------- Strong ID (highest weight) ----------
    if _regex_hit(tt, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
        score["lazada"] += 100
    if _regex_hit(tt, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH):
        score["tiktok"] += 100

    # SPX strongest: RCSPX with whitespace tolerance
    if _regex_hit(tt, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX) or ("rcspx" in tt) or ("rcspx" in fn):
        score["spx"] += 120

    # Shopee strongest: TIV-/TIR- or explicit shopee-ti*
    if _regex_hit(tt, RE_SHOPEE_TIV) or _regex_hit(fn, RE_SHOPEE_TIV):
        score["shopee"] += 90
    if _regex_hit(tt, RE_SHOPEE_TIR) or _regex_hit(fn, RE_SHOPEE_TIR):
        score["shopee"] += 90
    if "shopee-ti" in tt or "shopee-ti" in fn or "shopee-tiv" in tt or "shopee-tiv" in fn or "shopee-tir" in tt or "shopee-tir" in fn:
        score["shopee"] += 80

    # ---------- Soft keyword signals ----------
    score["shopee"] += 8 * _count_contains(tt, SHOPEE_SIGS) + 12 * _count_contains(fn, SHOPEE_SIGS)
    score["lazada"] += 8 * _count_contains(tt, LAZADA_SIGS) + 12 * _count_contains(fn, LAZADA_SIGS)
    score["tiktok"] += 8 * _count_contains(tt, TIKTOK_SIGS) + 12 * _count_contains(fn, TIKTOK_SIGS)
    score["spx"]    += 8 * _count_contains(tt, SPX_SIGS)    + 12 * _count_contains(fn, SPX_SIGS)

    # ---------- TRS handling (Shopee weak) ----------
    # Count TRS only if there is Shopee context (shopee/tiv/tir) in text or filename.
    trs_in_text = _regex_hit(tt, RE_SHOPEE_TRS) or ("trs" in tt)
    if trs_in_text:
        has_shopee_context = ("shopee" in tt) or ("tiv" in tt) or ("tir" in tt) or ("shopee" in fn) or ("tiv" in fn) or ("tir" in fn)
        if has_shopee_context:
            score["shopee"] += 15  # modest
        else:
            # no promotion; prevent collisions
            pass

    # ---------- Ads scoring ----------
    # Strong ads keywords are required; weak-only is not enough.
    strong_ads = _count_contains(tt, ADS_SIGS_STRONG) + _count_contains(fn, ADS_SIGS_STRONG)
    weak_ads = _count_contains(tt, ADS_SIGS_WEAK) + _count_contains(fn, ADS_SIGS_WEAK)

    # shipping context blocks ads
    shipping_ctx = _contains_any(tt, NEGATIVE_FOR_ADS) or _contains_any(fn, NEGATIVE_FOR_ADS)

    if not shipping_ctx:
        if strong_ads >= 2:
            score["ads"] += 70
        elif strong_ads >= 1 and weak_ads >= 2:
            score["ads"] += 60
        elif strong_ads >= 1:
            score["ads"] += 45
        # weak-only: do nothing (กันมั่ว)

    return score


def classify_platform(text: str, filename: str = "", debug: bool = False) -> PlatformLabel:
    """
    Platform classifier (robust, not random).

    Args:
        text: PDF text content
        filename: Original filename (helps classification)
        debug: Enable debug logging
    
    Returns:
        Platform label
    
    Key features:
    ✅ Accept filename (queue filenames are highly informative)
    ✅ SPX RCSPX wins (even across whitespace/newlines)
    ✅ Lazada THMPTI wins
    ✅ TikTok TTSTH wins
    ✅ Shopee TIV/TIR wins
    ✅ TRS alone never triggers Shopee
    ✅ Ads requires strong billing signals and no shipping context
    ✅ RELAXED thresholds (was too strict):
       - SPX: 80 → 40
       - Lazada: 70 → 40
       - TikTok: 60 → 30
       - Shopee: 60 → 30
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    
    try:
        t = _norm(text)
        fn = _norm(filename)

        if not t and not fn:
            logger.debug("Empty text and filename -> unknown")
            return "unknown"

        logger.debug(f"Classifying: {filename[:50] if filename else 'no filename'}...")

        # ---- hard-fast path (strong IDs) ----
        if _regex_hit(t, RE_SPX_RCSPX) or _regex_hit(fn, RE_SPX_RCSPX) or ("rcspx" in t) or ("rcspx" in fn):
            logger.info("✅ Fast path: SPX (RCSPX pattern)")
            return "spx"
        
        if _regex_hit(t, RE_LAZADA_THMPTI) or _regex_hit(fn, RE_LAZADA_THMPTI):
            logger.info("✅ Fast path: Lazada (THMPTI pattern)")
            return "lazada"
        
        if _regex_hit(t, RE_TIKTOK_TTSTH) or _regex_hit(fn, RE_TIKTOK_TTSTH) or _regex_hit(t, RE_TIKTOK_WORD) or _regex_hit(fn, RE_TIKTOK_WORD):
            logger.info("✅ Fast path: TikTok (TTSTH/word pattern)")
            return "tiktok"
        
        if _regex_hit(t, RE_SHOPEE_TIV) or _regex_hit(fn, RE_SHOPEE_TIV) or _regex_hit(t, RE_SHOPEE_TIR) or _regex_hit(fn, RE_SHOPEE_TIR) or _regex_hit(t, RE_SHOPEE_WORD) or _regex_hit(fn, RE_SHOPEE_WORD):
            # Strong shopee signals, but still allow scoring if ads is stronger
            pass

        # ---- weighted scoring ----
        score = _weighted_score(t, filename=filename)
        
        logger.debug(f"Scores: {score}")

        # Resolve winner by score
        best_label = max(score.items(), key=lambda kv: kv[1])[0]
        best_score = score[best_label]

        # ============================================================
        # ✅ RELAXED THRESHOLDS (not too strict!)
        # ============================================================
        
        # ✅ Changed from 80 → 40
        if score["spx"] >= 40:
            logger.info(f"✅ Classification: SPX (score: {score['spx']})")
            return "spx"
        
        # ✅ Changed from 70 → 40
        if score["lazada"] >= 40:
            logger.info(f"✅ Classification: Lazada (score: {score['lazada']})")
            return "lazada"
        
        # ✅ Changed from 60 → 30
        if score["tiktok"] >= 30:
            logger.info(f"✅ Classification: TikTok (score: {score['tiktok']})")
            return "tiktok"
        
        # ✅ Changed from 60 → 30
        if score["shopee"] >= 30:
            logger.info(f"✅ Classification: Shopee (score: {score['shopee']})")
            return "shopee"

        # Ads: only if clearly strong
        if score["ads"] >= 60 and score["ads"] > max(score["spx"], score["lazada"], score["tiktok"], score["shopee"]):
            logger.info(f"✅ Classification: Ads (score: {score['ads']})")
            return "ads"

        # If best has modest confidence, still return it
        if best_score >= 25:
            logger.info(f"⚠️  Modest confidence: {best_label} (score: {best_score})")
            return best_label  # type: ignore[return-value]

        # Fallback: looks like invoice but unknown platform
        if _contains_any(t, INVOICE_SIGS) or _contains_any(fn, INVOICE_SIGS):
            logger.info("⚠️  Generic invoice -> other")
            return "other"

        logger.info(f"❌ Unknown platform (scores: {score})")
        return "unknown"
    
    except Exception as e:
        logger.error(f"Classification error: {e}", exc_info=True)
        return "unknown"


def get_classification_details(text: str, filename: str = "") -> Tuple[PlatformLabel, Dict[str, int]]:
    """
    Get classification result WITH scores (for debugging)
    
    Args:
        text: PDF text content
        filename: Original filename
    
    Returns:
        Tuple of (platform, scores_dict)
    
    Example:
        platform, scores = get_classification_details(text, "spx.pdf")
        print(f"Platform: {platform}")
        print(f"Scores: {scores}")
        # Output:
        # Platform: spx
        # Scores: {'shopee': 0, 'lazada': 0, 'tiktok': 0, 'spx': 120, 'ads': 0}
    """
    try:
        t = _norm(text)
        fn = _norm(filename)
        
        # Get scores
        score = _weighted_score(t, filename=filename)
        
        # Get classification
        platform = classify_platform(text, filename, debug=False)
        
        return (platform, score)
    
    except Exception as e:
        logger.error(f"Error getting classification details: {e}")
        return ("unknown", {"shopee": 0, "lazada": 0, "tiktok": 0, "spx": 0, "ads": 0})


__all__ = ["PlatformLabel", "classify_platform", "get_classification_details"]