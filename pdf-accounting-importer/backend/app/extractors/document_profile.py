# -*- coding: utf-8 -*-
"""
document_profile.py (ENHANCED + FIXED)

Create lightweight profiles for each page (keywords, ids, hints) and segments.
Used by multi_page_analyzer + ai_document_router.

Fixes & upgrades vs your draft:
  ✅ Stronger, less noisy ID extraction:
     - Meta Transaction ID: allow long 2-part id with hyphen
     - Google Payment number: Vxxxxxxxx... (or similar)
     - Billing ID: 0000-0000-0000
     - SPX / Shopee doc refs: RCSPX..., TRSPEMKP..., etc.
  ✅ Better invoice/reference extraction: supports Thai + EN and "Payment number/Reference Number"
  ✅ Better platform detection & precedence:
     META/GOOGLE (Ads) > THAI_TAX > SPX > Marketplace (Shopee/Lazada/Tiktok)
  ✅ Fixed typo keyword ("รวมยอดที่ต้ระ" → "รวมยอดที่ชำระ"/"รวมยอดที่ต้องชำระ")
  ✅ Segment merge logic: prefer non-UNKNOWN correctly (your sort lambda was wrong)
  ✅ Safer normalization (keeps newlines for page patterns, but strips nulls)
  ✅ Debuggability: keywords are unique + capped

Design goals kept:
  - stdlib only
  - deterministic
  - debuggable
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


# ============================================================
# Regex (robust)
# ============================================================

def _re_compile(pattern: str, flags: int = 0) -> re.Pattern:
    try:
        return re.compile(pattern, flags)
    except Exception as e:
        logger.error(f"Regex compile failed: {pattern!r} err={e}")
        # fallback to something harmless
        return re.compile(r"(?!x)x")


RE_TAX_ID_13 = _re_compile(r"\b(\d{13})\b")

# Shopee / Marketplace seller id (commonly numeric)
RE_SELLER_ID = _re_compile(
    r"(?:seller\s*id|sellerid|seller\s*ID|Seller\s*ID)\s*[:#]?\s*([0-9]{6,20})",
    re.IGNORECASE
)

# Meta transaction id often like: 25371609625860721-25458101903878164
RE_META_TRANSACTION_ID = _re_compile(
    r"(?:transaction\s*id)\s*[:#]?\s*([0-9]{12,30}\s*-\s*[0-9]{12,30})",
    re.IGNORECASE
)

# Generic transaction/ref id (fallback)
RE_TRANSACTION_GENERIC = _re_compile(
    r"(?:transaction|txn|reference|ref)\s*(?:id|no|number)?\s*[:#]?\s*([A-Z0-9\-]{6,64})",
    re.IGNORECASE
)

# Invoice / doc no (Thai + EN)
RE_INVOICE_NO = _re_compile(
    r"(?:invoice\s*(?:no|number)|เลขที่ใบกำกับ(?:ภาษี)?|เลขที่เอกสาร|เลขที่)\s*[:#]?\s*([A-Z0-9\-/]{4,64})",
    re.IGNORECASE
)

# Google payment number like V0971174339667745
RE_GOOGLE_PAYMENT_NO = _re_compile(
    r"(?:payment\s*number)\s*[:#]?\s*([A-Z][A-Z0-9]{8,32})",
    re.IGNORECASE
)

# Google billing id like 5845-7123-1367
RE_GOOGLE_BILLING_ID = _re_compile(
    r"(?:billing\s*id)\s*[:#]?\s*([0-9]{3,6}\s*-\s*[0-9]{3,6}\s*-\s*[0-9]{3,6})",
    re.IGNORECASE
)

# Meta reference number like 8QDX88ZPM2
RE_META_REFERENCE_NO = _re_compile(
    r"(?:reference\s*number)\s*[:#]?\s*([A-Z0-9]{6,32})",
    re.IGNORECASE
)

# SPX / Shopee logistics refs often like: TRSPEMKP00-00000-251215-0011632 / RCSPX...
RE_DOCREF_SXP_SHOPEE = _re_compile(
    r"\b((?:TR|RC)[A-Z0-9]{3,20}\d{2}-\d{5}-\d{6,8}-\d{6,8})\b",
    re.IGNORECASE
)

RE_PAGE_X_OF_Y = _re_compile(
    r"\bpage\s*(\d{1,3})\s*(?:/|of)\s*(\d{1,3})\b",
    re.IGNORECASE
)


# ============================================================
# Keywords (expanded)
# ============================================================

KEYS_META = [
    "meta", "facebook", "ads manager", "business suite", "receipt",
    "account id", "transaction id", "reference number",
    "meta platforms ireland", "meta platforms, inc",
    "fbads", "fb ads", "instagram ads",
]

KEYS_GOOGLE = [
    "google", "adwords", "google ads", "payment receipt", "payment",
    "google asia pacific", "billing id", "payment number",
    "google advertising",
]

KEYS_SHOPEE = [
    "shopee", "ช้อปปี้", "shopee (thailand)", "shopee.co.th",
    "seller id", "คำสั่งซื้อ", "shopeepay",
]

KEYS_LAZADA = [
    "lazada", "ลาซาด้า", "alibaba", "seller center",
    "lazpay", "lazada.co.th",
]

KEYS_TIKTOK = [
    "tiktok", "tik tok", "tts", "tiktok shop",
    "bytedance", "douyin",
]

KEYS_SPX = [
    "shopee express", "spx", "spx express",
    "shopee logistics", "ขนส่งช้อปปี้",
    "tracking", "waybill",
]

KEYS_THAI_TAX = [
    "ใบเสร็จรับเงิน", "ใบกำกับภาษี", "ใบกำกับภาษีเต็มรูป",
    "tax invoice", "receipt/tax invoice",
    "เลขประจำตัวผู้เสียภาษี", "สำนักงานใหญ่", "สาขา",
    "รวมยอดที่ชำระ", "รวมยอดที่ต้องชำระ", "ภาษีมูลค่าเพิ่ม", "vat 7%",
]


# ============================================================
# Utility
# ============================================================

def _norm_text(s: str) -> str:
    """
    Safe normalize:
      - keep newlines (helps many receipts)
      - remove nulls
      - trim
    """
    try:
        return (s or "").replace("\x00", " ").strip()
    except Exception as e:
        logger.warning(f"Text normalization failed: {e}")
        return str(s or "")


def _contains_any(t: str, keys: List[str]) -> bool:
    try:
        tt = (t or "").lower()
        for k in keys:
            if k.lower() in tt:
                return True
        return False
    except Exception as e:
        logger.warning(f"Keyword matching failed: {e}")
        return False


def _filename_hint(filename: str) -> str:
    fn = (filename or "").lower()
    if not fn:
        return ""

    # ads
    if any(x in fn for x in ["meta", "facebook", "fbads", "instagram"]):
        return "META"
    if any(x in fn for x in ["google", "adwords"]):
        return "GOOGLE"

    # logistics first
    if "spx" in fn or "express" in fn:
        return "SPX"

    # marketplace
    if "shopee" in fn:
        return "SHOPEE"
    if "lazada" in fn or fn.startswith("laz"):
        return "LAZADA"
    if "tiktok" in fn or "tts" in fn:
        return "TIKTOK"

    # thai tax invoice sometimes in name
    if any(x in fn for x in ["tax", "invoice", "receipt", "ใบกำกับ", "ใบเสร็จ"]):
        return "THAI_TAX"

    return ""


# ============================================================
# Platform detection
# ============================================================

def detect_platform_hint(text: str, filename: str = "") -> str:
    """
    Return: "META" | "GOOGLE" | "SHOPEE" | "LAZADA" | "TIKTOK" | "SPX" | "THAI_TAX" | "UNKNOWN"

    Priority:
      META/GOOGLE > THAI_TAX > SPX > marketplace
    """
    try:
        t = (text or "").lower()

        # 1) filename hint (but still allow content override)
        fh = _filename_hint(filename)
        if fh in {"META", "GOOGLE"}:
            return fh
        # for others, we don't immediately return; we confirm with content later

        # 2) ads first
        if _contains_any(t, KEYS_META):
            return "META"
        if _contains_any(t, KEYS_GOOGLE):
            return "GOOGLE"

        # 3) thai tax invoice (require tax id to reduce false positives)
        if _contains_any(t, KEYS_THAI_TAX):
            if RE_TAX_ID_13.search(text or ""):
                return "THAI_TAX"

        # 4) logistics before shopee
        if _contains_any(t, KEYS_SPX) or RE_DOCREF_SXP_SHOPEE.search(text or ""):
            return "SPX"

        # 5) marketplace
        if _contains_any(t, KEYS_SHOPEE):
            return "SHOPEE"
        if _contains_any(t, KEYS_LAZADA):
            return "LAZADA"
        if _contains_any(t, KEYS_TIKTOK):
            return "TIKTOK"

        # 6) fallback to filename hint if it was non-ads
        if fh in {"SPX", "SHOPEE", "LAZADA", "TIKTOK", "THAI_TAX"}:
            return fh

        return "UNKNOWN"
    except Exception as e:
        logger.error(f"Platform detection failed: {e}")
        return "UNKNOWN"


# ============================================================
# ID extractors
# ============================================================

def extract_first_tax_id(text: str) -> str:
    try:
        m = RE_TAX_ID_13.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Tax ID extraction failed: {e}")
        return ""


def extract_seller_id(text: str) -> str:
    try:
        m = RE_SELLER_ID.search(text or "")
        return m.group(1) if m else ""
    except Exception as e:
        logger.warning(f"Seller ID extraction failed: {e}")
        return ""


def extract_transaction_id(text: str, platform_hint: str = "") -> str:
    """
    Prefer platform-specific ids to reduce noise.
    """
    t = text or ""
    p = (platform_hint or "").upper()

    try:
        if p == "META":
            m = RE_META_TRANSACTION_ID.search(t)
            if m:
                return re.sub(r"\s+", "", m.group(1)).strip()
            m2 = RE_META_REFERENCE_NO.search(t)
            if m2:
                return m2.group(1).strip()

        if p == "GOOGLE":
            m = RE_GOOGLE_PAYMENT_NO.search(t)
            if m:
                return m.group(1).strip()
            m2 = RE_GOOGLE_BILLING_ID.search(t)
            if m2:
                return re.sub(r"\s+", "", m2.group(1)).strip()

        # SPX / Shopee doc refs can act like transaction/ref id for segmentation
        if p == "SPX":
            m = RE_DOCREF_SXP_SHOPEE.search(t)
            if m:
                return m.group(1).strip()

        # fallback generic
        m = RE_TRANSACTION_GENERIC.search(t)
        if m:
            return re.sub(r"\s+", "", m.group(1)).strip()

        return ""
    except Exception as e:
        logger.warning(f"Transaction ID extraction failed: {e}")
        return ""


def extract_invoice_no(text: str, platform_hint: str = "") -> str:
    t = text or ""
    p = (platform_hint or "").upper()

    try:
        # For Google, payment number is effectively the invoice/receipt id
        if p == "GOOGLE":
            m = RE_GOOGLE_PAYMENT_NO.search(t)
            if m:
                return m.group(1).strip()

        # For Meta, reference number is the best invoice-like id
        if p == "META":
            m = RE_META_REFERENCE_NO.search(t)
            if m:
                return m.group(1).strip()

        # SPX doc ref can be invoice-like
        if p == "SPX":
            m = RE_DOCREF_SXP_SHOPEE.search(t)
            if m:
                return m.group(1).strip()

        m = RE_INVOICE_NO.search(t)
        if m:
            return re.sub(r"\s+", "", m.group(1)).strip()

        return ""
    except Exception as e:
        logger.warning(f"Invoice number extraction failed: {e}")
        return ""


def extract_page_x_of_y(text: str) -> Tuple[int, int]:
    try:
        m = RE_PAGE_X_OF_Y.search((text or ""))
        if not m:
            return (0, 0)
        return (int(m.group(1)), int(m.group(2)))
    except Exception:
        return (0, 0)


# ============================================================
# Doc kind
# ============================================================

def guess_doc_kind(platform_hint: str, text: str) -> str:
    """
    Rough doc kind used for grouping/segmentation:
      - META_RECEIPT
      - GOOGLE_PAYMENT
      - MARKETPLACE_BILL
      - SPX_WAYBILL
      - THAI_TAX_INVOICE
      - GENERIC
    """
    try:
        t = (text or "").lower()
        p = (platform_hint or "UNKNOWN").upper()

        if p == "META":
            if "receipt" in t or "transaction id" in t or "reference number" in t:
                return "META_RECEIPT"
            return "META_DOC"

        if p == "GOOGLE":
            if "payment receipt" in t or "payment number" in t or "billing id" in t:
                return "GOOGLE_PAYMENT"
            return "GOOGLE_DOC"

        if p == "SPX":
            if "waybill" in t or "tracking" in t or "shopee express" in t:
                return "SPX_WAYBILL"
            return "SPX_DOC"

        if p == "THAI_TAX":
            if "ใบกำกับภาษีเต็มรูป" in t or "tax invoice" in t:
                return "THAI_TAX_INVOICE"
            if "ใบเสร็จรับเงิน" in t or "receipt" in t:
                return "THAI_RECEIPT"
            return "THAI_TAX_DOC"

        if p in {"SHOPEE", "LAZADA", "TIKTOK"}:
            return "MARKETPLACE_BILL"

        return "GENERIC"
    except Exception as e:
        logger.error(f"Doc kind detection failed: {e}")
        return "GENERIC"


# ============================================================
# Profiles
# ============================================================

@dataclass
class PageProfile:
    page_index: int
    text_len: int
    platform_hint: str
    doc_kind: str
    tax_id_13: str = ""
    seller_id: str = ""
    transaction_id: str = ""
    invoice_no: str = ""
    page_x: int = 0
    page_y: int = 0
    keywords: List[str] = field(default_factory=list)

    def to_meta(self) -> Dict[str, Any]:
        return {
            "page_index": self.page_index,
            "text_len": self.text_len,
            "platform_hint": self.platform_hint,
            "doc_kind": self.doc_kind,
            "tax_id_13": self.tax_id_13,
            "seller_id": self.seller_id,
            "transaction_id": self.transaction_id,
            "invoice_no": self.invoice_no,
            "page_x": self.page_x,
            "page_y": self.page_y,
            "keywords": self.keywords,
        }


@dataclass
class SegmentProfile:
    segment_index: int
    page_indices: List[int]
    merged_text_len: int
    platform_hint: str
    doc_kind: str
    tax_id_13: str = ""
    seller_id: str = ""
    transaction_id: str = ""
    invoice_no: str = ""
    reasons: List[str] = field(default_factory=list)

    def to_meta(self) -> Dict[str, Any]:
        return {
            "segment_index": self.segment_index,
            "page_indices": self.page_indices,
            "merged_text_len": self.merged_text_len,
            "platform_hint": self.platform_hint,
            "doc_kind": self.doc_kind,
            "tax_id_13": self.tax_id_13,
            "seller_id": self.seller_id,
            "transaction_id": self.transaction_id,
            "invoice_no": self.invoice_no,
            "reasons": self.reasons,
        }


# ============================================================
# Builders
# ============================================================

def build_page_profile(page_index: int, page_text: str, filename: str = "") -> PageProfile:
    try:
        t = _norm_text(page_text)
        ph = detect_platform_hint(t, filename=filename)
        kind = guess_doc_kind(ph, t)

        tax = extract_first_tax_id(t)
        seller = extract_seller_id(t)
        txn = extract_transaction_id(t, platform_hint=ph)
        inv = extract_invoice_no(t, platform_hint=ph)
        px, py = extract_page_x_of_y(t)

        # keywords: unique & capped
        keys_all = (
            KEYS_META + KEYS_GOOGLE + KEYS_SPX +
            KEYS_THAI_TAX + KEYS_SHOPEE + KEYS_LAZADA + KEYS_TIKTOK
        )

        found: List[str] = []
        tt = t.lower()
        for k in keys_all:
            kk = k.lower()
            if kk in tt:
                found.append(k)

        # de-dup (preserve order)
        seen = set()
        keywords: List[str] = []
        for k in found:
            if k not in seen:
                keywords.append(k)
                seen.add(k)
            if len(keywords) >= 30:
                break

        return PageProfile(
            page_index=page_index,
            text_len=len(t),
            platform_hint=ph,
            doc_kind=kind,
            tax_id_13=tax,
            seller_id=seller,
            transaction_id=txn,
            invoice_no=inv,
            page_x=px,
            page_y=py,
            keywords=keywords,
        )
    except Exception as e:
        logger.error(f"Page profile building failed: {e}")
        return PageProfile(
            page_index=page_index,
            text_len=len(page_text or ""),
            platform_hint="UNKNOWN",
            doc_kind="GENERIC",
        )


def merge_segment_profile(segment_index: int, pages: List[PageProfile], merged_text: str) -> SegmentProfile:
    try:
        ph_counts: Dict[str, int] = {}
        kind_counts: Dict[str, int] = {}

        for p in pages:
            ph_counts[p.platform_hint] = ph_counts.get(p.platform_hint, 0) + 1
            kind_counts[p.doc_kind] = kind_counts.get(p.doc_kind, 0) + 1

        # choose platform: prefer non-UNKNOWN; then max count; then stable name
        def _ph_sort(item: Tuple[str, int]) -> Tuple[int, int, str]:
            name, cnt = item
            is_unknown = 1 if name == "UNKNOWN" else 0
            return (is_unknown, -cnt, name)

        ph = "UNKNOWN"
        if ph_counts:
            ph = sorted(ph_counts.items(), key=_ph_sort)[0][0]

        kind = "GENERIC"
        if kind_counts:
            kind = sorted(kind_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

        # IDs: first non-empty in page order
        tax = ""
        seller = ""
        txn = ""
        inv = ""
        for p in pages:
            if not tax and p.tax_id_13:
                tax = p.tax_id_13
            if not seller and p.seller_id:
                seller = p.seller_id
            if not txn and p.transaction_id:
                txn = p.transaction_id
            if not inv and p.invoice_no:
                inv = p.invoice_no

        reasons = [
            f"platform={ph} counts={ph_counts}",
            f"doc_kind={kind} counts={kind_counts}",
        ]

        return SegmentProfile(
            segment_index=segment_index,
            page_indices=[p.page_index for p in pages],
            merged_text_len=len(_norm_text(merged_text)),
            platform_hint=ph,
            doc_kind=kind,
            tax_id_13=tax,
            seller_id=seller,
            transaction_id=txn,
            invoice_no=inv,
            reasons=reasons,
        )
    except Exception as e:
        logger.error(f"Segment profile merging failed: {e}")
        return SegmentProfile(
            segment_index=segment_index,
            page_indices=[p.page_index for p in pages],
            merged_text_len=len(merged_text or ""),
            platform_hint="UNKNOWN",
            doc_kind="GENERIC",
            reasons=[f"error: {str(e)[:120]}"],
        )


__all__ = [
    "PageProfile",
    "SegmentProfile",
    "build_page_profile",
    "merge_segment_profile",
    "detect_platform_hint",
    "guess_doc_kind",
    "extract_first_tax_id",
    "extract_seller_id",
    "extract_transaction_id",
    "extract_invoice_no",
    "extract_page_x_of_y",
]
