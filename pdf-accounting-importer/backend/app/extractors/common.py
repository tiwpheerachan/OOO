# -*- coding: utf-8 -*-
# backend/app/extractors/common.py
"""
Common utilities and patterns for invoice extraction
Enhanced with AI-ready patterns for better accuracy
Version 3.3.2 (Finalize Row: client_tax_ids + compute_wht enforcement)

⭐ Fixes / requirements covered
✅ 1) เพิ่ม/คงไว้ finalize_row (กัน ImportError)
✅ 2) base_row_dict มีคอลัมน์ A_company_name / O_vat_rate / P_wht ครบ + format_peak_row ไม่ทำหาย
✅ 3) C_reference / G_invoice_no บังคับเป็นเลขจาก filename แบบ "TRS...." (ตัด Shopee-TIV- ฯลฯ)
✅ 4) L_description ตาม Desc structure + ใส่ Seller ID / Username / File
✅ 5) K_account เติม GL Code ให้ครบ SHD/Rabbit/TopOne (รวม ads_canva ให้ครบ)
✅ 6) NEW: finalize_row ดึง client_tax_id ได้จาก client_tax_ids (list/str) + รองรับหลายชื่อ field
✅ 7) NEW: finalize_row อ่าน compute_wht แล้วบังคับ P_wht/S_pnd (✅คำนวณ / ❌ไม่คำนวณ)
"""

from __future__ import annotations

import os
import re
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime
from decimal import Decimal, InvalidOperation


# ============================================================
# Text normalization utilities
# ============================================================

_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\ufeff]")

# Whitespace rules:
# - keep '\n' for multiline parsing (tables/lines)
# - collapse spaces/tabs within a line
# - normalize CRLF -> LF
_WS_INLINE_RE = re.compile(r"[ \t\f\v]+")
_WS_MANY_NL_RE = re.compile(r"\n{3,}")

# Reference cleanup
_WS_ANY_RE = re.compile(r"\s+")


def squash_all_ws(text: str) -> str:
    """Remove ALL whitespace (space/newline/tab) - used for strict reference outputs."""
    if not text:
        return ""
    return _WS_ANY_RE.sub("", str(text))


def normalize_reference_no_space(ref: str) -> str:
    """
    Normalize reference/invoice number to strict no-space format.
    - remove all whitespace
    - trim quote/punct tails
    - keep hyphen/slash/underscore/dot as-is (but without spaces)
    """
    if not ref:
        return ""
    s = str(ref).strip().strip('"').strip("'")
    s = squash_all_ws(s)
    s = re.sub(r"[,\.;:]+$", "", s)
    return s


def normalize_text(text: str) -> str:
    """
    Normalize text for pattern matching (LINE-AWARE)

    IMPORTANT:
    - Keep newlines so that MULTILINE patterns ( ^...$ ) still work.
    - Remove zero-width chars.
    - Collapse excessive spaces/tabs but do NOT merge lines.
    """
    if not text:
        return ""

    s = str(text)

    # Normalize line breaks
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # Remove zero-width characters
    s = _ZERO_WIDTH_RE.sub("", s)

    # Normalize spaces inside each line (keep line structure)
    lines: List[str] = []
    for line in s.split("\n"):
        line = _WS_INLINE_RE.sub(" ", line).strip()
        lines.append(line)

    s = "\n".join(lines).strip()
    s = _WS_MANY_NL_RE.sub("\n\n", s)  # prevent insane blank pages
    return s


def normalize_one_line(text: str) -> str:
    """Single-line normalization (for patterns that don't need line anchors)."""
    if not text:
        return ""
    s = normalize_text(text)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def fmt_tax_13(raw: str) -> str:
    """Format to 13-digit tax ID (0105561071873)"""
    if not raw:
        return ""
    digits = re.sub(r"\D", "", str(raw))
    return digits if len(digits) == 13 else ""


def fmt_branch_5(raw: str) -> str:
    """Format to 5-digit branch code (00000)"""
    if not raw:
        return "00000"
    digits = re.sub(r"\D", "", str(raw))
    if digits == "":
        return "00000"
    return digits.zfill(5)[:5]


def parse_date_to_yyyymmdd(date_str: str) -> str:
    """
    Parse various date formats to YYYYMMDD
    Supports: DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD, etc.
    """
    if not date_str:
        return ""
    s = str(date_str).strip()

    # Allow if it's already YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        try:
            datetime.strptime(s, "%Y%m%d")
            return s
        except Exception:
            pass

    formats = [
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%d/%m/%y", "%d-%m-%y", "%d.%m.%y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.strftime("%Y%m%d")
        except Exception:
            continue

    return ""


def parse_en_date(date_str: str) -> str:
    """Parse English date formats like 'Dec 9, 2025' to YYYYMMDD"""
    if not date_str:
        return ""
    s = str(date_str).strip()

    formats = [
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d %Y",
        "%B %d %Y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y%m%d")
        except Exception:
            continue

    return ""


def parse_money(value: str) -> str:
    """
    Parse money string to decimal format
    Removes commas, handles Thai baht symbol
    Returns string with 2 decimal places or empty if invalid
    """
    if value is None:
        return ""

    s = str(value)
    s = s.replace("฿", "").replace("THB", "").replace("บาท", "")
    s = s.replace(",", "").strip()

    # Common OCR artifacts
    s = s.replace("—", "-").replace("–", "-")

    # Disallow negative (for this project)
    try:
        amount = Decimal(s)
        if amount < 0:
            return ""
        return f"{amount:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def safe_decimal(s: str) -> Decimal:
    try:
        return Decimal(str(s).replace(",", "").strip())
    except Exception:
        return Decimal("0")


# ============================================================
# Core patterns (enhanced with full reference support)
# ============================================================

# Tax ID patterns
RE_TAX13 = re.compile(r"(\d[\d\s-]{11,20}\d)")
RE_TAX13_STRICT = re.compile(r"\b([0-9]{13})\b")

# Branch patterns
RE_BRANCH_HEAD_OFFICE = re.compile(r"(?:สำนักงานใหญ่|Head\s*Office|HeadOffice|本社)", re.IGNORECASE)
RE_BRANCH_NUM = re.compile(r"(?:สาขา(?:ที่)?\s*|Branch\s*(?:No\.?|Number)?\s*:?\s*)(\d{1,5})", re.IGNORECASE)

# Date patterns
RE_DATE_DMYYYY = re.compile(r"(\d{1,2})[\-/\. ](\d{1,2})[\-/\. ](\d{4})")
RE_DATE_YYYYMD = re.compile(r"(\d{4})[\-/\. ](\d{1,2})[\-/\. ](\d{1,2})")
RE_DATE_8DIGIT = re.compile(r"\b(\d{8})\b")
RE_DATE_EN = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})\b",
    re.IGNORECASE
)

# ============================================================
# Invoice/Document number patterns (ENHANCED - full reference)
# ============================================================

RE_INVOICE_WITH_REF = re.compile(
    r"\b([A-Z]{2,}[A-Z0-9\-/_.]{6,})\s+(\d{4})\s*-\s*(\d{6,9})\b",
    re.IGNORECASE
)

RE_INVOICE_WITH_LONG_REF = re.compile(
    r"\b([A-Z]{2,}[A-Z0-9\-/_.]{6,})\s+(\d{2,4})\s*[-/]\s*(\d{6,10})\b",
    re.IGNORECASE
)

RE_INVOICE_GENERIC = re.compile(
    r"(?:ใบกำกับ(?:ภาษี)?|Tax\s*Invoice|Invoice|เลขที่(?:เอกสาร)?|Document\s*(?:No\.?|Number)|Doc\s*No\.?|Receipt\s*No\.?)"
    r"\s*[:#：]?\s*[\"']?\s*([A-Za-z0-9\-/_.]+)",
    re.IGNORECASE
)

# Platform-specific doc patterns
RE_SPX_DOC = re.compile(r"\b(RCS[A-Z0-9\-/]{10,})\b", re.IGNORECASE)
RE_SPX_DOC_WITH_REF = re.compile(r"\b(RCS[A-Z0-9\-/]{10,})\s+(\d{4})\s*-\s*(\d{7})\b", re.IGNORECASE)

RE_SHOPEE_DOC = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-_/]{8,})\b",
    re.IGNORECASE
)
RE_SHOPEE_DOC_WITH_REF = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-_/]{8,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE
)

RE_LAZADA_DOC = re.compile(
    r"\b(THMPTI\d{16}|(?:LAZ|LZD)[A-Z0-9\-_/.]{6,}|INV[A-Z0-9\-_/.]{6,})\b",
    re.IGNORECASE
)
RE_LAZADA_DOC_WITH_REF = re.compile(
    r"\b(THMPTI\d{16}|(?:LAZ|LZD)[A-Z0-9\-_/.]{6,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE
)

RE_TIKTOK_DOC = re.compile(r"\b(TTSTH\d{14,})\b", re.IGNORECASE)
RE_TIKTOK_DOC_WITH_REF = re.compile(r"\b(TTSTH\d{14,})\s+(\d{4})\s*-\s*(\d{7})\b", re.IGNORECASE)

# Standalone reference code (allow spaces around dash for OCR)
RE_REFERENCE_CODE = re.compile(r"\b(\d{4})\s*-\s*(\d{6,9})\b")
RE_REFERENCE_CODE_NODASH = re.compile(r"\b(\d{4})(\d{6,9})\b")

# Seller / shop meta
RE_SELLER_ID = re.compile(
    r"(?:Seller\s*ID|Shop\s*ID|Store\s*ID|รหัสร้านค้า)\s*[:#：]?\s*([A-Z0-9_\-]+)",
    re.IGNORECASE
)
RE_USERNAME = re.compile(
    r"(?:Username|User\s*name|ชื่อผู้ใช้)\s*[:#：]?\s*([A-Za-z0-9_\-]+)",
    re.IGNORECASE
)
RE_SELLER_CODE = re.compile(r"\b([A-Z0-9]{8,15})\b")

# Amount patterns
RE_TOTAL_INC_VAT = re.compile(
    r"(?:Total\s*(?:amount)?\s*(?:\()?(?:Including|incl\.?|รวม)\s*(?:VAT|Tax|ภาษี)(?:\))?"
    r"|Grand\s*Total|Amount\s*Due|Total\s*Due|ยอด(?:ที่)?ชำระ|ยอดรวมทั้งสิ้น|รวมยอด(?:ที่)?(?:ชำระ|ต้องชำระ))"
    r"\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)

RE_TOTAL_EX_VAT = re.compile(
    r"(?:Total\s*(?:amount)?\s*(?:\()?(?:Excluding|excl\.?|ก่อน|ไม่รวม)\s*(?:VAT|Tax|ภาษี)(?:\))?"
    r"|Subtotal|รวม(?:เงิน)?ก่อน(?:VAT|ภาษี)|จำนวนเงิน(?:รวม)?)"
    r"\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)

RE_VAT_AMOUNT = re.compile(
    r"(?:Total\s*VAT|VAT\s*(?:7%)?|ภาษีมูลค่าเพิ่ม(?:\s*7%)?)"
    r"\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)

RE_WHT_AMOUNT = re.compile(
    r"(?:ภาษี(?:เงินได้)?(?:หัก)?(?:\s*ณ\s*ที่จ่าย)?|Withholding\s*Tax|WHT|Withheld\s*Tax)"
    r"(?:.*?(?:อัตรา|rate|ร้อยละ)\s*([0-9]{1,2})\s*%)?"
    r"(?:.*?(?:จำนวน(?:เงิน)?|amounting\s*to|เป็นจำนวน))?\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL
)

RE_WHT_HINT = re.compile(r"(withholding\s+tax|หักภาษี|ณ\s*ที่จ่าย|wht)", re.IGNORECASE)

# Payment method patterns
RE_PAYMENT_METHOD = re.compile(
    r"\b(EWL\d{2,6}|TRF\d{2,6}|CSH\d{2,6}|QR|CASH|CARD|TRANSFER|BANK\s*TRANSFER|CREDIT\s*CARD"
    r"|หักจาก(?:ยอด)?ขาย|โอน|เงินสด)\b",
    re.IGNORECASE
)

# Vendor detection patterns
RE_VENDOR_SHOPEE = re.compile(r"(?:Shopee|ช็อปปี้|ช้อปปี้)", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"(?:Lazada|ลาซาด้า)", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"(?:TikTok|ติ๊กต๊อก)", re.IGNORECASE)
RE_VENDOR_SPX = re.compile(r"(?:SPX\s*Express|Shopee\s*Express|Standard\s*Express)", re.IGNORECASE)

# Known client tax IDs (your companies)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

DATE_ANCHOR_KEYWORDS = [
    "invoice date", "tax invoice", "invoice", "receipt", "issue date", "date",
    "วันที่", "วันที", "ออกใบกำกับ", "ออกใบกํากับ", "วันที่ออก",
]


# ============================================================
# Row template (PEAK A-U format)
# ============================================================

def base_row_dict() -> Dict[str, Any]:
    """
    PEAK A-U format template
    ใช้สำหรับ import เข้าระบบบัญชี PEAK
    """
    return {
        "A_seq": "1",
        "A_company_name": "",      # ✅ MUST EXIST for XLSX
        "B_doc_date": "",
        "C_reference": "",
        "D_vendor_code": "",
        "E_tax_id_13": "",
        "F_branch_5": "",
        "G_invoice_no": "",
        "H_invoice_date": "",
        "I_tax_purchase_date": "",
        "J_price_type": "1",
        "K_account": "",
        "L_description": "",
        "M_qty": "1",
        "N_unit_price": "0",
        "O_vat_rate": "7%",        # ✅ MUST EXIST for XLSX
        "P_wht": "0",              # ✅ MUST EXIST for XLSX (rate-only)
        "Q_payment_method": "",
        "R_paid_amount": "0",
        "S_pnd": "",
        "T_note": "",
        "U_group": "",
    }


# ============================================================
# Enhanced extraction functions
# ============================================================

def detect_platform_vendor(text: str) -> Tuple[str, str]:
    """
    Detect platform/vendor from document text
    Returns: (vendor_full_name, vendor_code)
    """
    t = normalize_one_line(text)

    # ✅ prioritize SPX before Shopee
    if RE_VENDOR_SPX.search(t):
        return ("SPX Express (Thailand) Co., Ltd.", "SPX")
    if RE_VENDOR_SHOPEE.search(t):
        return ("Shopee (Thailand) Co., Ltd.", "Shopee")
    if RE_VENDOR_LAZADA.search(t):
        return ("Lazada Limited", "Lazada")
    if RE_VENDOR_TIKTOK.search(t):
        return ("TikTok Shop (Thailand) Ltd.", "TikTok")

    return ("", "")


def _tax_id_candidates_with_positions(text: str) -> List[Tuple[int, str]]:
    """Return list of (pos, tax_id_13) found in text."""
    t = normalize_text(text)
    out: List[Tuple[int, str]] = []
    for m in RE_TAX13_STRICT.finditer(t):
        out.append((m.start(), m.group(1)))
    return out


def find_vendor_tax_id(text: str, vendor_code: str = "") -> str:
    """
    Extract vendor/seller tax ID (not customer tax ID)
    """
    t = normalize_text(text)

    # Vendor-specific patterns
    if vendor_code == "SPX":
        m = re.search(r"(?:Tax\s*ID\s*(?:No\.?|Number)?|เลขประจำตัวผู้เสียภาษี)\s*[:#：]?\s*([0-9]{13})", t, re.IGNORECASE)
        if m and m.group(1) not in CLIENT_TAX_IDS:
            return m.group(1)

    if vendor_code == "TikTok":
        m = re.search(r"(?:Tax\s*Registration\s*Number|เลขประจำตัวผู้เสียภาษี)\s*[:#：]?\s*([0-9]{13})", t, re.IGNORECASE)
        if m and m.group(1) not in CLIENT_TAX_IDS:
            return m.group(1)

    if vendor_code in ("Shopee", "Lazada"):
        patterns = [
            r"(?:เลขประจำตัวผู้เสียภาษี(?:อากร)?|Tax\s*(?:ID|Registration)\s*(?:No\.?|Number)?)\s*[:#：]?\s*([0-9]{13})",
        ]
        for pat in patterns:
            m = re.search(pat, t, re.IGNORECASE)
            if m:
                tax_id = m.group(1)
                if tax_id and tax_id not in CLIENT_TAX_IDS:
                    return tax_id

    # Proximity scoring fallback
    candidates = _tax_id_candidates_with_positions(t)
    if not candidates:
        return ""

    vendor_kw = ""
    if vendor_code == "Shopee":
        vendor_kw = "shopee"
    elif vendor_code == "Lazada":
        vendor_kw = "lazada"
    elif vendor_code == "TikTok":
        vendor_kw = "tiktok"
    elif vendor_code == "SPX":
        vendor_kw = "spx"

    anchor_positions: List[int] = []
    if vendor_kw:
        for m in re.finditer(vendor_kw, t, re.IGNORECASE):
            anchor_positions.append(m.start())

    best_tax = ""
    best_score: Optional[int] = None

    for pos, tax in candidates:
        if tax in CLIENT_TAX_IDS:
            continue
        if not anchor_positions:
            return tax
        dist = min(abs(pos - a) for a in anchor_positions)
        if best_score is None or dist < best_score:
            best_score = dist
            best_tax = tax

    return best_tax or ""


def find_branch(text: str) -> str:
    """Extract branch code (00000 for head office)"""
    t = normalize_text(text)

    if RE_BRANCH_HEAD_OFFICE.search(t):
        return "00000"

    m = RE_BRANCH_NUM.search(t)
    if m:
        return fmt_branch_5(m.group(1))

    return "00000"


def _find_reference_code_near(text: str, doc_number: str, max_distance: int = 140) -> str:
    """
    Find reference code (MMDD-NNNNNNN) near a document number.
    Returns "MMDD-NNNNNNN" (normalized)
    """
    if not doc_number:
        return ""

    pos = text.find(doc_number)
    if pos != -1:
        start = max(0, pos - max_distance)
        end = min(len(text), pos + len(doc_number) + max_distance)
        nearby = text[start:end]

        m = RE_REFERENCE_CODE.search(nearby)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

        m2 = RE_REFERENCE_CODE_NODASH.search(squash_all_ws(nearby))
        if m2:
            return f"{m2.group(1)}-{m2.group(2)}"

        return ""

    # Squashed fallback
    t_sq = squash_all_ws(text)
    d_sq = squash_all_ws(doc_number)
    p2 = t_sq.find(d_sq)
    if p2 == -1:
        return ""

    start = max(0, p2 - max_distance * 2)
    end = min(len(t_sq), p2 + len(d_sq) + max_distance * 2)
    nearby = t_sq[start:end]

    m = RE_REFERENCE_CODE.search(nearby)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m2 = RE_REFERENCE_CODE_NODASH.search(nearby)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}"

    return ""


def _clean_doc_number(s: str) -> str:
    if not s:
        return ""
    x = str(s).strip().strip('"').strip("'")
    x = re.sub(r"[,\.;:]+$", "", x)
    return x


def find_invoice_no(text: str, platform: str = "") -> str:
    """
    Extract invoice/document number with full reference.
    ✅ Returns STRICT NO-SPACE reference by default.
    """
    t = normalize_text(text)

    def pack(doc: str, ref: str = "") -> str:
        if not doc:
            return ""
        s = f"{doc}{ref}" if ref else doc
        return normalize_reference_no_space(s)

    # Platform-specific WITH reference first
    if platform == "SPX":
        m = RE_SPX_DOC_WITH_REF.search(t)
        if m:
            return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
        m = RE_SPX_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    if platform == "Shopee":
        m = RE_SHOPEE_DOC_WITH_REF.search(t)
        if m:
            return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
        m = RE_SHOPEE_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    if platform == "Lazada":
        m = RE_LAZADA_DOC_WITH_REF.search(t)
        if m:
            return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
        m = RE_LAZADA_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    if platform == "TikTok":
        m = RE_TIKTOK_DOC_WITH_REF.search(t)
        if m:
            return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
        m = RE_TIKTOK_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    # Generic full reference patterns
    m = RE_INVOICE_WITH_REF.search(t)
    if m:
        doc = m.group(1)
        ref = f"{m.group(2)}-{m.group(3)}"
        return pack(doc, ref)

    m = RE_INVOICE_WITH_LONG_REF.search(t)
    if m:
        doc = m.group(1)
        ref = f"{m.group(2)}-{m.group(3)}"
        return pack(doc, ref)

    # Any platform doc WITH ref (generic try)
    for pat in (RE_SPX_DOC_WITH_REF, RE_SHOPEE_DOC_WITH_REF, RE_LAZADA_DOC_WITH_REF, RE_TIKTOK_DOC_WITH_REF):
        m = pat.search(t)
        if m:
            if m.lastindex and m.lastindex >= 3:
                return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
            if m.lastindex and m.lastindex >= 2:
                return pack(m.group(1), m.group(2))
            return pack(m.group(1))

    # Platform patterns (without ref)
    for pat in (RE_SPX_DOC, RE_SHOPEE_DOC, RE_LAZADA_DOC, RE_TIKTOK_DOC):
        m = pat.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    # Generic invoice field (fallback)
    m = RE_INVOICE_GENERIC.search(t)
    if m:
        doc = _clean_doc_number(m.group(1))
        if len(doc) >= 6:
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    return ""


def _date_candidates_with_positions(text: str) -> List[Tuple[int, str]]:
    """Return list of (pos, yyyymmdd) candidates extracted from multiple patterns."""
    t = normalize_text(text)
    out: List[Tuple[int, str]] = []

    for m in RE_DATE_EN.finditer(t):
        ds = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
        y = parse_en_date(ds)
        if y:
            out.append((m.start(), y))

    for m in RE_DATE_YYYYMD.finditer(t):
        ds = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        y = parse_date_to_yyyymmdd(ds)
        if y:
            out.append((m.start(), y))

    for m in RE_DATE_DMYYYY.finditer(t):
        ds = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
        y = parse_date_to_yyyymmdd(ds)
        if y:
            out.append((m.start(), y))

    for m in RE_DATE_8DIGIT.finditer(t):
        y = parse_date_to_yyyymmdd(m.group(1))
        if y:
            out.append((m.start(), y))

    return out


def find_best_date(text: str) -> str:
    """
    Find best date from text (YYYYMMDD).
    Prefer date near anchor keywords; tie-breaker: latest date.
    """
    t = normalize_text(text)
    candidates = _date_candidates_with_positions(t)
    if not candidates:
        return ""

    anchors: List[int] = []
    low = t.lower()
    for kw in DATE_ANCHOR_KEYWORDS:
        idx = low.find(kw)
        while idx != -1:
            anchors.append(idx)
            idx = low.find(kw, idx + 1)

    def score(pos: int) -> int:
        if not anchors:
            return 10_000_000
        return min(abs(pos - a) for a in anchors)

    best = None  # (score, -date_int, yyyymmdd)
    for pos, y in candidates:
        try:
            y_int = int(y)
        except Exception:
            continue
        key = (score(pos), -y_int, y)
        if best is None or key < best:
            best = key

    return best[2] if best else ""


def extract_seller_info(text: str) -> Dict[str, str]:
    """Extract seller/shop information"""
    t = normalize_text(text)
    info = {"seller_id": "", "username": "", "seller_code": ""}

    m = RE_SELLER_ID.search(t)
    if m:
        info["seller_id"] = m.group(1)

    m = RE_USERNAME.search(t)
    if m:
        info["username"] = m.group(1)

    # seller_code: pick the first plausible code
    for m in RE_SELLER_CODE.finditer(t):
        code = m.group(1)
        if code.isdigit():
            continue
        if code.startswith("010") and len(code) == 13:
            continue
        if len(code) >= 8:
            info["seller_code"] = code
            break

    return info


def _best_amount_candidate(matches: List[Tuple[int, str]], anchors: List[str], text: str) -> str:
    """
    Choose best amount candidate by proximity to anchors + sane numeric check.
    ✅ anti-WHT pollution: ignore matches whose context contains WHT hints.
    """
    if not matches:
        return ""

    t_low = text.lower()
    anchor_pos: List[int] = []
    for kw in anchors:
        idx = t_low.find(kw)
        while idx != -1:
            anchor_pos.append(idx)
            idx = t_low.find(kw, idx + 1)

    def dist(pos: int) -> int:
        if not anchor_pos:
            return 10_000_000
        return min(abs(pos - a) for a in anchor_pos)

    best = None  # (dist, -amount, str)
    for pos, amt_str in matches:
        ctx = text[max(0, pos - 80): min(len(text), pos + 120)]
        if RE_WHT_HINT.search(ctx):
            continue

        amt = parse_money(amt_str)
        if not amt:
            continue

        a = safe_decimal(amt)
        if a <= 0:
            continue

        key = (dist(pos), -a, amt)
        if best is None or key < best:
            best = key

    return best[2] if best else ""


def extract_amounts(text: str) -> Dict[str, str]:
    """Extract subtotal/vat/total/wht (rate-only for P_wht later)."""
    t = normalize_text(text)
    amounts = {
        "subtotal": "",
        "vat": "",
        "total": "",
        "wht_rate": "",
        "wht_amount": "",
    }

    total_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_TOTAL_INC_VAT.finditer(t)]
    sub_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_TOTAL_EX_VAT.finditer(t)]
    vat_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_VAT_AMOUNT.finditer(t)]

    amounts["total"] = _best_amount_candidate(
        total_matches,
        anchors=["total", "grand total", "amount due", "ยอดชำระ", "ยอดรวม", "including", "รวมยอด"],
        text=t,
    )
    amounts["subtotal"] = _best_amount_candidate(
        sub_matches,
        anchors=["subtotal", "excluding", "ก่อน", "ไม่รวม", "รวมก่อน"],
        text=t,
    )
    amounts["vat"] = _best_amount_candidate(
        vat_matches,
        anchors=["vat", "ภาษีมูลค่าเพิ่ม", "total vat"],
        text=t,
    )

    # WHT
    wht_best = None  # (dist, amount, rate)
    for m in RE_WHT_AMOUNT.finditer(t):
        rate = ""
        amt_raw = ""
        if m.lastindex and m.lastindex >= 2:
            if m.group(1):
                rate = f"{m.group(1)}%"
            amt_raw = m.group(2)
        else:
            amt_raw = (m.group(1) if m.lastindex == 1 else "")

        amt = parse_money(amt_raw)
        if not amt:
            continue

        pos = m.start()
        ctx = t[max(0, pos - 100): min(len(t), pos + 160)]
        if not RE_WHT_HINT.search(ctx):
            continue

        d = 0
        try:
            low = ctx.lower()
            hits = []
            for kw in ["withholding", "wht", "หักภาษี", "ณ ที่จ่าย"]:
                p = low.find(kw)
                if p != -1:
                    hits.append(p)
            d = min(hits) if hits else 0
        except Exception:
            d = 0

        if wht_best is None:
            wht_best = (d, amt, rate)
        else:
            cur_d, cur_amt, _cur_rate = wht_best
            if d < cur_d:
                wht_best = (d, amt, rate)
            elif d == cur_d:
                if safe_decimal(amt) < safe_decimal(cur_amt):
                    wht_best = (d, amt, rate)

    if wht_best:
        amounts["wht_amount"] = wht_best[1]
        amounts["wht_rate"] = wht_best[2] or ""

    # never allow total == wht_amount
    if amounts["total"] and amounts["wht_amount"] and amounts["total"] == amounts["wht_amount"]:
        amounts["total"] = ""

    # calculate missing
    try:
        if amounts["subtotal"] and amounts["vat"] and not amounts["total"]:
            sub = safe_decimal(amounts["subtotal"])
            v = safe_decimal(amounts["vat"])
            if sub > 0 and v >= 0:
                amounts["total"] = f"{(sub + v):.2f}"

        if amounts["total"] and amounts["vat"] and not amounts["subtotal"]:
            tot = safe_decimal(amounts["total"])
            v = safe_decimal(amounts["vat"])
            if tot > 0 and v >= 0 and tot >= v:
                amounts["subtotal"] = f"{(tot - v):.2f}"

        if amounts["subtotal"] and not amounts["vat"]:
            sub = safe_decimal(amounts["subtotal"])
            if sub > 0:
                v = (sub * Decimal("0.07"))
                amounts["vat"] = f"{v:.2f}"
                if not amounts["total"]:
                    amounts["total"] = f"{(sub + v):.2f}"
    except Exception:
        pass

    return amounts


def find_payment_method(text: str, platform: str = "") -> str:
    """Extract payment method"""
    t = normalize_text(text)

    if platform in ("Shopee", "Lazada", "TikTok"):
        low = t.lower()
        if "หักจาก" in t or "deduct" in low:
            return "หักจากยอดขาย"

    m = RE_PAYMENT_METHOD.search(t)
    if m:
        return m.group(1).upper().replace(" ", "")

    return ""


# ============================================================
# ✅ Post-process Central Enforcer (C/G + filename + K_account + L_description)
# ============================================================

CLIENT_SHD     = "0105563022918"
CLIENT_RABBIT  = "0105561071873"
CLIENT_TOPONE  = "0105565027615"

# ========== GL CODE MATRIX (ตามรูป) ==========
GL_MATRIX: Dict[str, Dict[str, str]] = {
    # Marketplace expense
    "marketplace_shopee": {CLIENT_SHD:"520317", CLIENT_RABBIT:"520315", CLIENT_TOPONE:"520314"},
    "marketplace_lazada": {CLIENT_SHD:"520318", CLIENT_RABBIT:"520316", CLIENT_TOPONE:"520315"},
    "marketplace_tiktok": {CLIENT_SHD:"520319", CLIENT_RABBIT:"520317", CLIENT_TOPONE:"520316"},

    # Ads
    "ads_google":         {CLIENT_SHD:"520201", CLIENT_RABBIT:"520201", CLIENT_TOPONE:"520201"},
    "ads_meta":           {CLIENT_SHD:"520202", CLIENT_RABBIT:"520202", CLIENT_TOPONE:"520202"},
    "ads_tiktok":         {CLIENT_SHD:"520223", CLIENT_RABBIT:"520223", CLIENT_TOPONE:"520221"},
    "ads_canva":          {CLIENT_SHD:"520224", CLIENT_RABBIT:"520224", CLIENT_TOPONE:"520224"},  # ✅ ให้ครบ

    # Other
    "online_other":       {CLIENT_SHD:"520203", CLIENT_RABBIT:"520203", CLIENT_TOPONE:"520203"},
}

# Description templates (ตาม structure ที่คุณให้ + seller_id/username/file สำหรับ Shopee marketplace)
DESC_TEMPLATE: Dict[str, str] = {
    "marketplace_shopee": "Record Marketplace Expense - Shopee - Seller ID {seller_id} - {username} - {file}",
    "marketplace_lazada": "Record Marketplace Expense - Lazada - {username} - {period} - {file}",
    "marketplace_tiktok": "Record Marketplace Expense - Tiktok - {username} - {period} - {file}",

    "ads_google": "Record Ads - Google - {brand} - Payment number {payment_number} - Payment method {payment_method}",
    "ads_meta": "Record Ads - Meta - {brand} - {account_id} - Transaction ID {transaction_id} - Payment Method {payment_method}",
    "ads_tiktok": "Record Ads - Tiktok - {brand} - Contract No.{contract_no}",
    "ads_canva": "Record Ads - Canva Ads",

    "online_other": "Record online expense",
}

# Filename helpers
RE_FILE_TRS_CORE = re.compile(r"(TRS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_FILE_RCS_CORE = re.compile(r"(RCS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_FILE_TTSTH_CORE = re.compile(r"(TTSTH\d{10,})", re.IGNORECASE)
RE_FILE_LAZ_CORE = re.compile(r"(THMPTI\d{16}|(?:LAZ|LZD)[A-Z0-9\-_/.]{6,}|INV[A-Z0-9\-_/.]{6,})", re.IGNORECASE)

# ✅ strip noise prefixes incl. "Shopee-TIV-" / "Shopee-TIR-" etc.
RE_LEADING_NOISE_PREFIX = re.compile(
    r"^(?:Shopee-)?TI[VR]-|^Shopee-|^TIV-|^TIR-|^SPX-|^LAZ-|^LZD-|^TikTok-",
    re.IGNORECASE
)


def filename_basename(filename: str) -> str:
    """Return basename only (no directories)."""
    if not filename:
        return ""
    return os.path.basename(str(filename)).strip()


def filename_core(filename: str) -> str:
    """Return filename without extension."""
    base = filename_basename(filename)
    if not base:
        return ""
    return re.sub(r"\.(pdf|png|jpg|jpeg|xlsx)$", "", base, flags=re.IGNORECASE).strip()


def _best_core_from_filename(name_wo_ext: str) -> str:
    """
    Pick the best “document core” from filename:
    - prefer TRS... / RCS... / TTSTH... / LAZ...
    - else strip known leading noise prefixes (Shopee-TIV- etc)
    - else return whole core
    """
    if not name_wo_ext:
        return ""
    s = str(name_wo_ext).strip()

    for pat in (RE_FILE_TRS_CORE, RE_FILE_RCS_CORE, RE_FILE_TTSTH_CORE, RE_FILE_LAZ_CORE):
        m = pat.search(s)
        if m:
            return m.group(1).strip()

    s2 = RE_LEADING_NOISE_PREFIX.sub("", s).strip()
    return s2 if s2 else s


def reference_from_filename(filename: str) -> str:
    """
    ✅ Source-of-truth for C_reference/G_invoice_no based on filename.
    Example:
      Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf -> TRSPEMKP00-00000-251203-0012589
    """
    core = filename_core(filename)
    core = _best_core_from_filename(core)
    return normalize_reference_no_space(core)


def pick_gl_code(rule_key: str, client_tax_id: str) -> str:
    """Get GL code by rule_key + client_tax_id."""
    if not rule_key or not client_tax_id:
        return ""
    return str((GL_MATRIX.get(rule_key) or {}).get(str(client_tax_id).strip(), "") or "")


def build_description(rule_key: str, **kw) -> str:
    """Format description by rule template; missing keys become empty safely."""
    tpl = DESC_TEMPLATE.get(rule_key, "")
    if not tpl:
        return ""
    safe_kw = {k: ("" if v is None else str(v)) for k, v in kw.items()}
    try:
        return tpl.format(**safe_kw).strip()
    except Exception:
        s = tpl
        s = re.sub(r"\{[a-zA-Z0-9_]+\}", "", s)
        s = re.sub(r"\s{2,}", " ", s).strip()
        return s


def infer_rule_key(
    *,
    platform: str = "",
    kind: str = "",
    row: Optional[Dict[str, Any]] = None,
) -> str:
    """Infer rule_key when extractor/AI didn't specify."""
    p = (platform or "").strip().lower()
    k = (kind or "").strip().lower()
    r = row or {}

    if k in ("ads_google", "google_ads", "google"):
        return "ads_google"
    if k in ("ads_meta", "meta_ads", "meta", "facebook"):
        return "ads_meta"
    if k in ("ads_tiktok", "tiktok_ads"):
        return "ads_tiktok"
    if k in ("ads_canva", "canva_ads", "canva"):
        return "ads_canva"
    if k in ("online_other", "other"):
        return "online_other"

    ug = str(r.get("U_group") or "").strip().lower()
    if "marketplace" in ug and "expense" in ug:
        if p == "shopee":
            return "marketplace_shopee"
        if p == "lazada":
            return "marketplace_lazada"
        if p == "tiktok":
            return "marketplace_tiktok"

    if p == "shopee":
        return "marketplace_shopee"
    if p == "lazada":
        return "marketplace_lazada"
    if p == "tiktok":
        return "marketplace_tiktok"

    return ""


def enforce_reference_from_filename(
    row: Dict[str, Any],
    filename: str,
    *,
    force: bool = True,
) -> Dict[str, Any]:
    """
    ✅ Central enforcer: C_reference == G_invoice_no
    - If filename exists and force=True: override both with reference_from_filename(filename)
    - Else: just sync between C/G using existing values
    - Always normalize to NO-SPACE
    """
    if row is None:
        return {}

    ref = reference_from_filename(filename) if filename else ""
    if ref and force:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref
        return row

    c = normalize_reference_no_space(str(row.get("C_reference") or ""))
    g = normalize_reference_no_space(str(row.get("G_invoice_no") or ""))
    v = c or g
    row["C_reference"] = v
    row["G_invoice_no"] = v
    return row


def apply_account_and_description(
    row: Dict[str, Any],
    *,
    client_tax_id: str,
    filename: str,
    rule_key: str = "",
    platform: str = "",
    kind: str = "",
    seller_id: str = "",
    username: str = "",
    period: str = "",
    brand: str = "",
    payment_number: str = "",
    payment_method: str = "",
    account_id: str = "",
    transaction_id: str = "",
    contract_no: str = "",
    set_account_if_empty: bool = False,
) -> Dict[str, Any]:
    """✅ Central enforcer: K_account + L_description"""
    if row is None:
        return {}

    rk = rule_key or infer_rule_key(platform=platform, kind=kind, row=row)
    if not rk:
        return row

    # K_account
    gl = pick_gl_code(rk, client_tax_id or "")
    if gl:
        if set_account_if_empty:
            if not str(row.get("K_account") or "").strip():
                row["K_account"] = gl
        else:
            row["K_account"] = gl

    # L_description
    file_base = filename_basename(filename)
    desc = build_description(
        rk,
        seller_id=seller_id or str(row.get("_seller_id") or row.get("seller_id") or ""),
        username=username or str(row.get("_username") or row.get("username") or ""),
        period=period or str(row.get("_period") or ""),
        brand=brand or str(row.get("_brand") or ""),
        payment_number=payment_number or str(row.get("_payment_number") or ""),
        payment_method=payment_method or str(row.get("_payment_method") or ""),
        account_id=account_id or str(row.get("_account_id") or ""),
        transaction_id=transaction_id or str(row.get("_transaction_id") or ""),
        contract_no=contract_no or str(row.get("_contract_no") or ""),
        file=file_base or "",
    )

    if desc:
        row["L_description"] = desc

    return row


def post_process_peak_row(
    row: Dict[str, Any],
    *,
    filename: str = "",
    client_tax_id: str = "",
    platform: str = "",
    kind: str = "",
    rule_key: str = "",
    seller_id: str = "",
    username: str = "",
    period: str = "",
    brand: str = "",
    payment_number: str = "",
    payment_method: str = "",
    account_id: str = "",
    transaction_id: str = "",
    contract_no: str = "",
    force_filename_reference: bool = True,
) -> Dict[str, Any]:
    """✅ One-shot helper: C/G + K_account + L_description"""
    if row is None:
        row = {}

    enforce_reference_from_filename(row, filename, force=force_filename_reference)

    apply_account_and_description(
        row,
        client_tax_id=client_tax_id or "",
        filename=filename or "",
        rule_key=rule_key or "",
        platform=platform or "",
        kind=kind or "",
        seller_id=seller_id or "",
        username=username or "",
        period=period or "",
        brand=brand or "",
        payment_number=payment_number or "",
        payment_method=payment_method or "",
        account_id=account_id or "",
        transaction_id=transaction_id or "",
        contract_no=contract_no or "",
    )
    return row


# ============================================================
# ✅ FINALIZER (BACKWARD-COMPAT): finalize_row
# ============================================================

def _coerce_bool(v: Any, default: Optional[bool] = None) -> Optional[bool]:
    """Robust bool parser for cfg flags."""
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off"):
            return False
    return default


def _extract_client_tax_id_from_cfg(cfg: Dict[str, Any]) -> str:
    """
    ✅ NEW: get client_tax_id from cfg even when UI sends client_tax_ids (list or csv string)
    Priority:
      1) cfg.client_tax_id / A_company_tax_id / _client_tax_id
      2) cfg.client_tax_ids (list/tuple) -> if len==1 use it, else first as fallback
      3) cfg.client_tax_ids as "a,b,c" -> first
    """
    if not cfg:
        return ""

    direct = str(cfg.get("client_tax_id") or cfg.get("A_company_tax_id") or cfg.get("_client_tax_id") or "").strip()
    if direct:
        return direct

    cands = cfg.get("client_tax_ids") or cfg.get("client_tax_id_list") or cfg.get("client_tax") or cfg.get("client_tax_ids[]")
    if isinstance(cands, (list, tuple)):
        parts = [str(x).strip() for x in cands if str(x).strip()]
        if len(parts) == 1:
            return parts[0]
        if len(parts) > 1:
            return parts[0]  # fallback (ถ้าจะทำ per-file mapping ค่อยเพิ่มทีหลัง)
        return ""
    if isinstance(cands, str):
        parts = [p.strip() for p in cands.split(",") if p.strip()]
        return parts[0] if parts else ""

    return ""


def finalize_row(
    row: Dict[str, Any],
    *,
    filename: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    platform: str = "",
    kind: str = "",
    rule_key: str = "",
    force_filename_reference: bool = True,
) -> Dict[str, Any]:
    """
    ✅ สำคัญ: ต้องมีเพื่อกัน ImportError และเป็นจุด “บังคับมาตรฐานกลาง”

    - บังคับ C_reference และ G_invoice_no ให้ตรงกัน + no-space
    - ถ้ามี filename -> ใช้เลขจาก filename เป็น source-of-truth (ตัด Shopee-TIV- ฯลฯ)
    - เติม K_account + L_description ตาม mapping/template
    - กันคอลัมน์ A_company_name / O_vat_rate / P_wht ไม่หาย (ทำให้มี key เสมอ)
    - ✅ NEW: รับ client_tax_ids ได้ + compute_wht บังคับ P_wht/S_pnd
    """
    if row is None:
        row = {}
    cfg = cfg or {}

    # ensure key existence for XLSX exporter (บางตัว export ตาม keys/columns)
    if "A_company_name" not in row:
        row["A_company_name"] = str(cfg.get("company_name") or cfg.get("A_company_name") or row.get("A_company_name") or "")
    if "O_vat_rate" not in row:
        row["O_vat_rate"] = str(cfg.get("vat_rate") or cfg.get("O_vat_rate") or row.get("O_vat_rate") or "7%")
    if "P_wht" not in row:
        row["P_wht"] = str(cfg.get("wht_rate") or cfg.get("P_wht") or row.get("P_wht") or "0")
    if "S_pnd" not in row:
        row["S_pnd"] = str(cfg.get("S_pnd") or row.get("S_pnd") or "")

    # ✅ NEW: robust client_tax_id from cfg (supports client_tax_ids)
    client_tax_id = _extract_client_tax_id_from_cfg(cfg)

    # ✅ NEW: compute_wht flag (default True to keep legacy behavior)
    compute_wht = _coerce_bool(cfg.get("compute_wht"), default=True)

    if not platform:
        platform = str(cfg.get("platform") or cfg.get("_platform") or row.get("_platform") or "").strip()
    if not kind:
        kind = str(cfg.get("kind") or cfg.get("_kind") or row.get("_kind") or "").strip()
    if not rule_key:
        rule_key = str(cfg.get("rule_key") or cfg.get("_rule_key") or row.get("_rule_key") or "").strip()

    # meta (seller_id/username/etc) — รองรับหลายชื่อ key กันหลุด
    seller_id = str(
        cfg.get("seller_id")
        or row.get("_seller_id")
        or row.get("seller_id")
        or row.get("shop_id")
        or ""
    ).strip()

    username = str(
        cfg.get("username")
        or row.get("_username")
        or row.get("username")
        or row.get("user_name")
        or ""
    ).strip()

    period = str(cfg.get("period") or row.get("_period") or "").strip()
    brand = str(cfg.get("brand") or row.get("_brand") or "").strip()
    payment_number = str(cfg.get("payment_number") or row.get("_payment_number") or "").strip()
    payment_method = str(cfg.get("payment_method") or row.get("_payment_method") or "").strip()
    account_id = str(cfg.get("account_id") or row.get("_account_id") or "").strip()
    transaction_id = str(cfg.get("transaction_id") or row.get("_transaction_id") or "").strip()
    contract_no = str(cfg.get("contract_no") or row.get("_contract_no") or "").strip()

    post_process_peak_row(
        row,
        filename=filename or "",
        client_tax_id=client_tax_id,
        platform=platform,
        kind=kind,
        rule_key=rule_key,
        seller_id=seller_id,
        username=username,
        period=period,
        brand=brand,
        payment_number=payment_number,
        payment_method=payment_method,
        account_id=account_id,
        transaction_id=transaction_id,
        contract_no=contract_no,
        force_filename_reference=force_filename_reference,
    )

    # hard sync + normalize C/G
    c = normalize_reference_no_space(str(row.get("C_reference") or ""))
    g = normalize_reference_no_space(str(row.get("G_invoice_no") or ""))
    v = c or g
    row["C_reference"] = v
    row["G_invoice_no"] = v

    # ✅ NEW: enforce WHT behavior by compute_wht
    # - ❌ compute_wht=False -> ไม่คำนวณภาษี -> rate=0 และไม่ใส่ ภ.ง.ด.
    if compute_wht is False:
        row["P_wht"] = "0"
        row["S_pnd"] = ""

    # ensure keys again (กันหลุดจาก upstream)
    if "A_company_name" not in row:
        row["A_company_name"] = ""
    if "O_vat_rate" not in row:
        row["O_vat_rate"] = "7%"
    if "P_wht" not in row:
        row["P_wht"] = "0"
    if "S_pnd" not in row:
        row["S_pnd"] = ""

    return row


# ============================================================
# Validation and formatting
# ============================================================

def validate_tax_id(tax_id: str) -> bool:
    return bool(tax_id) and len(tax_id) == 13 and tax_id.isdigit()


def validate_date(date_str: str) -> bool:
    if not date_str or len(date_str) != 8:
        return False
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except Exception:
        return False


def format_peak_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Final formatting and validation for PEAK import

    ✅ IMPORTANT:
    - P_wht is RATE-ONLY: "3%" or "0"
    """
    formatted = base_row_dict()
    formatted.update(row)

    # Normalize numeric fields (always 2 decimals)
    for key in ["N_unit_price", "R_paid_amount"]:
        val = formatted.get(key, "")
        if val in (None, "", "0", 0):
            formatted[key] = "0"
            continue
        try:
            d = safe_decimal(val)
            if d <= 0:
                formatted[key] = "0"
            else:
                formatted[key] = f"{d:.2f}"
        except Exception:
            formatted[key] = "0"

    # ✅ P_wht rate-only enforcement
    w = str(formatted.get("P_wht", "") or "").strip()
    if not w or w in ("0", "0.0", "0.00"):
        formatted["P_wht"] = "0"
    else:
        if w.endswith("%"):
            w_num = re.sub(r"[^\d\.]", "", w)
            formatted["P_wht"] = f"{w_num}%" if w_num else "0"
        else:
            w_num = re.sub(r"[^\d\.]", "", w)
            if not w_num:
                formatted["P_wht"] = "0"
            else:
                try:
                    dv = safe_decimal(w_num)
                    if dv == 0:
                        formatted["P_wht"] = "0"
                    elif dv < 1:
                        formatted["P_wht"] = f"{(dv * Decimal('100')):.0f}%"
                    else:
                        formatted["P_wht"] = f"{dv:.0f}%"
                except Exception:
                    formatted["P_wht"] = "0"

    # PND: set only if P_wht != 0
    if formatted["P_wht"] != "0" and not formatted.get("S_pnd"):
        formatted["S_pnd"] = "53"
    if formatted["P_wht"] == "0":
        formatted["S_pnd"] = formatted.get("S_pnd") or ""

    # Validate dates
    for k in ["B_doc_date", "H_invoice_date", "I_tax_purchase_date"]:
        if formatted.get(k) and not validate_date(formatted[k]):
            formatted[k] = ""

    # Sync C/G (and enforce no-space)
    if not formatted.get("C_reference") and formatted.get("G_invoice_no"):
        formatted["C_reference"] = formatted["G_invoice_no"]
    if not formatted.get("G_invoice_no") and formatted.get("C_reference"):
        formatted["G_invoice_no"] = formatted["C_reference"]

    formatted["C_reference"] = normalize_reference_no_space(formatted.get("C_reference", ""))
    formatted["G_invoice_no"] = normalize_reference_no_space(formatted.get("G_invoice_no", ""))

    # Branch safety
    formatted["F_branch_5"] = fmt_branch_5(formatted.get("F_branch_5", "00000"))

    # Tax safety
    if formatted.get("E_tax_id_13") and not validate_tax_id(formatted["E_tax_id_13"]):
        formatted["E_tax_id_13"] = ""

    # ensure XLSX-visible keys exist (สุดท้ายกันตก)
    if "A_company_name" not in formatted:
        formatted["A_company_name"] = ""
    if "O_vat_rate" not in formatted:
        formatted["O_vat_rate"] = "7%"
    if "P_wht" not in formatted:
        formatted["P_wht"] = "0"
    if "S_pnd" not in formatted:
        formatted["S_pnd"] = ""

    return formatted


# ============================================================
# Backward Compatibility Functions (for generic.py)
# ============================================================

def find_tax_id(text: str) -> str:
    t = normalize_text(text)
    m = RE_TAX13_STRICT.search(t)
    return m.group(1) if m else ""


def find_first_date(text: str) -> str:
    return find_best_date(text)


def find_total_amount(text: str) -> str:
    t = normalize_text(text)

    m = RE_TOTAL_INC_VAT.search(t)
    if m:
        x = parse_money(m.group(1))
        if x and x != "0.00":
            return x

    m = RE_TOTAL_EX_VAT.search(t)
    if m:
        x = parse_money(m.group(1))
        if x and x != "0.00":
            return x

    patterns = [
        r"(?:Total|รวม|Grand\s*Total|Amount\s*Due|จำนวนเงิน)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
        r"฿\s*([0-9,]+\.[0-9]{2})",
    ]
    for pat in patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            x = parse_money(m.group(1))
            if x and x != "0.00":
                return x

    return ""


# ============================================================
# Export all functions
# ============================================================

__all__ = [
    # Normalization
    "normalize_text",
    "normalize_one_line",
    "squash_all_ws",
    "normalize_reference_no_space",
    "fmt_tax_13",
    "fmt_branch_5",
    "parse_date_to_yyyymmdd",
    "parse_en_date",
    "parse_money",
    "safe_decimal",

    # Row template
    "base_row_dict",

    # Detection
    "detect_platform_vendor",

    # Extraction
    "find_vendor_tax_id",
    "find_branch",
    "find_invoice_no",
    "find_best_date",
    "extract_seller_info",
    "extract_amounts",
    "find_payment_method",

    # ✅ Post-process Central Enforcer
    "filename_basename",
    "filename_core",
    "reference_from_filename",
    "pick_gl_code",
    "build_description",
    "infer_rule_key",
    "enforce_reference_from_filename",
    "apply_account_and_description",
    "post_process_peak_row",

    # ✅ FINALIZER (กัน ImportError)
    "finalize_row",

    # Validation
    "validate_tax_id",
    "validate_date",
    "format_peak_row",

    # Backward compatibility
    "find_tax_id",
    "find_first_date",
    "find_total_amount",
]
