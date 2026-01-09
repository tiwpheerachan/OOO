# backend/app/extractors/common.py
"""
Common utilities and patterns for invoice extraction
Enhanced with AI-ready patterns for better accuracy
Version 3.2.1 - Line-aware + Full Reference Number Support + No-space Reference + WHT-safe

⭐ Key upgrades (สำคัญมาก)
1) normalize_text() "รักษา newline" เพื่อให้ regex แบบ MULTILINE (^ / $) ใช้งานได้จริง
2) find_best_date() ฉลาดขึ้น: ให้คะแนนวันที่ที่อยู่ใกล้คำว่า Invoice/Tax Invoice/Receipt/วันที่/Issue date
3) extract_amounts() ฉลาดขึ้น: เก็บหลาย candidate แล้วเลือกอันที่ “เหมาะสมสุด”
   + ✅ anti-WHT pollution: ไม่เลือกยอดรวมที่อยู่ใกล้คำว่า WHT/หักภาษี
4) find_vendor_tax_id() กันหลุด: เลือกเลขที่อยู่ใกล้ชื่อ platform + กัน client tax id ของ Rabbit/SHD/TopOne
5) find_invoice_no() แข็งแรงขึ้น:
   - จับ “เอกสาร + reference” ได้ดีขึ้นแม้ OCR แยกข้ามบรรทัด
   - ✅ รองรับ "MMDD - NNNNNNN" (space around dash)
   - ✅ มี helper บังคับ "no-space reference" ให้ทุก extractor ใช้ได้
6) format_peak_row():
   - ✅ P_wht เป็น "rate-only" (เช่น "3%") ตาม requirement ล่าสุด
   - ❌ ไม่คำนวณจำนวนเงินจาก rate ไปทับ P_wht อีกต่อไป
"""

from __future__ import annotations

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
    """
    Single-line normalization (for patterns that don't need line anchors).
    """
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
# Invoice/Document number patterns (ENHANCED - จับ reference เต็ม)
# ============================================================

# Full reference pattern with document number + reference code
# ✅ allow whitespace/newline between
# ✅ allow spaces around dash in reference
# Examples:
#   TRSPEMKP00-00000-25 1203-0012589
#   RCSPXSPB00-00000-25 1205-0012345
#   RCSPXSPB00-00000-25 1205 - 0012345
RE_INVOICE_WITH_REF = re.compile(
    r"\b([A-Z]{2,}[A-Z0-9\-/_.]{6,})\s+(\d{4})\s*-\s*(\d{6,9})\b",
    re.IGNORECASE
)

# More tolerant long ref patterns like "25/0012345678" or "2512-0001593"
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
    r"|Grand\s*Total|Amount\s*Due|Total\s*Due|ยอด(?:ที่)?ชำระ|ยอดรวมทั้งสิ้น)"
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
RE_VENDOR_SPX = re.compile(r"(?:SPX\s*Express|Standard\s*Express)", re.IGNORECASE)

# Known client tax IDs (your companies)
CLIENT_TAX_IDS = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# Common keywords for better date selection
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
        "O_vat_rate": "7%",
        "P_wht": "0",          # ✅ rate-only string "3%" or "0"
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

    Strategy:
    1) Vendor-specific patterns when available (more reliable).
    2) If platform doc has multiple tax ids, prefer the one closest to vendor name keywords.
    3) Never return your known client tax ids (Rabbit/SHD/TopOne).
    4) Fallback to first non-client tax id.
    """
    t = normalize_text(text)

    # ----------------------------
    # Vendor-specific patterns
    # ----------------------------
    if vendor_code == "SPX":
        m = re.search(r"Tax\s*ID\s*No\.?\s*([0-9]{13})", t, re.IGNORECASE)
        if m and m.group(1) not in CLIENT_TAX_IDS:
            return m.group(1)

    if vendor_code == "TikTok":
        m = re.search(r"Tax\s*Registration\s*Number\s*[:#：]?\s*([0-9]{13})", t, re.IGNORECASE)
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

    # ----------------------------
    # Proximity scoring fallback
    # ----------------------------
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
            return tax  # first non-client
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

def _find_reference_code_near(text: str, doc_number: str, max_distance: int = 120) -> str:
    """
    Find reference code (MMDD-NNNNNNN) near a document number.
    - allow spaces around dash
    Returns "MMDD-NNNNNNN" (normalized)
    """
    if not doc_number:
        return ""

    pos = text.find(doc_number)
    if pos == -1:
        # try squashed search
        t_sq = squash_all_ws(text)
        d_sq = squash_all_ws(doc_number)
        p2 = t_sq.find(d_sq)
        if p2 == -1:
            return ""
        start = max(0, p2 - max_distance * 2)
        end = min(len(t_sq), p2 + len(d_sq) + max_distance * 2)
        nearby = t_sq[start:end]
        m = RE_REFERENCE_CODE.search(nearby)
        if not m:
            return ""
        return f"{m.group(1)}-{m.group(2)}"

    start = max(0, pos - max_distance)
    end = min(len(text), pos + len(doc_number) + max_distance)
    nearby = text[start:end]

    m = RE_REFERENCE_CODE.search(nearby)
    if not m:
        return ""
    return f"{m.group(1)}-{m.group(2)}"

def _clean_doc_number(s: str) -> str:
    if not s:
        return ""
    x = str(s).strip().strip('"').strip("'")
    x = re.sub(r"[,\.;:]+$", "", x)
    return x

def find_invoice_no(text: str, platform: str = "") -> str:
    """
    Extract invoice/document number with full reference.
    ENHANCED: captures "DOC ... REF" and tries to attach ref code if near.
    ✅ Returns STRICT NO-SPACE reference by default.

    Returns:
      - "DOCREF"  (no spaces) if ref exists
      - else "DOC" (no spaces)
    """
    t = normalize_text(text)
    t_sq = squash_all_ws(t)

    def pack(doc: str, ref: str = "") -> str:
        if not doc:
            return ""
        s = f"{doc}{ref}" if ref else doc
        return normalize_reference_no_space(s)

    # 1) Platform-specific WITH reference first (highest precision)
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
            # NOTE: RE has only (doc)(mmdd)(seq) form in this file
            # Here pattern already returns exact groups? For safety:
            # If your upstream regex returns "doc + (mmdd-seq)" as group2, it still works via pack(doc, group2)
            try:
                return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")  # if 3 groups
            except Exception:
                return pack(m.group(1), m.group(2))
        m = RE_SHOPEE_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    if platform == "Lazada":
        m = RE_LAZADA_DOC_WITH_REF.search(t)
        if m:
            try:
                return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
            except Exception:
                return pack(m.group(1), m.group(2))
        m = RE_LAZADA_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    if platform == "TikTok":
        m = RE_TIKTOK_DOC_WITH_REF.search(t)
        if m:
            try:
                return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
            except Exception:
                return pack(m.group(1), m.group(2))
        m = RE_TIKTOK_DOC.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    # 2) Generic full reference patterns (DOC + mmdd-seq)
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

    # 3) Try any platform doc WITH ref (generic try)
    for pat in (RE_SPX_DOC_WITH_REF, RE_SHOPEE_DOC_WITH_REF, RE_LAZADA_DOC_WITH_REF, RE_TIKTOK_DOC_WITH_REF):
        m = pat.search(t)
        if m:
            # attempt to join groups robustly
            if m.lastindex and m.lastindex >= 3:
                return pack(m.group(1), f"{m.group(2)}-{m.group(3)}")
            if m.lastindex and m.lastindex >= 2:
                return pack(m.group(1), m.group(2))
            return pack(m.group(1))

    # 4) Try platform patterns (without ref)
    for pat in (RE_SPX_DOC, RE_SHOPEE_DOC, RE_LAZADA_DOC, RE_TIKTOK_DOC):
        m = pat.search(t)
        if m:
            doc = m.group(1)
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    # 5) Generic invoice field (fallback)
    m = RE_INVOICE_GENERIC.search(t)
    if m:
        doc = _clean_doc_number(m.group(1))
        if len(doc) >= 6:
            ref = _find_reference_code_near(t, doc)
            return pack(doc, ref) if ref else pack(doc)

    # 6) LAST: attempt to recover from squashed text
    m_doc = re.search(r"(RCS[A-Z0-9\-/]{8,})", t_sq, flags=re.IGNORECASE)
    m_ref = RE_REFERENCE_CODE.search(t_sq)
    if m_doc and m_ref:
        return pack(m_doc.group(1), f"{m_ref.group(1)}-{m_ref.group(2)}")
    if m_doc:
        return pack(m_doc.group(1))

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

    New scoring:
    - Prefer date near anchor keywords (invoice date / วันที่ / issue date)
    - If multiple, pick the best-scored; tie-breaker: latest date
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
        # anti-WHT vicinity guard
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
    """
    Extract financial amounts from document (subtotal/vat/total/wht).

    New logic:
    - Collect multiple candidates for total/subtotal/vat then choose the best by proximity.
    - ✅ anti-WHT pollution: do not choose totals near WHT keywords
    - ✅ never let WHT amount become total (common OCR bug)
    """
    t = normalize_text(text)
    amounts = {
        "subtotal": "",
        "vat": "",
        "total": "",
        "wht_rate": "",
        "wht_amount": "",
    }

    # Collect candidates
    total_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_TOTAL_INC_VAT.finditer(t)]
    sub_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_TOTAL_EX_VAT.finditer(t)]
    vat_matches: List[Tuple[int, str]] = [(m.start(), m.group(1)) for m in RE_VAT_AMOUNT.finditer(t)]

    amounts["total"] = _best_amount_candidate(
        total_matches,
        anchors=["total", "grand total", "amount due", "ยอดชำระ", "ยอดรวม", "including"],
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

    # WHT (collect best match)
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
        # anchor: WHT keywords
        ctx = t[max(0, pos - 100): min(len(t), pos + 160)]
        if not RE_WHT_HINT.search(ctx):
            # if regex hit but context has no WHT hint, ignore (false positive)
            continue

        # distance to nearest hint occurrence inside ctx (smaller is better)
        # safe scoring
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

        key = (d, safe_decimal(amt))
        if wht_best is None or key < (wht_best[0], safe_decimal(wht_best[1])):
            wht_best = (d, amt, rate)

    if wht_best:
        amounts["wht_amount"] = wht_best[1]
        amounts["wht_rate"] = wht_best[2] or ""

    # ✅ never allow total == wht_amount
    if amounts["total"] and amounts["wht_amount"] and amounts["total"] == amounts["wht_amount"]:
        amounts["total"] = ""

    # Calculate missing values
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

        # If only subtotal found, infer VAT and total (7%)
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
# Validation and formatting
# ============================================================

def validate_tax_id(tax_id: str) -> bool:
    """Validate 13-digit tax ID"""
    return bool(tax_id) and len(tax_id) == 13 and tax_id.isdigit()

def validate_date(date_str: str) -> bool:
    """Validate YYYYMMDD format"""
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

    ✅ IMPORTANT CHANGE (per your latest requirement):
    - P_wht is RATE-ONLY: "3%" or "0"
    - Do NOT compute withholding amount here (extractor notes may store amount elsewhere if needed)
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
        # Accept "3%" style; accept "3" -> "3%"
        if w.endswith("%"):
            # normalize
            w_num = re.sub(r"[^\d\.]", "", w)
            formatted["P_wht"] = f"{w_num}%" if w_num else "0"
        else:
            # numeric -> treat as rate and convert to percent if plausible (e.g. "3" => "3%")
            w_num = re.sub(r"[^\d\.]", "", w)
            if not w_num:
                formatted["P_wht"] = "0"
            else:
                # if someone sent "0.03" accidentally, convert to 3%
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

    return formatted

# ============================================================
# Backward Compatibility Functions (for generic.py)
# ============================================================

def find_tax_id(text: str) -> str:
    """Backward compatibility wrapper for generic.py"""
    t = normalize_text(text)
    m = RE_TAX13_STRICT.search(t)
    return m.group(1) if m else ""

def find_first_date(text: str) -> str:
    """Backward compatibility wrapper"""
    return find_best_date(text)

def find_total_amount(text: str) -> str:
    """Extract total amount from text (legacy helper)"""
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

    # Validation
    "validate_tax_id",
    "validate_date",
    "format_peak_row",

    # Backward compatibility
    "find_tax_id",
    "find_first_date",
    "find_total_amount",
]
