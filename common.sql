from __future__ import annotations

import re
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

from ..utils.text_utils import (
    normalize_text,
    fmt_tax_13,
    fmt_branch_5,
    parse_date_to_yyyymmdd,
    parse_money,
)

# ============================================================
# Core patterns (generic)
# ============================================================

RE_TAX13 = re.compile(r"(\d[\d\s-]{11,20}\d)")

# NOTE: RE_BRANCH นี้รองรับทั้ง "สำนักงานใหญ่" และ "สาขาที่ xxx" / "Branch: xxx"
RE_BRANCH = re.compile(
    r"(?:สำนักงานใหญ่|Head\s*Office)|(?:สาขา(?:ที่)?\s*|Branch\s*:?\s*)(\d{1,5})",
    re.IGNORECASE
)

# Date variants:
#  - 15/12/2025, 15-12-25, 2025-12-15, 20251215
RE_DATE = re.compile(
    r"(\d{1,2}[\-/\. ]\d{1,2}[\-/\. ]\d{2,4}"
    r"|\d{4}[\-/\. ]\d{1,2}[\-/\. ]\d{1,2}"
    r"|\d{8})"
)

RE_INVOICE_GENERIC = re.compile(
    r"(?:ใบกำกับ(?:ภาษี)?|Tax\s*Invoice|Invoice|เลขที่เอกสาร|Document\s*No\.?|Document\s*Number|Doc\s*No\.?)"
    r"\s*[:#]?\s*\"?\s*([A-Za-z0-9\-/_.]+)",
    re.IGNORECASE,
)

# Shopee doc no patterns (พบจริงบ่อย)
RE_SHOPEE_DOCNO = re.compile(
    r"\b("
    r"(?:Shopee-)?TIV-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}"
    r"|TIV-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}"
    r"|TRS[A-Z0-9\-_/]{8,}"
    r")\b",
    re.IGNORECASE
)

# Lazada / Marketplace doc no patterns (เพิ่มให้ชาญฉลาดขึ้น)
RE_LAZADA_DOCNO = re.compile(
    r"\b("
    r"(?:LAZ|LZD|Lazada)[A-Z0-9\-_/.]{6,}"
    r"|INV[A-Z0-9\-_/.]{6,}"
    r"|BILL[A-Z0-9\-_/.]{6,}"
    r")\b",
    re.IGNORECASE
)

# TikTok fallback doc no like TTSTH2025...
RE_TIKTOK_DOCNO_FALLBACK = re.compile(r"\b(TTSTH[0-9]{6,})\b", re.IGNORECASE)

# Totals / VAT / WHT (generic)
RE_TOTAL_DUE = re.compile(
    r"(?:ยอดชำระ|ยอดที่ต้องชำระ|Total\s*Due|Amount\s*Due|Grand\s*Total|Total\s*amount|Total\s*Amount|Total)\s*[: ]*"
    r"([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)
RE_SUBTOTAL = re.compile(
    r"(?:รวมก่อนภาษี|Subtotal|Total\s*before\s*VAT|Total\s*excluding\s*VAT)\s*[: ]*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)
RE_VAT_AMT = re.compile(
    r"(?:Total\s*VAT|VAT|ภาษีมูลค่าเพิ่ม)\s*(?:7%|7\s*%)?\s*[: ]*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)
RE_WHT_AMT = re.compile(
    r"(?:ภาษีหัก\s*ณ\s*ที่จ่าย|Withholding\s*Tax|WHT|withheld\s*tax)\s*(?:3%|3\s*%)?\s*[: ]*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE
)
RE_WHT_RATE = re.compile(
    r"(?:ภาษีหัก\s*ณ\s*ที่จ่าย|Withholding\s*Tax|WHT|withheld\s*tax)\s*[: ]*(\d{1,2}\s*%)",
    re.IGNORECASE
)

# Payment method (โค้ด/คีย์เวิร์ดที่พบจริงในเอกสาร/สรุปค่าใช้จ่าย)
RE_PAYMENT_METHOD = re.compile(
    r"\b("
    r"EWL\d{2,6}"             # EWL008, EWL0012 ...
    r"|TRF\d{2,6}"            # TRFxxx
    r"|CSH\d{2,6}"            # CSHxxx
    r"|QR"
    r"|CASH"
    r"|CARD"
    r"|TRANSFER"
    r"|BANK\s*TRANSFER"
    r"|CREDIT\s*CARD"
    r")\b",
    re.IGNORECASE
)

# Vendor detection (platform)
RE_VENDOR_SHOPEE = re.compile(r"\bShopee\b", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"\bLazada\b", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"\bTikTok\b", re.IGNORECASE)

# ============================================================
# TikTok-specific (more precise extraction)
# ============================================================

RE_TIKTOK_TAX_REG = re.compile(r"Tax\s*Registration\s*Number\s*:?[\s]*([0-9]{13})", re.IGNORECASE)
RE_TIKTOK_CLIENT_TAX = re.compile(r"\bTax\s*ID\s*:?[\s]*([0-9]{13})\b", re.IGNORECASE)

RE_TIKTOK_INVOICE_NO = re.compile(r"Invoice\s*number\s*:?[\s]*([A-Z0-9\-]+)", re.IGNORECASE)
RE_TIKTOK_INVOICE_DATE = re.compile(
    r"Invoice\s*date\s*:?[\s]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE
)

RE_TIKTOK_SUBTOTAL_EX = re.compile(r"Subtotal\s*\(excluding\s*VAT\)\s*฿?\s*([0-9,]+\.[0-9]{2})", re.IGNORECASE)
RE_TIKTOK_VAT7 = re.compile(r"Total\s*VAT\s*7%\s*฿?\s*([0-9,]+\.[0-9]{2})", re.IGNORECASE)
RE_TIKTOK_TOTAL_INC = re.compile(r"Total\s*amount.*including\s*VAT.*฿?\s*([0-9,]+\.[0-9]{2})", re.IGNORECASE)

RE_TIKTOK_WHT3 = re.compile(
    r"withheld\s*tax.*rate\s*of\s*3%.*amounting\s*to\s*฿?\s*([0-9,]+\.[0-9]{2})",
    re.IGNORECASE
)

# fee lines: "- Commerce Growth Fee  exVAT VAT incVAT"
RE_TIKTOK_FEE_LINE = re.compile(
    r"^\s*-\s*(.+?)\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s*$",
    re.IGNORECASE | re.MULTILINE
)

# ============================================================
# Shopee / Marketplace helpers (date inside doc no)
# ============================================================

# Shopee doc no มักมี -YYMMDD- เช่น ...-251201-...
RE_DOCNO_YYMMDD = re.compile(r"(?:^|[-_/\.])(\d{6})(?:$|[-_/\.])")

def infer_doc_date_from_docno(docno: str) -> str:
    """
    พยายามดึงวันที่จากเลขเอกสารรูปแบบ YYMMDD → YYYYMMDD
    เช่น ...-251201-... => 20251201
    """
    if not docno:
        return ""
    m = RE_DOCNO_YYMMDD.search(docno)
    if not m:
        return ""
    yymmdd = m.group(1)
    try:
        yy = int(yymmdd[:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            return ""
        yyyy = 2000 + yy  # สมมติ 20xx (สำหรับเอกสารยุคนี้)
        return f"{yyyy:04d}{mm:02d}{dd:02d}"
    except Exception:
        return ""

# ============================================================
# Row template
# ============================================================

def base_row_dict() -> Dict[str, Any]:
    """
    โครงเดียวกับไฟล์เดิมของโปรเจกต์คุณ เพื่อไม่ให้ส่วนอื่นพัง
    """
    return dict(
        A_seq="1",
        B_doc_date="",
        C_reference="",
        D_vendor_code="",
        E_tax_id_13="",
        F_branch_5="",
        G_invoice_no="",
        H_invoice_date="",
        I_tax_purchase_date="",
        J_price_type="1",          # 1=แยก VAT
        K_account="",
        L_description="",
        M_qty="1",
        N_unit_price="0",
        O_vat_rate="7%",
        P_wht="0",
        Q_payment_method="",
        R_paid_amount="0",
        S_pnd="",
        T_note="",
        U_group="",
    )

# ============================================================
# Generic helpers
# ============================================================

def _parse_en_date_to_yyyymmdd(s: str) -> str:
    """
    Parse 'Dec 9, 2025' -> '20251209'
    """
    s = (s or "").strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d.strftime("%Y%m%d")
        except Exception:
            continue
    return ""

def find_tax_id(text: str) -> str:
    """
    ดึงเลขภาษี 13 หลักแบบ generic
    """
    t = normalize_text(text)
    m = RE_TAX13.search(t)
    if not m:
        return ""
    return fmt_tax_13(m.group(1))

def find_branch(text: str) -> str:
    """
    ดึงเลขสาขา 5 หลัก
    - ถ้าเจอ "สำนักงานใหญ่/Head Office" => 00000
    """
    t = normalize_text(text)
    if "สำนักงานใหญ่" in t or "Head Office" in t or "HeadOffice" in t:
        return "00000"

    m = RE_BRANCH.search(t)
    if not m:
        return ""

    g1 = m.group(1) if m.lastindex else None
    if not g1:
        return "00000"
    return fmt_branch_5(g1)

def find_best_date(text: str) -> str:
    """
    เลือกวันที่ที่น่าเชื่อถือจากข้อความ:
    - parse ได้จริงเป็น YYYYMMDD
    - prefer ปีขึ้นต้น 20xx
    """
    t = normalize_text(text)
    candidates = RE_DATE.findall(t) or []
    parsed = []
    for c in candidates:
        p = parse_date_to_yyyymmdd(c)
        if p:
            parsed.append(p)

    for p in parsed:
        if p.startswith("20"):
            return p
    return parsed[0] if parsed else ""

def find_invoice_no(text: str) -> str:
    """
    ดึงเลขเอกสาร/ใบกำกับ
    - Prefer Shopee docno
    - Then Lazada docno
    - Then TikTok docno fallback
    - Then generic invoice keyword
    """
    t = normalize_text(text)

    m2 = RE_SHOPEE_DOCNO.search(t)
    if m2:
        return m2.group(1)

    m3 = RE_LAZADA_DOCNO.search(t)
    if m3:
        return m3.group(1)

    m4 = RE_TIKTOK_DOCNO_FALLBACK.search(t)
    if m4:
        return m4.group(1)

    m = RE_INVOICE_GENERIC.search(t)
    if m:
        return m.group(1).strip()

    return ""

def find_best_reference(text: str) -> str:
    """
    พยายามหา "reference" ที่ดีที่สุด:
    - Shopee TIV/TRS
    - Lazada docno
    - TikTok TTSTH...
    - fallback invoice generic
    """
    return find_invoice_no(text)

def detect_platform_vendor(text: str) -> Tuple[str, str]:
    """
    คืน (vendor_display_name, platform_key)
    """
    t = normalize_text(text)
    if RE_VENDOR_SHOPEE.search(t):
        return ("Shopee (Thailand) Co., Ltd.", "shopee")
    if RE_VENDOR_LAZADA.search(t):
        return ("Lazada (Thailand) Co., Ltd.", "lazada")
    if RE_VENDOR_TIKTOK.search(t):
        return ("TikTok Shop (Thailand) Ltd.", "tiktok")
    return ("", "")

def find_payment_method(text: str) -> str:
    """
    ดึงรหัส/คำที่บ่งบอกช่องทางชำระ
    เช่น EWL008, TRF001, CASH, CARD, QR, TRANSFER
    """
    t = normalize_text(text)
    m = RE_PAYMENT_METHOD.search(t)
    if not m:
        return ""
    return (m.group(1) or "").upper().replace(" ", "")

def find_totals(text: str) -> Dict[str, str]:
    """
    คืน dict:
      - total_due (ยอดชำระ)
      - subtotal  (ก่อน VAT)
      - vat_amt
      - wht_amt
      - wht_rate (เช่น 3%)
    """
    t = normalize_text(text)

    def pick(rex):
        m = rex.search(t)
        return parse_money(m.group(1)) if m else ""

    out = {
        "total_due": pick(RE_TOTAL_DUE),
        "subtotal": pick(RE_SUBTOTAL),
        "vat_amt": pick(RE_VAT_AMT),
        "wht_amt": pick(RE_WHT_AMT),
        "wht_rate": "",
    }
    mr = RE_WHT_RATE.search(t)
    if mr:
        out["wht_rate"] = mr.group(1).replace(" ", "")
    return out

def find_best_doc_date(text: str) -> str:
    """
    เวอร์ชันฉลาดขึ้น:
    1) ถ้ามีเลขเอกสารที่ฝังวันที่ YYMMDD -> ใช้อันนั้นก่อน (Shopee/Lazada บางแบบ)
    2) ไม่งั้น fallback find_best_date
    """
    t = normalize_text(text)
    docno = find_invoice_no(t)
    dt = infer_doc_date_from_docno(docno)
    if dt:
        return dt
    return find_best_date(t)

# ============================================================
# Backward-compat (ชื่อเดิมที่ไฟล์อื่นเรียก)
# ============================================================

def find_first_date(text: str) -> str:
    """
    เดิมไฟล์อื่นเรียก find_first_date -> ยังใช้งานได้
    """
    return find_best_doc_date(text)

def find_total_amount(text: str) -> str:
    """
    เดิม generic extractor มักต้องการ total_due อย่างเดียว
    """
    t = find_totals(text)
    return t.get("total_due") or t.get("subtotal") or ""

# ============================================================
# TikTok precise helpers (ใช้ใน tiktok extractor)
# ============================================================

def tiktok_tax_reg_number(text: str) -> str:
    t = normalize_text(text)
    m = RE_TIKTOK_TAX_REG.search(t)
    if m:
        return m.group(1)
    m2 = RE_TIKTOK_CLIENT_TAX.search(t)
    return m2.group(1) if m2 else ""

def tiktok_invoice_number(text: str) -> str:
    t = normalize_text(text)
    m = RE_TIKTOK_INVOICE_NO.search(t)
    if m:
        return m.group(1).strip()
    m2 = RE_TIKTOK_DOCNO_FALLBACK.search(t)
    return m2.group(1) if m2 else ""

def tiktok_invoice_date_yyyymmdd(text: str) -> str:
    t = normalize_text(text)
    m = RE_TIKTOK_INVOICE_DATE.search(t)
    if not m:
        return ""
    return _parse_en_date_to_yyyymmdd(m.group(1))

def tiktok_totals(text: str) -> Dict[str, str]:
    """
    คืนค่าแบบ TikTok:
      - subtotal_ex (excluding VAT)
      - vat_7
      - total_inc (including VAT)
      - wht_3
    """
    t = normalize_text(text)

    def pick(rex):
        m = rex.search(t)
        return parse_money(m.group(1)) if m else ""

    return {
        "subtotal_ex": pick(RE_TIKTOK_SUBTOTAL_EX),
        "vat_7": pick(RE_TIKTOK_VAT7),
        "total_inc": pick(RE_TIKTOK_TOTAL_INC),
        "wht_3": pick(RE_TIKTOK_WHT3),
    }

def tiktok_fee_summary(text: str, max_lines: int = 8) -> Tuple[str, str]:
    """
    คืน (short_summary, long_note)
    - short_summary: สรุปสั้นๆสำหรับ description
    - long_note: รายละเอียด fee lines สำหรับ note
    """
    t = normalize_text(text)
    names = []
    lines = []
    for m in RE_TIKTOK_FEE_LINE.finditer(t):
        name = (m.group(1) or "").strip()
        exv = m.group(2)
        vat = m.group(3)
        inc = m.group(4)
        if name:
            names.append(name)
            lines.append(f"- {name}: exVAT {exv}, VAT {vat}, incVAT {inc}")
        if len(lines) >= max_lines:
            break

    if not lines:
        return ("", "")

    short = "Fees: " + ", ".join(names[:5]) + (" ..." if len(names) > 5 else "")
    note = "\n".join(lines)
    return (short, note)
