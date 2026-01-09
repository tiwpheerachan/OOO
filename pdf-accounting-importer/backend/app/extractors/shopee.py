# backend/app/extractors/shopee.py
"""
Shopee extractor - PEAK A-U format (Enhanced v3.3 - Ref Glue + P_wht Blank)
Fix goals (สำคัญที่สุด):
  ✅ Amounts ต้องถูก: subtotal(excl vat), vat, total(incl vat), wht(3%) (แต่ไม่ลง P_wht)
  ✅ กัน VAT/WHT สลับช่อง: P_wht ว่างเสมอ (ตาม requirement ใหม่)
  ✅ บังคับ full reference = DOCNO + MMDD-XXXXXXX และ "ห้ามมีช่องว่าง/ขึ้นบรรทัด" (glue ข้ามบรรทัด)
  ✅ D_vendor_code = Cxxxxx แบบ client-aware (ถ้ามี vendor_mapping)
  ✅ T_note เว้นว่างเสมอ
"""

from __future__ import annotations

import re
from typing import Dict, Any, Tuple

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_best_date,
    parse_date_to_yyyymmdd,
    extract_amounts,
    extract_seller_info,
    format_peak_row,
    parse_money,
)

# ========================================
# Import vendor mapping (with fallback)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_SHOPEE,
    )
    VENDOR_MAPPING_AVAILABLE = True
except Exception:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SHOPEE = "0105558019581"  # Shopee (Thailand) default tax id


# ============================================================
# Shopee-specific patterns
# ============================================================

RE_SHOPEE_DOC_TI_FORMAT = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,})\b",
    re.IGNORECASE,
)
RE_SHOPEE_DOC_TRS_FORMAT = re.compile(
    r"\b(TRS[A-Z0-9\-/]{10,})\b",
    re.IGNORECASE,
)
RE_SHOPEE_DOC_STRICT = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-/]{10,})\b",
    re.IGNORECASE,
)

# Code like: 1218-0001593 (may have whitespace/newlines around '-')
RE_SHOPEE_REFERENCE_CODE_FLEX = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")

# TRS + (space/newline) + 1218 - 0001593
RE_SHOPEE_FULL_REFERENCE = re.compile(
    r"\b(TRS[A-Z0-9\-/]{10,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE,
)

RE_SHOPEE_DOC_DATE = re.compile(
    r"(?:วันที่(?:เอกสาร|ออกเอกสาร)?|Date\s*(?:of\s*issue)?|Issue\s*date|Document\s*date)\s*[:#：]?\s*"
    r"(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})",
    re.IGNORECASE,
)
RE_SHOPEE_INVOICE_DATE = re.compile(
    r"(?:วันที่ใบกำกับ(?:ภาษี)?|Invoice\s*date|Tax\s*Invoice\s*date)\s*[:#：]?\s*"
    r"(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})",
    re.IGNORECASE,
)

RE_SHOPEE_SELLER_ID = re.compile(
    r"(?:Seller\s*ID|Shop\s*ID|รหัสร้านค้า)\s*[:#：]?\s*([0-9]{8,12})",
    re.IGNORECASE,
)
RE_SHOPEE_USERNAME = re.compile(
    r"(?:Username|Shop\s*name|User\s*name|ชื่อผู้ใช้|ชื่อร้าน)\s*[:#：]?\s*([A-Za-z0-9_\-\.]{3,30})",
    re.IGNORECASE,
)

# WHT patterns (we still detect to set S_pnd optionally, but P_wht must stay blank)
RE_SHOPEE_WHT_THAI = re.compile(
    r"(?:หัก|ภาษี).*?ที่จ่าย.*?(?:อัตรา|ร้อยละ)\s*([0-9]{1,2})\s*%.*?(?:จำนวน|เป็นเงิน)\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE | re.DOTALL,
)
# remark english line in many Shopee docs:
# "deducted 3% withholding tax ... at 8,716.68 THB"
RE_SHOPEE_WHT_EN = re.compile(
    r"withholding\s+tax.*?(\d{1,2})\s*%.*?(?:at|=)\s*([0-9,]+(?:\.[0-9]{2})?)\s*THB",
    re.IGNORECASE | re.DOTALL,
)

# Summary money lines (key fix!)
# Examples (Thai/Eng mixed):
# "Total Value of Services (Excluded VAT) 290,556.08"
# "VAT 7% 20,338.92"
# "Total Value of Services (Included VAT) 310,895.00"
RE_SUM_EXCL = re.compile(
    r"Total\s*Value\s*of\s*Services\s*\(Excluded\s*VAT\)\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE,
)
RE_SUM_INCL = re.compile(
    r"Total\s*Value\s*of\s*Services\s*\(Included\s*VAT\)\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE,
)
RE_SUM_VAT = re.compile(
    r"(?:VAT\s*7%\s*|ภาษีมูลค่าเพิ่ม\s*7%\s*)([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE,
)

# Some templates show "after discount" line; we still want the excl VAT number
RE_SUM_EXCL_AFTER_DISCOUNT = re.compile(
    r"Excluded\s*VAT\)\s*after\s*discount\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE,
)

# Remove ALL whitespace (space/tab/newline) to glue tokens across lines
RE_ALL_WS = re.compile(r"\s+")


# ============================================================
# Helpers
# ============================================================

def _money(v: str) -> str:
    """Return normalized money '1234.56' or ''."""
    try:
        return parse_money(v)
    except Exception:
        return ""


def _compact_ref(v: Any) -> str:
    """
    Remove ALL whitespace (spaces, tabs, newlines) to glue tokens across lines.
    Example:
      "RCSPXSPB00-00000-25 1218-0001593"
      "RCSPXSPB00-00000-25\n1218-0001593"
      -> "RCSPXSPB00-00000-251218-0001593"
    """
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _clean_ref_code(mmdd: str, seq7: str) -> str:
    return f"{mmdd}-{seq7}"


def _extract_ref_code_anywhere(t: str) -> str:
    m = RE_SHOPEE_REFERENCE_CODE_FLEX.search(t)
    if not m:
        return ""
    return _clean_ref_code(m.group(1), m.group(2))


# ============================================================
# Seller ID helpers (used by job_worker wallet mapping)
# ============================================================

def extract_seller_id_shopee(text: str) -> Tuple[str, str]:
    t = normalize_text(text)
    seller_id = ""
    username = ""

    m = RE_SHOPEE_SELLER_ID.search(t)
    if m:
        seller_id = m.group(1)

    m = RE_SHOPEE_USERNAME.search(t)
    if m:
        username = m.group(1)

    if not seller_id:
        seller_info = extract_seller_info(t)
        seller_id = seller_info.get("seller_id", "") or ""
        if not username:
            username = seller_info.get("username", "") or ""

    return seller_id, username


# ============================================================
# WHT extraction (for detecting PND only; P_wht must stay blank)
# ============================================================

def extract_wht_from_shopee_text(text: str) -> Tuple[str, str]:
    """
    Returns: (rate, amount) e.g. ("3%", "8716.68")
    Prefer Thai pattern, fallback to English remark.
    """
    t = text or ""
    m = RE_SHOPEE_WHT_THAI.search(t)
    if m:
        rate = f"{m.group(1)}%"
        amount = _money(m.group(2))
        return rate, amount

    m2 = RE_SHOPEE_WHT_EN.search(t)
    if m2:
        rate = f"{m2.group(1)}%"
        amount = _money(m2.group(2))
        return rate, amount

    return "", ""


# ============================================================
# Reference extraction (NO whitespace allowed)
# ============================================================

def extract_shopee_full_reference(text: str, filename: str = "") -> str:
    """
    Return a FULL reference that matches your legacy logic:
      DOCNO + MMDD-XXXXXXX
    BUT must be glued (no whitespace) even if printed on separate lines.
    """
    t = normalize_text(text or "")
    fn = normalize_text(filename or "")

    # 1) Best: TRS + mmdd-xxxxxxx
    m = RE_SHOPEE_FULL_REFERENCE.search(t)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _compact_ref(f"{doc}{ref}")  # ✅ glue

    # 2) TIxx full token
    m_doc = RE_SHOPEE_DOC_TI_FORMAT.search(t)
    if m_doc:
        return _compact_ref(m_doc.group(1))

    # 3) TRS doc + ref code anywhere
    doc_no = ""
    m_doc = RE_SHOPEE_DOC_TRS_FORMAT.search(t)
    if m_doc:
        doc_no = m_doc.group(1)

    if doc_no:
        ref = _extract_ref_code_anywhere(t)
        if ref:
            return _compact_ref(f"{doc_no}{ref}")   # ✅ glue
        return _compact_ref(doc_no)

    # 4) strict doc
    m = RE_SHOPEE_DOC_STRICT.search(t)
    if m:
        doc = m.group(1)
        if doc.upper().startswith("TRS"):
            ref = _extract_ref_code_anywhere(t)
            if ref:
                return _compact_ref(f"{doc}{ref}")  # ✅ glue
        return _compact_ref(doc)

    # -------- filename fallback --------
    m = RE_SHOPEE_FULL_REFERENCE.search(fn)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _compact_ref(f"{doc}{ref}")          # ✅ glue

    m_doc = RE_SHOPEE_DOC_TRS_FORMAT.search(fn)
    if m_doc:
        doc = m_doc.group(1)
        ref = _extract_ref_code_anywhere(fn)
        if ref:
            return _compact_ref(f"{doc}{ref}")      # ✅ glue
        return _compact_ref(doc)

    m_doc = RE_SHOPEE_DOC_TI_FORMAT.search(fn)
    if m_doc:
        return _compact_ref(m_doc.group(1))

    return ""


# ============================================================
# Amount extraction (summary-first)
# ============================================================

def extract_amounts_shopee_summary(text: str) -> Dict[str, str]:
    """
    Pull amounts from Shopee summary block (bottom of invoice).
    Returns dict: {subtotal, vat, total, wht_rate, wht_amount}
    All values are normalized strings.
    NOTE: P_wht must be blank in the final row; we keep wht here for detection only.
    """
    t = normalize_text(text or "")

    subtotal = ""
    vat = ""
    total = ""

    # subtotal (prefer "Excluded VAT" main line)
    m = RE_SUM_EXCL.search(t)
    if m:
        subtotal = _money(m.group(1))

    # sometimes only "after discount" exists or OCR shifts line
    if not subtotal:
        m2 = RE_SUM_EXCL_AFTER_DISCOUNT.search(t)
        if m2:
            subtotal = _money(m2.group(1))

    # vat
    m = RE_SUM_VAT.search(t)
    if m:
        vat = _money(m.group(1))

    # total
    m = RE_SUM_INCL.search(t)
    if m:
        total = _money(m.group(1))

    # withholding
    wht_rate, wht_amount = extract_wht_from_shopee_text(t)

    # If WHT missing but we have subtotal: compute 3% (detection only)
    if (not wht_amount) and subtotal:
        try:
            base = float(subtotal)
            if base > 0:
                calc = round(base * 0.03, 2)
                wht_amount = f"{calc:.2f}"
                wht_rate = wht_rate or "3%"
        except Exception:
            pass

    out: Dict[str, str] = {}
    if subtotal:
        out["subtotal"] = subtotal
    if vat:
        out["vat"] = vat
    if total:
        out["total"] = total
    if wht_rate:
        out["wht_rate"] = wht_rate
    if wht_amount:
        out["wht_amount"] = wht_amount

    return out


# ============================================================
# Main extraction function
# ============================================================

def extract_shopee(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Extract Shopee receipt/tax invoice to PEAK A-U.

    Key enforced rules:
      - C_reference & G_invoice_no are ALWAYS compacted (no whitespace)
      - P_wht is ALWAYS blank (per requirement)
      - T_note is ALWAYS blank
    """
    t = normalize_text(text)
    row = base_row_dict()

    # STEP 1: vendor tax + vendor code
    vendor_tax = find_vendor_tax_id(t, "Shopee") or VENDOR_SHOPEE
    row["E_tax_id_13"] = vendor_tax

    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            code = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=vendor_tax,
                vendor_name="Shopee",
            )
            row["D_vendor_code"] = code or "Shopee"
        except Exception:
            row["D_vendor_code"] = "Shopee"
    else:
        row["D_vendor_code"] = "Shopee"

    # Branch
    row["F_branch_5"] = find_branch(t) or "00000"

    # STEP 2: full reference (glued)
    full_ref = extract_shopee_full_reference(t, filename=filename)
    if full_ref:
        full_ref = _compact_ref(full_ref)
        row["G_invoice_no"] = full_ref
        row["C_reference"] = full_ref

    # STEP 3: dates
    date = ""
    m = RE_SHOPEE_DOC_DATE.search(t)
    if m:
        date = parse_date_to_yyyymmdd(m.group(1))
    if not date:
        m = RE_SHOPEE_INVOICE_DATE.search(t)
        if m:
            date = parse_date_to_yyyymmdd(m.group(1))
    if not date:
        date = find_best_date(t) or ""

    if date:
        row["B_doc_date"] = date
        row["H_invoice_date"] = date
        row["I_tax_purchase_date"] = date

    # STEP 4: Amounts (summary first, fallback later)
    sums = extract_amounts_shopee_summary(t)

    subtotal = sums.get("subtotal", "")
    vat = sums.get("vat", "")
    total = sums.get("total", "")
    wht_amount = sums.get("wht_amount", "")  # detection only

    # fallback to common extractor only if summary missing
    if not (subtotal or vat or total):
        amounts = extract_amounts(t)
        subtotal = subtotal or (amounts.get("subtotal", "") or "")
        vat = vat or (amounts.get("vat", "") or "")
        total = total or (amounts.get("total", "") or "")

        # detect WHT from fallback
        wht_amount = wht_amount or (amounts.get("wht_amount", "") or "")
        if not wht_amount:
            _wr, wa = extract_wht_from_shopee_text(t)
            if wa:
                wht_amount = wa

    # ✅ PEAK mapping (keep as your latest policy)
    row["M_qty"] = "1"
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # Unit price should be Excluded VAT (subtotal) whenever available
    if subtotal:
        row["N_unit_price"] = subtotal
    elif total:
        row["N_unit_price"] = total
    elif vat:
        row["N_unit_price"] = vat
    else:
        row["N_unit_price"] = "0"

    # Paid amount should be Total Included VAT whenever available
    if total:
        row["R_paid_amount"] = total
    elif subtotal:
        row["R_paid_amount"] = subtotal
    else:
        row["R_paid_amount"] = row.get("N_unit_price") or "0"

    # ✅ P_wht must be blank ALWAYS (your rule)
    row["P_wht"] = ""

    # Optional: keep PND when WHT detected (but P_wht stays blank)
    if wht_amount:
        row["S_pnd"] = "53"

    # Payment method (wallet mapping will override in job_worker for Shopee)
    row["Q_payment_method"] = "หักจากยอดขาย"

    # STEP 5: description + group
    row["L_description"] = "Marketplace Expense"
    row["U_group"] = "Marketplace Expense"

    # STEP 6: notes must be blank
    row["T_note"] = ""

    # STEP 7: final safety sync + COMPACT (no whitespace in C/G)
    row["C_reference"] = _compact_ref(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no", ""))

    if not row.get("C_reference") and row.get("G_invoice_no"):
        row["C_reference"] = row["G_invoice_no"]
    if not row.get("G_invoice_no") and row.get("C_reference"):
        row["G_invoice_no"] = row["C_reference"]

    # Ensure P_wht blank (again)
    row["P_wht"] = ""

    row["K_account"] = ""

    return format_peak_row(row)


__all__ = [
    "extract_shopee",
    "extract_shopee_full_reference",
    "extract_seller_id_shopee",
    "extract_wht_from_shopee_text",
    "extract_amounts_shopee_summary",
]
