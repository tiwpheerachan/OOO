# -*- coding: utf-8 -*-
# backend/app/extractors/ads_google.py
"""
Google Ads Receipt Extractor (PEAK A-U) - Enhanced v1.2 (POST-PROCESS READY)

ทำตามแผนของคุณครบ:
1) ✅ รับ filename + client_tax_id ทุกครั้ง
2) ✅ คำนวณตัวเลขให้ถูก: PRIMARY = Payment amount THB
3) ✅ จบด้วย post_process_peak_row() เพื่อ:
   - enforce C_reference/G_invoice_no จาก filename
   - apply K_account (GL) จาก client_tax_id + platform_key (google_ads)
   - apply description template
   - enforce N/R เป็นเงินที่ถูกต้อง
4) ✅ policy:
   - O_vat_rate = "NO"
   - T_note = ""
   - P_wht = "" (ตาม flow ล่าสุดของคุณ)
   - ไม่ให้ตัวเลขอื่นไปทับ N/R

หมายเหตุ:
- เราเติม meta fields ให้ template ทำงาน:
  _brand, _payment_no, _payment_method
  (post_process จะใช้สร้าง "Record Ads - Google - ...")
"""

from __future__ import annotations

import re
from typing import Dict, Any

from .common import (
    base_row_dict,
    normalize_text,
    format_peak_row,
    parse_money,
    find_best_date,
)

# ✅ NEW: post process (Plan C)
try:
    from .post_process import post_process_peak_row
except Exception:  # pragma: no cover
    post_process_peak_row = None  # type: ignore


# ========================================
# Vendor constants
# ========================================
VENDOR_NAME = "Google Asia Pacific"
VENDOR_TAX_ID = "200817984R"  # Singapore TIN (not 13 digits; ok for this pipeline)

# ========================================
# Optional vendor mapping
# ========================================
try:
    from .vendor_mapping import get_vendor_code  # type: ignore
    VENDOR_MAPPING_AVAILABLE = True
except Exception:  # pragma: no cover
    VENDOR_MAPPING_AVAILABLE = False


# ========================================
# Regex
# ========================================
RE_PAYMENT_DATE = re.compile(
    r"\bPayment\s*date\b\s*[:\-]?\s*([A-Z][a-z]{2})\s+(\d{1,2})(?:,)?\s+(\d{4})",
    re.IGNORECASE,
)

RE_BILLING_ID = re.compile(
    r"\bBilling\s*ID\b\s*[:\-]?\s*([0-9]{3,6}(?:-[0-9]{3,6}){2,4})",
    re.IGNORECASE,
)

RE_PAYMENT_NO = re.compile(
    r"\bPayment\s*number\b\s*[:\-]?\s*([A-Z0-9]{8,40})",
    re.IGNORECASE,
)

RE_PAYMENT_AMOUNT = re.compile(
    r"\bPayment\s*amount\b\s*[:\-]?\s*THB\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Fallback: "THB 50,000.00" anywhere (pick largest THB)
RE_ANY_THB = re.compile(
    r"\bTHB\s*([0-9,]+(?:\.[0-9]{1,2})?)\b",
    re.IGNORECASE,
)

RE_PAYMENT_METHOD = re.compile(
    r"\bPayment\s*method\b\s*[:\-]?\s*([^\n]+)",
    re.IGNORECASE,
)

# Optional: Brand / account hint lines (ไว้ช่วย description template)
RE_ACCOUNT_NAME = re.compile(
    r"\bAccount\s*(?:name|Name)\b\s*[:\-]?\s*([^\n]{2,80})",
    re.IGNORECASE,
)
RE_PROFILE_NAME = re.compile(
    r"\bPayments?\s+profile\b\s*[:\-]?\s*([^\n]{2,80})",
    re.IGNORECASE,
)

MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def _squash_no_space(s: str) -> str:
    """Remove ALL whitespace (spaces/newlines/tabs)."""
    if not s:
        return ""
    return re.sub(r"\s+", "", str(s))


def _parse_en_month_date(mon: str, day: str, year: str) -> str:
    mm = MONTHS.get(mon[:3].title(), "01")
    dd = str(day).zfill(2)
    return f"{year}{mm}{dd}"


def _pick_amount_primary(t: str) -> str:
    """
    Primary: Payment amount THB xxxx.xx
    Fallback: any THB amount (pick largest numeric).
    """
    m = RE_PAYMENT_AMOUNT.search(t)
    if m:
        return parse_money(m.group(1)) or ""

    cands = []
    for mm in RE_ANY_THB.finditer(t):
        v = parse_money(mm.group(1)) or ""
        if v:
            try:
                cands.append(float(v))
            except Exception:
                pass
    if not cands:
        return ""
    return f"{max(cands):.2f}"


def extract_google_ads(text: str, *, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    Output PEAK A-U dict.

    Pre post-process mapping:
      - N_unit_price  = Payment amount (THB)
      - R_paid_amount = Payment amount (THB)
      - O_vat_rate    = "NO"
      - U_group       = "Advertising Expense"
      - T_note        = ""

    Post-process (Plan C):
      - enforce C_reference/G_invoice_no from filename
      - apply K_account based on client_tax_id + platform_key (google_ads)
      - apply description template
      - enforce amounts safe
    """
    t = normalize_text(text or "")
    row = base_row_dict()

    # --------------------------
    # Vendor
    # --------------------------
    row["E_tax_id_13"] = VENDOR_TAX_ID

    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            row["D_vendor_code"] = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=VENDOR_TAX_ID,
                vendor_name=VENDOR_NAME,
            ) or VENDOR_NAME
        except Exception:
            row["D_vendor_code"] = VENDOR_NAME
    else:
        row["D_vendor_code"] = VENDOR_NAME

    row["F_branch_5"] = "00000"

    # --------------------------
    # Date (B/H): Payment date
    # --------------------------
    doc_date = ""
    md = RE_PAYMENT_DATE.search(t)
    if md:
        doc_date = _parse_en_month_date(md.group(1), md.group(2), md.group(3))
    else:
        doc_date = find_best_date(t) or ""

    if doc_date:
        row["B_doc_date"] = doc_date
        row["H_invoice_date"] = doc_date
    row["I_tax_purchase_date"] = ""  # reverse charge doc; keep empty by your style

    # --------------------------
    # Reference (C/G): Payment number > Billing ID
    # (สุดท้าย Plan C จะ enforce จาก filename)
    # --------------------------
    ref = ""
    mref = RE_PAYMENT_NO.search(t)
    if mref:
        ref = mref.group(1).strip()
    else:
        mb = RE_BILLING_ID.search(t)
        if mb:
            ref = mb.group(1).strip()

    ref = _squash_no_space(ref)
    if ref:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref

    # --------------------------
    # Amount: PRIMARY = Payment amount THB
    # --------------------------
    amt = _pick_amount_primary(t)

    row["M_qty"] = "1"
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "NO"

    if amt:
        row["N_unit_price"] = amt
        row["R_paid_amount"] = amt
    else:
        row["N_unit_price"] = "0"
        row["R_paid_amount"] = "0"

    # --------------------------
    # Accounting classification
    # --------------------------
    row["U_group"] = "Advertising Expense"
    row["L_description"] = "Advertising Expense"  # จะถูก template ทับใน post-process

    # --------------------------
    # Policy-required empties
    # --------------------------
    row["K_account"] = ""   # post-process จะใส่ GL ให้
    row["P_wht"] = ""       # ✅ policy ล่าสุดคุณ: ไม่ใส่ wht
    row["Q_payment_method"] = ""
    row["S_pnd"] = ""
    row["T_note"] = ""      # ✅ ต้องว่าง

    # --------------------------
    # Meta fields for description template (post-process ใช้)
    # --------------------------
    # Brand/hint: พยายามหา Account name หรือ Payments profile
    macc = RE_ACCOUNT_NAME.search(t)
    if macc:
        row["_brand"] = macc.group(1).strip()
    else:
        mp = RE_PROFILE_NAME.search(t)
        if mp:
            row["_brand"] = mp.group(1).strip()

    if ref:
        row["_payment_no"] = ref

    pm = RE_PAYMENT_METHOD.search(t)
    if pm:
        row["_payment_method"] = pm.group(1).strip()

    # --------------------------
    # Final: run post-process (Plan C)
    # --------------------------
    if post_process_peak_row:
        out = post_process_peak_row(
            row,
            platform="google",         # ให้ infer_platform_key() เห็นว่าเป็น google_ads
            filename=filename or "",
            client_tax_id=client_tax_id or "",
            text=text or "",
        )
        return format_peak_row(out)

    # fallback: no post-process available
    return format_peak_row(row)


__all__ = ["extract_google_ads"]
