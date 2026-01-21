# -*- coding: utf-8 -*-
# backend/app/extractors/meta_ads.py
"""
Meta Ads Receipt Extractor (PEAK A-U) - Enhanced v1.2 (POST-PROCESS READY)

แผนที่ทำตามที่คุณวางไว้:
1) ✅ extractor ต้องรับ/ส่งต่อ filename + client_tax_id
2) ✅ จบด้วย post_process_peak_row() เพื่อ:
   - enforce C_reference/G_invoice_no จาก filename
   - apply K_account (GL) ตาม client_tax_id + platform_key
   - apply description template
   - enforce N/R ให้เป็นตัวเลขเงินที่ถูก
3) ✅ คำนวณตัวเลขไม่มั่ว:
   - ใช้ Paid ฿... THB เป็น PRIMARY เสมอ
   - ห้าม WHT/ตัวเลขอื่นไปทับ N/R
4) ✅ policy:
   - O_vat_rate = "NO" (reverse charge)
   - T_note = ""
   - ไม่ยัด P_wht (ปล่อยว่างตาม flow ล่าสุดของคุณ)

หมายเหตุ:
- post_process จะ "ทับ" C/G จาก filename ตามแผน (Plan C)
- เพื่อให้ description template ใช้ได้: ใส่ meta fields:
    _brand, _account_id, _transaction_id, _payment_method
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
VENDOR_NAME = "Meta Platforms Ireland"
# NOTE: meta VAT ID is not Thai 13-digit; keep your pipeline-friendly value
VENDOR_TAX_ID = "0993000454995"

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
RE_RECEIPT_FOR = re.compile(r"\bReceipt\s+for\s+([^\n]+)", re.IGNORECASE)
RE_ACCOUNT_ID = re.compile(r"\bAccount\s*ID\b\s*[:\-]?\s*([0-9]{5,})", re.IGNORECASE)

# "Invoice/Payment Date" ... "Dec 4, 2025, 11:42 AM"
RE_INV_PAY_DATE = re.compile(
    r"\b(?:Invoice/Payment\s*Date|Payment\s*Date)\b\s*[:\-]?\s*([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})",
    re.IGNORECASE,
)

RE_REFERENCE_NO = re.compile(
    r"\bReference\s*Number\b\s*[:\-]?\s*([A-Z0-9]{6,32})",
    re.IGNORECASE,
)

RE_TRANSACTION_ID = re.compile(
    r"\bTransaction\s*ID\b\s*[:\-]?\s*([0-9][0-9\-]{8,80})",
    re.IGNORECASE,
)

# PRIMARY: Paid ฿30,000.00 THB
RE_PAID = re.compile(
    r"\bPaid\b\s*[:\-]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)\s*THB\b",
    re.IGNORECASE,
)

# fallback: any "฿x,xxx.xx THB" (pick largest)
RE_ANY_BAHT_THB = re.compile(
    r"฿\s*([0-9,]+(?:\.[0-9]{1,2})?)\s*THB\b",
    re.IGNORECASE,
)

RE_PAYMENT_METHOD = re.compile(r"\bPayment\s*method\b\s*[:\-]?\s*([^\n]+)", re.IGNORECASE)

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


def _pick_paid_amount(t: str) -> str:
    """
    Primary: Paid ... ฿x THB
    Fallback: choose largest ฿...THB in doc.
    """
    m = RE_PAID.search(t)
    if m:
        return parse_money(m.group(1)) or ""

    cands = []
    for mm in RE_ANY_BAHT_THB.finditer(t):
        v = parse_money(mm.group(1)) or ""
        if v:
            try:
                cands.append(float(v))
            except Exception:
                pass
    if not cands:
        return ""
    return f"{max(cands):.2f}"


def extract_meta_ads(text: str, *, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    Output PEAK A-U dict.

    Mapping policy (pre post-process):
      - N_unit_price  = Paid amount (THB)
      - R_paid_amount = Paid amount (THB)
      - O_vat_rate    = "NO"
      - U_group       = "Advertising Expense"
      - T_note        = "" (must be empty)

    Post-process (Plan C):
      - enforce C_reference/G_invoice_no from filename
      - apply K_account (GL) based on client_tax_id + platform_key (meta_ads)
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
    # Date (B/H)
    # --------------------------
    doc_date = ""
    md = RE_INV_PAY_DATE.search(t)
    if md:
        doc_date = _parse_en_month_date(md.group(1), md.group(2), md.group(3))
    else:
        doc_date = find_best_date(t) or ""

    if doc_date:
        row["B_doc_date"] = doc_date
        row["H_invoice_date"] = doc_date
    row["I_tax_purchase_date"] = ""  # keep empty by your style (reverse charge/overseas)

    # --------------------------
    # Reference (C/G) - pre fill only
    # (สุดท้าย Plan C จะ enforce จาก filename)
    # --------------------------
    ref = ""
    mref = RE_REFERENCE_NO.search(t)
    if mref:
        ref = mref.group(1).strip()
    else:
        mtx = RE_TRANSACTION_ID.search(t)
        if mtx:
            ref = mtx.group(1).strip()

    ref = _squash_no_space(ref)
    if ref:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref

    # --------------------------
    # Amount (Paid เป็น PRIMARY)
    # --------------------------
    amt = _pick_paid_amount(t)

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
    # L_description จะถูก template ทับใน post-process; แต่กันว่างไว้
    row["L_description"] = "Advertising Expense"

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
    mf = RE_RECEIPT_FOR.search(t)
    if mf:
        # ใช้เป็น "brand_or_hint" สำหรับ Ads template
        row["_brand"] = mf.group(1).strip()

    ma = RE_ACCOUNT_ID.search(t)
    if ma:
        row["_account_id"] = ma.group(1).strip()

    mtx2 = RE_TRANSACTION_ID.search(t)
    if mtx2:
        row["_transaction_id"] = mtx2.group(1).strip()

    pm = RE_PAYMENT_METHOD.search(t)
    if pm:
        row["_payment_method"] = pm.group(1).strip()

    # บางใบไม่มี payment_no แต่มี reference number
    if ref:
        row["_payment_no"] = ref

    # --------------------------
    # Final: run post-process (Plan C)
    # --------------------------
    if post_process_peak_row:
        out = post_process_peak_row(
            row,
            platform="meta",           # ให้ infer_platform_key() เห็นว่าเป็น meta_ads
            filename=filename or "",
            client_tax_id=client_tax_id or "",
            text=text or "",
        )
        # post_process จะ format แล้ว แต่กันไว้
        return format_peak_row(out)

    # fallback: no post-process available
    return format_peak_row(row)


__all__ = ["extract_meta_ads"]
