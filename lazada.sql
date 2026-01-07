# backend/app/extractors/lazada.py
from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List

# ✅ import เฉพาะของที่ "ชัวร์ว่ามี" ใน common.py
from .common import (
    base_row_dict,
    detect_platform_vendor,
    find_invoice_no,
    find_totals,
    find_best_date,
)

from ..utils.text_utils import (
    normalize_text,
    fmt_tax_13,
    fmt_branch_5,
    parse_date_to_yyyymmdd,
    parse_money,
)

# ============================================================
# Optional (ถ้ามีใน common.py ก็ใช้ / ถ้าไม่มีไม่ทำให้พัง)
# ============================================================

def _try_import_optional_common():
    """
    กันไฟล์พังตอน import ถ้า common.py ยังไม่มีฟังก์ชันใหม่
    """
    try:
        from . import common as _common  # type: ignore
    except Exception:
        return None
    return _common

_COMMON = _try_import_optional_common()

def _find_first_date(text: str) -> str:
    if _COMMON and hasattr(_COMMON, "find_first_date"):
        try:
            return str(_COMMON.find_first_date(text) or "")
        except Exception:
            pass
    # fallback
    return find_best_date(text) or ""

def _find_best_reference(text: str) -> str:
    if _COMMON and hasattr(_COMMON, "find_best_reference"):
        try:
            return str(_COMMON.find_best_reference(text) or "")
        except Exception:
            pass
    # fallback: invoice no
    return find_invoice_no(text) or ""

def _find_payment_method(text: str) -> str:
    if _COMMON and hasattr(_COMMON, "find_payment_method"):
        try:
            return str(_COMMON.find_payment_method(text) or "")
        except Exception:
            pass
    # fallback: keyword-based
    t = normalize_text(text)
    if re.search(r"(?:หักจากยอดขาย|deduct(?:ed)?\s+from\s+sales)", t, re.IGNORECASE):
        return "หักจากยอดขาย"
    if re.search(r"(?:โอน|transfer|bank\s+transfer)", t, re.IGNORECASE):
        return "โอน"
    return ""


# ============================================================
# Lazada patterns (รองรับหลาย layout + ไทย/อังกฤษ)
# ============================================================

# Seller tax id (Lazada)
RE_LAZADA_SELLER_TAX = re.compile(
    r"(?:Lazada|LAZADA).*?(?:Tax\s*ID|Tax\s*Registration\s*No\.?|เลขประจำตัวผู้เสียภาษี)\s*[:：]?\s*([0-9][0-9\s-]{10,20}[0-9])",
    re.IGNORECASE | re.DOTALL,
)

# Branch (Head office / Branch no)
RE_BRANCH_NO = re.compile(
    r"(?:Head\s*office/Branch\s*no\.?|Head\s*Office\s*/\s*Branch\s*No\.?|สาขา(?:ที่)?|Branch\s*No\.?)\s*[:：]?\s*(สำนักงานใหญ่|Head\s*Office|[0-9]{1,5})",
    re.IGNORECASE,
)

# Invoice/Doc/Receipt numbers
RE_INVOICE_NO = re.compile(
    r"(?:Invoice\s*No\.?|Tax\s*Invoice\s*No\.?|Document\s*No\.?|Receipt\s*No\.?|Statement\s*No\.?)\s*[:#： ]*\s*([A-Z0-9][A-Z0-9\-_/.]{4,})",
    re.IGNORECASE,
)

# Dates
RE_INVOICE_DATE = re.compile(
    r"(?:Invoice\s*Date|Document\s*Date|Receipt\s*Date|Date)\s*[:：]?\s*([0-9]{4}[-/\.][0-9]{1,2}[-/\.][0-9]{1,2})",
    re.IGNORECASE,
)

RE_PERIOD = re.compile(
    r"(?:Period|Billing\s*Period|งวด)\s*[:：]?\s*([0-9]{4}[-/\.][0-9]{1,2}[-/\.][0-9]{1,2})\s*[-–]\s*([0-9]{4}[-/\.][0-9]{1,2}[-/\.][0-9]{1,2})",
    re.IGNORECASE,
)

# Totals variants
RE_TOTAL_INC = re.compile(
    r"(?:Total\s*\(Including\s*Tax\)|Total\s*Including\s*Tax|Total\s*Amount\s*\(Incl\.?\s*VAT\)|Grand\s*Total|Amount\s*Due|Total\s*Due)\s*[:： ]*\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

RE_TOTAL_EX = re.compile(
    r"(?:Total\s*\(Excluding\s*Tax\)|Total\s*Excluding\s*Tax|Subtotal|Total\s*before\s*VAT)\s*[:： ]*\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

RE_VAT_AMT = re.compile(
    r"(?:VAT|ภาษีมูลค่าเพิ่ม)\s*(?:7%|7\s*%)?\s*[:： ]*\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# WHT
RE_WHT_TH = re.compile(
    r"หักภาษีณ\s*ที่จ่าย.*?(?:ร้อยละ|อัตรา)\s*([0-9]{1,2})\s*%.*?(?:จำนวน|เป็นจำนวน)\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)

RE_WHT_EN = re.compile(
    r"(?:Withholding\s*Tax|WHT|Withheld\s*Tax)\s*(?:\(?\s*([0-9]{1,2})\s*%\s*\)?)?\s*[:： ]*\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Line items (best effort) – หลีกเลี่ยงบรรทัดรวม/ภาษี
RE_LINE_ITEM = re.compile(
    r"^\s*(?!.*(?:Total|Subtotal|VAT|WHT|Grand\s*Total|รวม|ยอด|ภาษี))"
    r"(.{6,120}?)\s{2,}([0-9,]+(?:\.[0-9]{1,2})?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


# ============================================================
# Safe pickers
# ============================================================

def _pick_invoice_no(text: str) -> str:
    t = normalize_text(text)
    m = RE_INVOICE_NO.search(t)
    if m:
        return m.group(1).strip()
    return find_invoice_no(t) or ""

def _pick_invoice_date(text: str) -> str:
    t = normalize_text(text)
    m = RE_INVOICE_DATE.search(t)
    if m:
        return parse_date_to_yyyymmdd(m.group(1)) or ""
    return _find_first_date(t)

def _pick_period(text: str) -> Tuple[str, str]:
    t = normalize_text(text)
    m = RE_PERIOD.search(t)
    if not m:
        return ("", "")
    a = parse_date_to_yyyymmdd(m.group(1)) or ""
    b = parse_date_to_yyyymmdd(m.group(2)) or ""
    return (a, b)

def _pick_seller_tax_id(text: str) -> str:
    t = normalize_text(text)
    m = RE_LAZADA_SELLER_TAX.search(t)
    if m:
        return fmt_tax_13(m.group(1))
    # fallback: ไม่มีจริงก็ปล่อยว่าง
    return ""

def _pick_branch(text: str) -> str:
    t = normalize_text(text)
    if "สำนักงานใหญ่" in t or "head office" in t or "headoffice" in t.replace(" ", "").lower():
        return "00000"
    m = RE_BRANCH_NO.search(t)
    if not m:
        return ""
    raw = (m.group(1) or "").strip()
    if raw.lower().replace(" ", "") in ("สำนักงานใหญ่", "headoffice"):
        return "00000"
    return fmt_branch_5(raw)

def _pick_wht(text: str) -> Tuple[str, str]:
    """
    Returns (rate, amount) where amount is numeric string (e.g., 123.45)
    """
    t = normalize_text(text)

    m = RE_WHT_TH.search(t)
    if m:
        rate = f"{m.group(1).strip()}%"
        amt = parse_money(m.group(2)) or ""
        return (rate, amt)

    m2 = RE_WHT_EN.search(t)
    if m2:
        rate_raw = (m2.group(1) or "").strip()
        rate = f"{rate_raw}%" if rate_raw and not rate_raw.endswith("%") else rate_raw
        amt = parse_money(m2.group(2)) or ""
        return (rate, amt)

    return ("", "")

def _pick_amounts(text: str) -> Tuple[str, str, str]:
    """
    Return (total_inc, subtotal_ex, vat_amt)
    """
    t = normalize_text(text)

    # 1) explicit total including tax
    m_inc = RE_TOTAL_INC.search(t)
    total_inc = parse_money(m_inc.group(1)) if m_inc else ""

    # 2) common totals
    common = find_totals(t) or {}
    if not total_inc:
        total_inc = common.get("total_due") or ""

    # 3) subtotal
    m_ex = RE_TOTAL_EX.search(t)
    subtotal_ex = parse_money(m_ex.group(1)) if m_ex else (common.get("subtotal") or "")

    # 4) vat
    m_v = RE_VAT_AMT.search(t)
    vat_amt = parse_money(m_v.group(1)) if m_v else (common.get("vat_amt") or "")

    # 5) compute if needed
    if not total_inc and subtotal_ex and vat_amt:
        try:
            total_inc = f"{(float(subtotal_ex) + float(vat_amt)):.2f}"
        except Exception:
            pass

    return (total_inc or "", subtotal_ex or "", vat_amt or "")

def _build_description(text: str, period_a: str, period_b: str) -> Tuple[str, str]:
    t = normalize_text(text)
    items: List[str] = []
    lines: List[str] = []

    for m in RE_LINE_ITEM.finditer(t):
        desc = (m.group(1) or "").strip()
        amt = parse_money(m.group(2)) or ""
        if not desc or not amt:
            continue
        items.append(f"{desc} {amt}")
        lines.append(f"- {desc}: {amt}")
        if len(items) >= 6:
            break

    period_txt = f"Period {period_a}-{period_b}" if (period_a and period_b) else ""

    if items:
        short = "Lazada Fees: " + "; ".join(items[:3])
        if period_txt:
            short += f" | {period_txt}"
        note = "\n".join(lines[:10])
        return short, note

    short = "Lazada เอกสารค่าธรรมเนียม/ภาษี (ตรวจสอบรายละเอียด)"
    if period_txt:
        short += f" | {period_txt}"
    return short, ""


# ============================================================
# Main extractor
# ============================================================

def extract_lazada(text: str) -> Dict[str, Any]:
    """
    Lazada TAX INVOICE/RECEIPT → PEAK A–U row

    - คู่ค้า: Lazada
    - Tax ID / Branch: ใช้ของ Lazada (ผู้ขาย) เป็นหลัก
    - วันที่: ใช้ Invoice Date ถ้ามี ไม่งั้น fallback first/best date
    - N_unit_price: ถ้ามี subtotal ก่อน VAT ให้ใส่ subtotal (แม่นกว่า) ไม่งั้นใช้ total
    - R_paid_amount: total including tax
    - WHT: ใส่ "จำนวนเงิน" ที่ถูกหัก (P_wht) และ S_pnd=53
    """
    t = normalize_text(text)
    row = base_row_dict()

    # vendor/platform detect
    vendor_full, vendor_key = detect_platform_vendor(t)
    # ไม่บังคับ vendor_full เพราะ D_vendor_code เราใช้เป็นรหัสสั้น
    row["D_vendor_code"] = "Lazada"
    row["U_group"] = "Marketplace Expense"

    # invoice/date/period
    inv_no = _pick_invoice_no(t)
    inv_date = _pick_invoice_date(t)
    period_a, period_b = _pick_period(t)

    if inv_no:
        row["G_invoice_no"] = inv_no
        row["C_reference"] = _find_best_reference(t) or inv_no
    else:
        row["C_reference"] = _find_best_reference(t)

    if inv_date:
        row["B_doc_date"] = inv_date
        row["H_invoice_date"] = inv_date
        row["I_tax_purchase_date"] = inv_date

    # tax/branch (seller Lazada)
    seller_tax = _pick_seller_tax_id(t)
    branch_5 = _pick_branch(t)

    if seller_tax:
        row["E_tax_id_13"] = seller_tax
    if branch_5:
        row["F_branch_5"] = branch_5

    # payment method (optional)
    pm = _find_payment_method(t)
    if pm:
        row["Q_payment_method"] = pm

    # amounts
    total_inc, subtotal_ex, vat_amt = _pick_amounts(t)

    row["M_qty"] = "1"

    # ✅ แนะนำให้ N_unit_price = subtotal (ก่อน VAT) ถ้ามี
    if subtotal_ex:
        row["N_unit_price"] = subtotal_ex
    elif total_inc:
        row["N_unit_price"] = total_inc

    if total_inc:
        row["R_paid_amount"] = total_inc
    elif subtotal_ex:
        row["R_paid_amount"] = subtotal_ex

    # VAT defaults
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # WHT
    wht_rate, wht_amt = _pick_wht(t)
    if wht_amt:
        row["P_wht"] = wht_amt
        row["S_pnd"] = "53"
        # เก็บ rate ไว้ audit
        if wht_rate:
            row["T_note"] = f"WHT {wht_rate}".strip()

    # description
    desc, note = _build_description(t, period_a, period_b)
    row["L_description"] = desc
    if note:
        row["T_note"] = (row.get("T_note") or "").strip()
        row["T_note"] = (row["T_note"] + ("\n" if row["T_note"] else "") + note).strip()

    return row
