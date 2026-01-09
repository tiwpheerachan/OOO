# backend/app/extractors/spx.py
"""
SPX Express extractor - PEAK A-U format (Enhanced v3.2 FIX AMOUNTS)

Fix goals (per your requirements):
  ✅ Avoid wrong amount root cause: NEVER put WHT amount into N_unit_price / R_paid_amount
  ✅ Separate amounts clearly:
      - total_ex_vat
      - vat_amount
      - total_inc_vat   (PRIMARY)
      - wht_amount_3pct
  ✅ Mapping must be:
      row["N_unit_price"]  = total_inc_vat
      row["R_paid_amount"] = total_inc_vat
      row["O_vat_rate"]    = "7%"
      row["P_wht"]         = "3%"   (only if WHT exists; safe default)
  ✅ Use Total Included VAT as the main total when available
  ✅ Clean notes (blank)
  ✅ Safe: never crash extraction, keep output stable

CRITICAL REF RULE (your requirement):
  - C_reference and G_invoice_no must have NO SPACES even if OCR splits lines
    Example: "RCSPXSPB00-00000-25 1218-0001593" -> "RCSPXSPB00-00000-251218-0001593"
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
    extract_seller_info,
    extract_amounts,     # fallback only
    format_peak_row,
    parse_money,
)

# ========================================
# POLICY FLAGS
# ========================================
# "AUTO"  -> P_wht = "3%" only when WHT 3% is detected (recommended safe)
# "ALWAYS"-> P_wht = "3%" always (if you want strict constant)
WHT_RATE_MODE = "AUTO"

# ========================================
# Import vendor mapping (optional)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_SPX,
    )
    VENDOR_MAPPING_AVAILABLE = True
except Exception:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SPX = "0105561164871"  # SPX Express (Thailand) Co., Ltd. Tax ID


# ============================================================
# SPX-specific patterns
# ============================================================

# Receipt/document number patterns
# e.g. "No: RCSPXSPB00-00000-25"
RE_SPX_DOCNO = re.compile(
    r"(?:เลขที่|No\.?)\s*[:#：]?\s*(RCS[A-Z0-9\-/]{8,})",
    re.IGNORECASE,
)

# Reference code (MMDD-XXXXXXX) — allow whitespace around dash
RE_SPX_REF_CODE_FLEX = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")

# Full reference pattern across whitespace/newlines:
#   DOCNO + (space/newline) + MMDD-XXXXXXX
RE_SPX_FULL_REFERENCE = re.compile(
    r"\b(RCS[A-Z0-9\-/]{8,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE,
)

# Seller info (optional)
RE_SPX_SELLER_ID = re.compile(r"(?:Seller\s*ID|Shop\s*ID|รหัสร้านค้า)\s*[:#：]?\s*(\d{8,12})", re.IGNORECASE)
RE_SPX_USERNAME  = re.compile(r"(?:Username|Shop\s*name|User\s*name|ชื่อผู้ใช้|ชื่อร้าน)\s*[:#：]?\s*([A-Za-z0-9_\-\.]{3,30})", re.IGNORECASE)

# Totals (VAT separated) — PRIMARY: Total inc VAT
RE_TOTAL_INC_VAT = re.compile(
    r"(?:รวม\s*ทั้ง\s*สิ้น|จำนวนเงินรวม\s*\(รวม\s*ภาษี|Total\s*(?:amount)?\s*\(?(?:including|incl\.?)\s*VAT\)?|Grand\s*Total)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

RE_TOTAL_EX_VAT = re.compile(
    r"(?:ก่อน\s*ภาษี|ยอดรวม\s*\(ไม่รวม\s*ภาษี|Subtotal\s*\(?(?:excluding|excl\.?)\s*VAT\)?|Total\s*excluding\s*VAT)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

RE_VAT_AMOUNT = re.compile(
    r"(?:ภาษีมูลค่าเพิ่ม|VAT)\s*(?:7\s*%|7%|@?\s*7%)?\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Fallback total amount keyword (less ideal)
RE_SPX_TOTAL_AMOUNT = re.compile(
    r"(?:จำนวนเงินรวม|Total\s*amount)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# WHT patterns (3%)
RE_SPX_WHT_TH = re.compile(
    r"หักภาษีเงินได้\s*ณ\s*ที่จ่าย.*?อัตรา(?:ร้อย)?ละ\s*(\d+)\s*%.*?(?:เป็นจำนวนเงิน|จำนวน)\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_SPX_WHT_EN = re.compile(
    r"withholding\s+tax.*?(\d+)\s*%.*?(?:at|=)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s*THB",
    re.IGNORECASE | re.DOTALL,
)

# Heuristic: prevent picking WHT lines as totals
RE_WHT_HINT = re.compile(r"(withholding\s+tax|หักภาษี|ณ\s*ที่จ่าย|wht)", re.IGNORECASE)


# ============================================================
# Helpers
# ============================================================

def _squash_all_ws(s: str) -> str:
    """Remove all whitespace for strict reference rules."""
    if not s:
        return ""
    return re.sub(r"\s+", "", s)

def _clean_ref_code(mmdd: str, seq7: str) -> str:
    return f"{mmdd}-{seq7}"

def _extract_ref_code_anywhere(raw_text: str) -> str:
    t = raw_text or ""
    m = RE_SPX_REF_CODE_FLEX.search(t)
    if not m:
        return ""
    return _clean_ref_code(m.group(1), m.group(2))

def _vendor_code_fallback_for_spx(client_tax_id: str) -> str:
    """
    Hard fallback mapping for SPX (per your client-aware rule examples)
    """
    cid = (client_tax_id or "").strip()
    if cid == "0105565027615":  # TopOne
        return "C00038"
    if cid == "0105563022918":  # SHD
        return "C01133"
    if cid == "0105561071873":  # Rabbit
        return "C00563"
    return "SPX"

def _get_vendor_code_safe(client_tax_id: str, vendor_tax_id: str) -> str:
    """
    Prefer vendor_mapping.get_vendor_code if available; otherwise use hard fallback.
    Must never raise.
    """
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            code = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=vendor_tax_id,
                vendor_name="SPX",
            )
            return code or _vendor_code_fallback_for_spx(client_tax_id)
        except Exception:
            return _vendor_code_fallback_for_spx(client_tax_id)
    return _vendor_code_fallback_for_spx(client_tax_id)

def extract_spx_seller_info(text: str) -> Tuple[str, str]:
    t = normalize_text(text)
    seller_id = ""
    username = ""

    m = RE_SPX_SELLER_ID.search(t)
    if m:
        seller_id = m.group(1)

    m = RE_SPX_USERNAME.search(t)
    if m:
        username = m.group(1)

    if not seller_id or not username:
        info = extract_seller_info(t) or {}
        seller_id = seller_id or (info.get("seller_id") or "")
        username = username or (info.get("username") or "")

    return seller_id, username

def extract_spx_full_reference(text: str, filename: str = "") -> str:
    """
    ✅ Force Full Reference = DOCNO + MMDD-XXXXXXX (NO SPACES)
    Priority:
      1) doc+ref in text (across whitespace/newlines)
      2) doc in text + ref anywhere in text
      3) doc+ref in filename
      4) doc in filename + ref in filename
      5) doc only
      6) ""
    Output MUST have NO spaces.
    """
    t_norm = normalize_text(text or "")
    f_norm = normalize_text(filename or "")

    # Use squashed version to catch cross-line joins aggressively
    t_sq = _squash_all_ws(t_norm)
    f_sq = _squash_all_ws(f_norm)

    # 1) full ref in text
    m = RE_SPX_FULL_REFERENCE.search(t_norm)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _squash_all_ws(f"{doc}{ref}")  # NO SPACES

    # 2) doc in text + ref anywhere (use squashed for robustness)
    m_doc = RE_SPX_DOCNO.search(t_norm)
    doc = m_doc.group(1) if m_doc else ""
    if doc:
        ref = _extract_ref_code_anywhere(t_norm)
        if ref:
            return _squash_all_ws(f"{doc}{ref}")
        return _squash_all_ws(doc)

    # 3) full ref in filename
    m = RE_SPX_FULL_REFERENCE.search(f_norm)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _squash_all_ws(f"{doc}{ref}")

    # 4) doc in filename + ref in filename
    m_doc = RE_SPX_DOCNO.search(f_norm)
    doc = m_doc.group(1) if m_doc else ""
    if doc:
        ref = _extract_ref_code_anywhere(f_norm)
        if ref:
            return _squash_all_ws(f"{doc}{ref}")
        return _squash_all_ws(doc)

    # As a last attempt: search doc/ref in squashed text directly
    m_doc2 = re.search(r"(RCS[A-Z0-9\-/]{8,})", t_sq, flags=re.IGNORECASE)
    m_ref2 = RE_SPX_REF_CODE_FLEX.search(t_sq)
    if m_doc2 and m_ref2:
        doc = m_doc2.group(1)
        ref = _clean_ref_code(m_ref2.group(1), m_ref2.group(2))
        return _squash_all_ws(f"{doc}{ref}")

    return ""

def _safe_float(s: str) -> float:
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return 0.0

def _money(s: str) -> str:
    try:
        return parse_money(s) or ""
    except Exception:
        return ""

def _extract_amounts_spx_strict(t: str) -> Tuple[str, str, str, str, bool]:
    """
    Return (total_ex_vat, vat_amount, total_inc_vat, wht_amount_3pct, has_wht_3pct)
    - Prefer explicit Inc VAT line as PRIMARY
    - Never let WHT amount override totals
    """
    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""
    wht_amount = ""
    has_wht = False

    # --- WHT (separate, never used as totals) ---
    m = RE_SPX_WHT_TH.search(t)
    if m:
        rate = (m.group(1) or "").strip()
        amt = _money(m.group(2))
        if rate == "3" and amt:
            wht_amount = amt
            has_wht = True

    if not has_wht:
        m = RE_SPX_WHT_EN.search(t)
        if m:
            rate = (m.group(1) or "").strip()
            amt = _money(m.group(2))
            if rate == "3" and amt:
                wht_amount = amt
                has_wht = True

    # --- Totals (guard against WHT vicinity) ---
    m = RE_TOTAL_INC_VAT.search(t)
    if m:
        ctx = t[max(0, m.start() - 60): m.end() + 60]
        if not RE_WHT_HINT.search(ctx):
            total_inc_vat = _money(m.group(1))

    m = RE_TOTAL_EX_VAT.search(t)
    if m:
        ctx = t[max(0, m.start() - 60): m.end() + 60]
        if not RE_WHT_HINT.search(ctx):
            total_ex_vat = _money(m.group(1))

    # VAT
    m = RE_VAT_AMOUNT.search(t)
    if m:
        ctx = t[max(0, m.start() - 60): m.end() + 60]
        if not RE_WHT_HINT.search(ctx):
            vat_amount = _money(m.group(1))

    # Fallback "Total amount" if inc-vat missing
    if not total_inc_vat:
        m = RE_SPX_TOTAL_AMOUNT.search(t)
        if m:
            ctx = t[max(0, m.start() - 80): m.end() + 80]
            if not RE_WHT_HINT.search(ctx):
                cand = _money(m.group(1))
                # reject if equals WHT amount (common OCR bug)
                if cand and (not wht_amount or cand != wht_amount):
                    total_inc_vat = cand

    # Derive
    if not total_inc_vat and total_ex_vat and vat_amount:
        v = _safe_float(total_ex_vat) + _safe_float(vat_amount)
        if v > 0:
            total_inc_vat = f"{v:.2f}"

    if not total_ex_vat and total_inc_vat and vat_amount:
        v = _safe_float(total_inc_vat) - _safe_float(vat_amount)
        if v > 0:
            total_ex_vat = f"{v:.2f}"

    # FINAL fallback from common extractor (still guard WHT)
    if not total_inc_vat:
        am = extract_amounts(t) or {}
        cand_total = (am.get("total") or "").strip()
        cand_wht = (am.get("wht_amount") or "").strip()
        if cand_total and cand_total != cand_wht:
            total_inc_vat = cand_total

        if not vat_amount:
            vat_amount = (am.get("vat") or "").strip() or vat_amount
        if not total_ex_vat:
            total_ex_vat = (am.get("subtotal") or "").strip() or total_ex_vat

    return (total_ex_vat, vat_amount, total_inc_vat, wht_amount, has_wht)


# ============================================================
# Main extraction
# ============================================================

def extract_spx(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Extract SPX receipt to PEAK A-U format.
    Safe: must never crash; keep output stable.
    """
    try:
        t = normalize_text(text or "")
        row = base_row_dict()

        # 1) Vendor tax + vendor code
        vendor_tax = find_vendor_tax_id(t, "SPX") or VENDOR_SPX
        row["E_tax_id_13"] = vendor_tax
        row["D_vendor_code"] = _get_vendor_code_safe(client_tax_id, vendor_tax)

        # 2) Branch
        row["F_branch_5"] = find_branch(t) or "00000"

        # 3) Full reference (NO SPACES)
        full_ref = extract_spx_full_reference(t, filename=filename)
        if full_ref:
            row["G_invoice_no"] = full_ref
            row["C_reference"] = full_ref

        # 4) Dates
        date = find_best_date(t) or ""
        if date:
            row["B_doc_date"] = date
            row["H_invoice_date"] = date
            row["I_tax_purchase_date"] = date

        # 5) Amounts strict
        row["M_qty"] = "1"
        total_ex_vat, vat_amount, total_inc_vat, wht_amount_3pct, has_wht_3 = _extract_amounts_spx_strict(t)

        # Primary mapping: inc VAT only
        if total_inc_vat:
            row["N_unit_price"] = total_inc_vat
            row["R_paid_amount"] = total_inc_vat
        else:
            # stable fallback
            row["N_unit_price"] = row.get("N_unit_price") or "0"
            row["R_paid_amount"] = row.get("R_paid_amount") or "0"

        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"

        # 6) WHT mapping
        if WHT_RATE_MODE.upper() == "ALWAYS":
            row["P_wht"] = "3%"
            row["S_pnd"] = "53"
        else:
            # AUTO: set only if real WHT 3% detected
            row["P_wht"] = "3%" if has_wht_3 else "0"
            row["S_pnd"] = "53" if has_wht_3 else ""

        # 7) Payment method
        row["Q_payment_method"] = "หักจากยอดขาย"

        # 8) Description / Group
        row["L_description"] = "Marketplace Expense"
        row["U_group"] = "Marketplace Expense"

        # 9) Notes blank (strict)
        row["T_note"] = ""

        # 10) Safety sync C/G
        if not row.get("C_reference") and row.get("G_invoice_no"):
            row["C_reference"] = row["G_invoice_no"]
        if not row.get("G_invoice_no") and row.get("C_reference"):
            row["G_invoice_no"] = row["C_reference"]

        row["K_account"] = ""

        return format_peak_row(row)

    except Exception:
        # Fail-safe: never crash, stable output
        row = base_row_dict()
        row["D_vendor_code"] = _vendor_code_fallback_for_spx(client_tax_id)
        row["E_tax_id_13"] = VENDOR_SPX
        row["F_branch_5"] = "00000"
        row["M_qty"] = "1"
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["P_wht"] = "0"
        row["S_pnd"] = ""
        row["N_unit_price"] = "0"
        row["R_paid_amount"] = "0"
        row["Q_payment_method"] = "หักจากยอดขาย"
        row["U_group"] = "Marketplace Expense"
        row["L_description"] = "Marketplace Expense"
        row["T_note"] = ""
        row["K_account"] = ""
        return format_peak_row(row)


__all__ = [
    "extract_spx",
    "extract_spx_full_reference",
    "extract_spx_seller_info",
]
