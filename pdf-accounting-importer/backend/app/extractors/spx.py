# -*- coding: utf-8 -*-
# backend/app/extractors/spx.py
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
    extract_amounts,
    format_peak_row,
    parse_money,
)

WHT_RATE_MODE = "AUTO"

try:
    from .vendor_mapping import get_vendor_code, VENDOR_SPX, CLIENT_RABBIT, CLIENT_SHD, CLIENT_TOPONE
    VENDOR_MAPPING_AVAILABLE = True
except Exception:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SPX = "0105561164871"
    CLIENT_RABBIT = "0105561071873"
    CLIENT_SHD    = "0105563022918"
    CLIENT_TOPONE = "0105565027615"


RE_SPX_DOCNO = re.compile(
    r"(?:เลขที่|No\.?)\s*[:#：]?\s*(RCS[A-Z0-9\-/]{8,})",
    re.IGNORECASE,
)
RE_SPX_FULL_REFERENCE = re.compile(
    r"\b(RCS[A-Z0-9\-/]{8,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE,
)
RE_SPX_REF_CODE_FLEX = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")

RE_TOTAL_INC_VAT = re.compile(
    r"(?:รวม\s*ทั้ง\s*สิ้น|Total\s*(?:amount)?\s*\(?(?:including|incl\.?)\s*VAT\)?|Grand\s*Total|จำนวนเงินรวม)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)
RE_TOTAL_EX_VAT = re.compile(
    r"(?:ก่อน\s*ภาษี|Subtotal\s*\(?(?:excluding|excl\.?)\s*VAT\)?|Total\s*excluding\s*VAT)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)
RE_VAT_AMOUNT = re.compile(
    r"(?:ภาษีมูลค่าเพิ่ม|VAT)\s*(?:7\s*%|7%|@?\s*7%)?\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

RE_SPX_WHT_TH = re.compile(
    r"หักภาษีเงินได้\s*ณ\s*ที่จ่าย.*?อัตรา(?:ร้อย)?ละ\s*(\d+)\s*%.*?(?:เป็นจำนวนเงิน|จำนวน)\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_SPX_WHT_EN = re.compile(
    r"withholding\s+tax.*?(\d+)\s*%.*?(?:at|=)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s*THB",
    re.IGNORECASE | re.DOTALL,
)

RE_WHT_HINT = re.compile(r"(withholding\s+tax|หักภาษี|ณ\s*ที่จ่าย|wht)", re.IGNORECASE)


def _squash_all_ws(s: str) -> str:
    return re.sub(r"\s+", "", s or "")

def _clean_ref_code(mmdd: str, seq7: str) -> str:
    return f"{mmdd}-{seq7}"

def _extract_ref_code_anywhere(raw_text: str) -> str:
    t = raw_text or ""
    m = RE_SPX_REF_CODE_FLEX.search(t)
    if not m:
        return ""
    return _clean_ref_code(m.group(1), m.group(2))

def _vendor_code_fallback_for_spx(client_tax_id: str) -> str:
    cid = _squash_all_ws(str(client_tax_id or ""))
    if cid == CLIENT_TOPONE:
        return "C00038"
    if cid == CLIENT_SHD:
        return "C01133"
    if cid == CLIENT_RABBIT:
        return "C00563"
    return "Unknown"

def _get_vendor_code_safe(client_tax_id: str, vendor_tax_id: str) -> str:
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

def extract_spx_full_reference(text: str, filename: str = "") -> str:
    """
    ✅ Force Full Reference = DOCNO + MMDD-XXXXXXX (NO SPACES)
    """
    t_norm = normalize_text(text or "")
    f_norm = normalize_text(filename or "")

    m = RE_SPX_FULL_REFERENCE.search(t_norm)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _squash_all_ws(f"{doc}{ref}")

    m_doc = RE_SPX_DOCNO.search(t_norm)
    doc = m_doc.group(1) if m_doc else ""
    if doc:
        ref = _extract_ref_code_anywhere(t_norm)
        if ref:
            return _squash_all_ws(f"{doc}{ref}")
        return _squash_all_ws(doc)

    # filename fallback
    m = RE_SPX_FULL_REFERENCE.search(f_norm)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _squash_all_ws(f"{doc}{ref}")

    m_doc = RE_SPX_DOCNO.search(f_norm)
    doc = m_doc.group(1) if m_doc else ""
    if doc:
        ref = _extract_ref_code_anywhere(f_norm)
        if ref:
            return _squash_all_ws(f"{doc}{ref}")
        return _squash_all_ws(doc)

    # ultimate fallback on squashed
    t_sq = _squash_all_ws(t_norm)
    m_doc2 = re.search(r"(RCS[A-Z0-9\-/]{8,})", t_sq, flags=re.IGNORECASE)
    m_ref2 = re.search(r"(\d{4})-(\d{7})", t_sq)
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
    Return (total_ex_vat, vat_amount, total_inc_vat, wht_amount, has_wht)
    - กัน WHT ไปทับ total
    """
    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""
    wht_amount = ""
    has_wht = False

    # WHT (separate)
    m = RE_SPX_WHT_TH.search(t)
    if m:
        rate = (m.group(1) or "").strip()
        amt = _money(m.group(2))
        if amt:
            wht_amount = amt
            has_wht = True

    if not has_wht:
        m = RE_SPX_WHT_EN.search(t)
        if m:
            rate = (m.group(1) or "").strip()
            amt = _money(m.group(2))
            if amt:
                wht_amount = amt
                has_wht = True

    # totals
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

    m = RE_VAT_AMOUNT.search(t)
    if m:
        ctx = t[max(0, m.start() - 60): m.end() + 60]
        if not RE_WHT_HINT.search(ctx):
            vat_amount = _money(m.group(1))

    # Derive
    if not total_inc_vat and total_ex_vat and vat_amount:
        v = _safe_float(total_ex_vat) + _safe_float(vat_amount)
        if v > 0:
            total_inc_vat = f"{v:.2f}"
    if not total_ex_vat and total_inc_vat and vat_amount:
        v = _safe_float(total_inc_vat) - _safe_float(vat_amount)
        if v > 0:
            total_ex_vat = f"{v:.2f}"

    # fallback common.extract_amounts
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

def extract_spx(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    ✅ SPX extractor:
    - ส่ง meta seller_id/username ให้ post-process ทำ description template
    """
    try:
        t = normalize_text(text or "")
        row = base_row_dict()

        vendor_tax = find_vendor_tax_id(t, "SPX") or VENDOR_SPX
        row["E_tax_id_13"] = vendor_tax
        row["D_vendor_code"] = _get_vendor_code_safe(client_tax_id, vendor_tax)

        row["F_branch_5"] = find_branch(t) or "00000"

        # full reference (แต่ post-process จะ enforce จาก filename อีกชั้น)
        full_ref = extract_spx_full_reference(t, filename=filename)
        if full_ref:
            row["G_invoice_no"] = full_ref
            row["C_reference"] = full_ref

        date = find_best_date(t) or ""
        if date:
            row["B_doc_date"] = date
            row["H_invoice_date"] = date
            row["I_tax_purchase_date"] = date

        # seller meta
        info = extract_seller_info(t) or {}
        row["_seller_id"] = info.get("seller_id", "") or ""
        row["_username"] = info.get("username", "") or ""

        total_ex_vat, vat_amount, total_inc_vat, wht_amount, has_wht = _extract_amounts_spx_strict(t)

        if total_inc_vat:
            row["N_unit_price"] = total_inc_vat
            row["R_paid_amount"] = total_inc_vat

        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["Q_payment_method"] = "หักจากยอดขาย"

        # ค่า WHT: ในระบบคุณตอนนี้ให้คงว่าง/ไม่บังคับ (post-process ไม่แตะ)
        # ถ้าจะให้โชว์ 1%/3% ค่อยกำหนด policy เพิ่มภายหลัง
        row["P_wht"] = ""
        row["S_pnd"] = ""

        row["U_group"] = "Marketplace Expense"
        row["T_note"] = ""

        # ยังไม่ใส่ K_account + L_description ที่นี่ (post-process จะใส่ให้)
        row["K_account"] = ""
        row["L_description"] = ""

        return row

    except Exception:
        row = base_row_dict()
        row["D_vendor_code"] = _vendor_code_fallback_for_spx(client_tax_id)
        row["E_tax_id_13"] = VENDOR_SPX
        row["F_branch_5"] = "00000"
        row["M_qty"] = "1"
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["P_wht"] = ""
        row["S_pnd"] = ""
        row["N_unit_price"] = "0"
        row["R_paid_amount"] = "0"
        row["Q_payment_method"] = "หักจากยอดขาย"
        row["U_group"] = "Marketplace Expense"
        row["T_note"] = ""
        row["K_account"] = ""
        row["L_description"] = ""
        return row

__all__ = ["extract_spx", "extract_spx_full_reference"]
