from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List

from .common import (
    base_row_dict,
    detect_platform_vendor,
    find_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    find_totals,
    find_payment_method,     # from your improved common.py
    find_best_reference,     # from your improved common.py
)

from ..utils.text_utils import normalize_text, parse_money, parse_date_to_yyyymmdd


# ------------------------------------------------------------
# Shopee-specific helpers (robust)
# ------------------------------------------------------------

# Shopee TIV / document number (extra strict)
RE_SHOPEE_DOCNO_STRICT = re.compile(
    r"\b(?:Shopee-)?TIV-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}\b",
    re.IGNORECASE
)

# Shopee doc date keywords (try to pick doc date near these)
RE_DOC_DATE_NEAR = re.compile(
    r"(?:วันที่เอกสาร|วันที่ออกเอกสาร|Date\s*of\s*issue|Issue\s*date|Document\s*date)\s*[: ]*"
    r"(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2}|\d{8})",
    re.IGNORECASE
)

# Invoice date keywords
RE_INVOICE_DATE_NEAR = re.compile(
    r"(?:วันที่ใบกำกับ(?:ภาษี)?|Invoice\s*date|Tax\s*Invoice\s*date)\s*[: ]*"
    r"(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2}|\d{8})",
    re.IGNORECASE
)

# Try to summarize fee lines (Thai/EN)
RE_FEE_LINE = re.compile(
    r"^\s*(?:(?:ค่าธรรมเนียม|ค่าบริการ|คอมมิชชั่น|ภาษีมูลค่าเพิ่ม|VAT|Commission|Service\s*fee|Transaction\s*fee|Platform\s*fee)"
    r".{0,80})\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$",
    re.IGNORECASE | re.MULTILINE
)

RE_FEE_LINE_NAME = re.compile(
    r"^\s*((?:ค่าธรรมเนียม|ค่าบริการ|คอมมิชชั่น|ภาษีมูลค่าเพิ่ม|VAT|Commission|Service\s*fee|Transaction\s*fee|Platform\s*fee).{0,80}?)\s+[0-9,]+(?:\.[0-9]{1,2})?\s*$",
    re.IGNORECASE | re.MULTILINE
)


def _pick_doc_date(text: str) -> str:
    """
    Pick best doc date for Shopee:
      1) Date near explicit 'document date' keywords
      2) Date near invoice date keywords
      3) fallback common best date
    """
    t = normalize_text(text)

    m = RE_DOC_DATE_NEAR.search(t)
    if m:
        d = parse_date_to_yyyymmdd(m.group(1)) or ""
        if d:
            return d

    m2 = RE_INVOICE_DATE_NEAR.search(t)
    if m2:
        d = parse_date_to_yyyymmdd(m2.group(1)) or ""
        if d:
            return d

    return find_best_date(t)


def _pick_invoice_no(text: str) -> str:
    """
    Prefer strict Shopee TIV format first, then common invoice finder.
    """
    t = normalize_text(text)
    m = RE_SHOPEE_DOCNO_STRICT.search(t)
    if m:
        return m.group(0).strip()
    # fallback: common (includes Shopee patterns too)
    return find_invoice_no(t)


def _build_fee_description(text: str, max_items: int = 5) -> Tuple[str, str]:
    """
    Returns (short_desc, note_lines)
    - short_desc: "Shopee Fees: A; B; C"
    - note_lines: "- line: amount"
    """
    t = normalize_text(text)
    pairs: List[Tuple[str, str]] = []

    # Collect fee lines with amount
    for m in RE_FEE_LINE.finditer(t):
        amt = parse_money(m.group(1)) or ""
        if not amt:
            continue
        # Try to capture the line name from same line
        # We'll search backwards: best effort by matching the whole line name regex at same position
        # (Simple: re-match line in that span)
        span_start = max(0, m.start() - 140)
        span = t[span_start:m.end() + 5]
        nm = RE_FEE_LINE_NAME.search(span)
        name = (nm.group(1).strip() if nm else "Fee").strip()

        # normalize name a bit
        name = re.sub(r"\s{2,}", " ", name)
        name = name.replace(" :", ":").replace(" : ", ": ")
        pairs.append((name, amt))
        if len(pairs) >= max_items:
            break

    if not pairs:
        return ("", "")

    short_names = [p[0] for p in pairs[:3]]
    short = "Shopee Fees: " + "; ".join(short_names)
    note = "\n".join([f"- {n}: {a}" for (n, a) in pairs])
    return (short, note)


def extract_shopee(text: str) -> Dict[str, Any]:
    """
    Shopee TAX INVOICE / TIV / fee doc → PEAK A–U row (one-row summary)

    Strategy:
    - Vendor: Shopee
    - Tax/branch: prefer Shopee seller tax id & branch (HQ often 00000)
    - Doc date: choose near doc/invoice keywords
    - Amount: N_unit_price=subtotal, R_paid_amount=total_due (fallback compute if possible)
    - WHT: put amount to P_wht, rate into T_note (audit)
    - Description: summarize fee lines if found
    """
    t = normalize_text(text)
    row = base_row_dict()

    vendor_full, platform_key = detect_platform_vendor(t)
    # detect_platform_vendor returns ("Shopee (Thailand) Co., Ltd.", "shopee") in your updated common.py
    if not vendor_full:
        vendor_full = "Shopee (Thailand) Co., Ltd."
        platform_key = "shopee"

    # keep vendor code short & consistent
    row["D_vendor_code"] = "Shopee"
    row["U_group"] = "Marketplace Expense"

    # Tax / branch
    row["E_tax_id_13"] = find_tax_id(t)
    row["F_branch_5"] = find_branch(t) or "00000"

    # Dates
    doc_date = _pick_doc_date(t)
    row["B_doc_date"] = doc_date
    row["H_invoice_date"] = doc_date
    row["I_tax_purchase_date"] = doc_date

    # Invoice / reference
    inv_no = _pick_invoice_no(t)
    best_ref = find_best_reference(t) or inv_no
    if best_ref:
        row["C_reference"] = best_ref
    if inv_no:
        row["G_invoice_no"] = inv_no

    # Payment method (if common can detect; else default for platform)
    pm = find_payment_method(t) or "หักจากยอดขาย"
    row["Q_payment_method"] = pm

    # Totals
    totals = find_totals(t)  # {total_due, subtotal, vat_amt, wht_amt, wht_rate}
    total_due = totals.get("total_due") or ""
    subtotal = totals.get("subtotal") or ""
    vat_amt = totals.get("vat_amt") or ""

    # If total_due missing but subtotal+vat exist, compute.
    if (not total_due) and subtotal and vat_amt:
        try:
            total_due = str(float(subtotal) + float(vat_amt))
        except Exception:
            pass

    # Fill amounts (one-row summary)
    row["M_qty"] = "1"
    if subtotal:
        row["N_unit_price"] = subtotal
    elif total_due:
        # fallback: if subtotal not found, at least set unit price to total
        row["N_unit_price"] = total_due

    if total_due:
        row["R_paid_amount"] = total_due
    elif subtotal:
        row["R_paid_amount"] = subtotal

    # VAT
    row["O_vat_rate"] = "7%"
    row["J_price_type"] = "1"  # split VAT

    # WHT: put amount in P_wht (more compatible with PEAK import),
    # keep rate in note (audit)
    wht_amt = totals.get("wht_amt") or ""
    wht_rate = totals.get("wht_rate") or ""
    if wht_amt:
        row["P_wht"] = wht_amt
        row["S_pnd"] = "53"
        if wht_rate:
            row["T_note"] = (row.get("T_note") or "").strip()
            row["T_note"] = (row["T_note"] + (" | " if row["T_note"] else "") + f"WHT {wht_rate}").strip()
    elif wht_rate:
        # if we only got rate but no amount, keep it in note (don’t poison numeric column)
        row["T_note"] = (row.get("T_note") or "").strip()
        row["T_note"] = (row["T_note"] + (" | " if row["T_note"] else "") + f"WHT {wht_rate}").strip()
        row["S_pnd"] = "53"

    # Description
    short_desc, note = _build_fee_description(t)
    row["L_description"] = short_desc or "Shopee ค่าธรรมเนียม/ภาษี (ตรวจสอบรายละเอียด)"

    # Audit notes (vendor legal, extra fee lines)
    row["T_note"] = (row.get("T_note") or "").strip()
    if vendor_full and vendor_full.lower() != "shopee":
        row["T_note"] = (row["T_note"] + (" | " if row["T_note"] else "") + f"Vendor: {vendor_full}").strip()
    if note:
        row["T_note"] = (row["T_note"] + ("\n" if row["T_note"] else "") + note).strip()

    # Account mapping left blank (your preference)
    row["K_account"] = ""

    return row
