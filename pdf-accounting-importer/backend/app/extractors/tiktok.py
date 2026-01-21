# -*- coding: utf-8 -*-
# backend/app/extractors/tiktok.py
from __future__ import annotations

import re
from typing import Any, Dict

from .common import normalize_text, finalize_row

# Prefer vendor code mapping from your source-of-truth mapping module
try:
    from .vendor_mapping import get_vendor_code  # type: ignore
except Exception:
    get_vendor_code = None  # type: ignore


# -------------------------------------------------------------------
# TikTok invoice patterns (best-effort, robust)
# -------------------------------------------------------------------

RE_TTSTH = re.compile(r"\bTTSTH\d{8,}\b", re.IGNORECASE)

RE_VENDOR_TAX_LINE = re.compile(
    r"(tax\s*registration\s*number)\s*[:：\-]?\s*(\d{13})",
    re.IGNORECASE,
)
RE_TAX_ID_13_ANY = re.compile(r"\b\d{13}\b")

RE_BRANCH_5 = re.compile(r"(branch|สาขา)\s*[:\-]?\s*(\d{1,5})", re.IGNORECASE)

RE_DATE_ANY = re.compile(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b")
RE_INVOICE_DATE_LINE = re.compile(r"(invoice\s*date)\s*[:：\-]?\s*(.+)", re.IGNORECASE)
RE_DOC_DATE_LINE = re.compile(r"(document\s*date|วันที่เอกสาร)\s*[:：\-]?\s*(.+)", re.IGNORECASE)
RE_DATE_MON_DD_YYYY = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})\b",
    re.IGNORECASE,
)

RE_MONEY = re.compile(r"(-?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|-?\d+(?:\.\d{1,2})?)")
RE_TOTAL_INCL = re.compile(
    r"(total\s*amount.*including\s*vat|total\s*amount\s*\(including\s*vat\)|amount\s*due|grand\s*total|total\s*amount（including\s*vat）)",
    re.IGNORECASE,
)
RE_TOTAL_VAT = re.compile(r"(total\s*vat|vat\s*amount|value\s*added\s*tax)", re.IGNORECASE)
RE_SUBTOTAL_EXCL = re.compile(
    r"(subtotal.*excluding\s*vat|total.*excluding\s*vat|subtotal\s*\(excluding\s*vat\)|amount\s*in\s*thb\s*\(excluding\s*vat\))",
    re.IGNORECASE,
)

RE_WHT_AMOUNTING = re.compile(
    r"(withheld\s*tax|withholding\s*tax).*?rate\s*of\s*(\d{1,2})\s*%.*?amounting\s*to\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_WHT_GENERIC = re.compile(
    r"(withheld|withholding|wht).*?(\d{1,2})\s*%.*?(?:฿|THB)?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_WHT_HINT = re.compile(r"(withheld\s*tax|withholding\s*tax|wht|ภาษี\s*ณ\s*ที่\s*จ่าย)", re.IGNORECASE)

RE_ADS_HINT = re.compile(r"\b(ads|advertising|promotion|โฆษณา|ค่าโฆษณา)\b", re.IGNORECASE)

RE_ALL_WS = re.compile(r"\s+")

RE_INVOICE_NO_LINE = re.compile(
    r"(invoice\s*(?:no|number)|reference|ref\.?)\s*[:：#\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\_\/]{6,})",
    re.IGNORECASE,
)
RE_REF_TOKEN_1 = re.compile(r"\b([A-Z]{2,}[A-Z0-9]*\d{2,}[A-Z0-9\-]{6,})\b")
RE_REF_TOKEN_2 = re.compile(r"\b(\d{2,4}-\d{5,})\b")

# TikTok vendor tax-id aliasing (ปรับได้ตาม vendor_mapping.py ของคุณ)
TIKTOK_VENDOR_TAX_ALIASES: Dict[str, str] = {
    "0105566214176": "0105566214176",
}


def _clean_digits(s: Any, max_len: int | None = None) -> str:
    if s is None:
        return ""
    out = "".join([c for c in str(s) if c.isdigit()])
    if max_len is not None:
        out = out[:max_len]
    return out


def _compact_ref(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _money_to_str(v: str) -> str:
    if not v:
        return ""
    s = v.strip().replace(",", "").replace("฿", "").replace("THB", "").strip()
    try:
        x = float(s)
        if x < 0:
            return ""
        return f"{x:.2f}"
    except Exception:
        return ""


def _to_yyyymmdd_from_text(s: str) -> str:
    if not s:
        return ""

    m = RE_DATE_ANY.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mo:02d}{d:02d}"

    m2 = RE_DATE_MON_DD_YYYY.search(s)
    if m2:
        mon_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        mon = mon_map.get(m2.group(1).lower(), 0)
        d = int(m2.group(2))
        y = int(m2.group(3))
        if 1 <= mon <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mon:02d}{d:02d}"

    return ""


def _find_amount_near_keyword_excluding(text: str, keyword_re: re.Pattern, window: int = 240) -> str:
    if not text:
        return ""
    m = keyword_re.search(text)
    if not m:
        return ""

    start = max(0, m.start() - 80)
    end = min(len(text), m.end() + window)
    chunk = text[start:end]

    w = RE_WHT_HINT.search(chunk)
    if w:
        chunk = chunk[: w.start()]

    nums = RE_MONEY.findall(chunk)
    if not nums:
        return ""
    return _money_to_str(nums[-1])


def _extract_wht_amount_3pct(text: str) -> str:
    if not text:
        return ""

    m = RE_WHT_AMOUNTING.search(text)
    if m:
        rate = _clean_digits(m.group(2), 2)
        amt = _money_to_str(m.group(3))
        if rate == "3" and amt:
            return amt

    m2 = RE_WHT_GENERIC.search(text)
    if m2:
        rate = _clean_digits(m2.group(2), 2)
        amt = _money_to_str(m2.group(3))
        if rate == "3" and amt:
            return amt

    return ""


def _blank_row() -> Dict[str, Any]:
    return {
        "B_doc_date": "",
        "C_reference": "",
        "D_vendor_code": "Unknown",
        "E_tax_id_13": "",
        "F_branch_5": "00000",
        "G_invoice_no": "",
        "H_invoice_date": "",
        "I_tax_purchase_date": "",
        "J_price_type": "1",
        "K_account": "",
        "L_description": "",
        "M_qty": "1",
        "N_unit_price": "0",
        "O_vat_rate": "7%",
        "P_wht": "",          # ✅ TikTok: blank always
        "Q_payment_method": "",
        "R_paid_amount": "0",
        "S_pnd": "",
        "T_note": "",
        "U_group": "Marketplace Expense",
    }


def _extract_reference_invoice_glued(t: str) -> str:
    if not t:
        return ""

    m = RE_TTSTH.search(t)
    if m:
        return _compact_ref(m.group(0))

    m2 = RE_INVOICE_NO_LINE.search(t)
    if m2:
        return _compact_ref(m2.group(2))

    m3 = RE_REF_TOKEN_1.search(t)
    if m3:
        start = m3.end()
        window = t[start: min(len(t), start + 120)]
        m4 = RE_REF_TOKEN_2.search(window)
        if m4:
            return _compact_ref(m3.group(1) + m4.group(1))
        return _compact_ref(m3.group(1))

    return ""


def _alias_vendor_tax_id(vendor_tax_id: str) -> str:
    v = _clean_digits(vendor_tax_id, 13)
    if not v:
        return ""
    return TIKTOK_VENDOR_TAX_ALIASES.get(v, v)


def extract_tiktok(text: str, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    ✅ Updated signature: (text, filename, client_tax_id)
    ✅ Always ends with finalize_row() => post-process กลาง
    """
    t = normalize_text(text or "")
    row = _blank_row()

    if not t.strip():
        return finalize_row(row, text=t, filename=filename, client_tax_id=client_tax_id, platform="TikTok")

    # --- Reference / invoice no (glued) ---
    ref = _extract_reference_invoice_glued(t)
    if ref:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref

    # --- Vendor Tax ID ---
    vendor_tax = ""
    m_vendor = RE_VENDOR_TAX_LINE.search(t)
    if m_vendor:
        vendor_tax = _clean_digits(m_vendor.group(2), 13)

    ctax = _clean_digits(client_tax_id, 13) if client_tax_id else ""
    if not vendor_tax:
        all_tax = RE_TAX_ID_13_ANY.findall(t)
        for x in all_tax:
            x13 = _clean_digits(x, 13)
            if ctax and x13 == ctax:
                continue
            if x13:
                vendor_tax = x13
                break

    vendor_tax = _alias_vendor_tax_id(vendor_tax)
    row["E_tax_id_13"] = vendor_tax

    # --- Branch ---
    m_br = RE_BRANCH_5.search(t)
    if m_br:
        br = _clean_digits(m_br.group(2), 5)
        row["F_branch_5"] = br.zfill(5) if br else "00000"
    else:
        row["F_branch_5"] = "00000"

    # --- Dates ---
    inv_date = ""
    m_inv = RE_INVOICE_DATE_LINE.search(t)
    if m_inv:
        inv_date = _to_yyyymmdd_from_text(m_inv.group(2))
    if not inv_date:
        m_doc = RE_DOC_DATE_LINE.search(t)
        if m_doc:
            inv_date = _to_yyyymmdd_from_text(m_doc.group(2))
    if not inv_date:
        inv_date = _to_yyyymmdd_from_text(t)

    row["H_invoice_date"] = inv_date
    row["B_doc_date"] = inv_date
    row["I_tax_purchase_date"] = inv_date

    # --- Amount separation (never use WHT) ---
    total_ex_vat = _find_amount_near_keyword_excluding(t, RE_SUBTOTAL_EXCL)
    vat_amount = _find_amount_near_keyword_excluding(t, RE_TOTAL_VAT)
    total_inc_vat = _find_amount_near_keyword_excluding(t, RE_TOTAL_INCL)

    _wht_amount_3pct = _extract_wht_amount_3pct(t)
    _ = _wht_amount_3pct  # reserved for future internal use

    if (not total_inc_vat) and total_ex_vat and vat_amount:
        try:
            total_inc_vat = f"{(float(total_ex_vat) + float(vat_amount)):.2f}"
        except Exception:
            pass
    if not total_inc_vat and total_ex_vat:
        total_inc_vat = total_ex_vat

    if total_inc_vat:
        row["N_unit_price"] = total_inc_vat
        row["R_paid_amount"] = total_inc_vat

    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # TikTok: P_wht must be blank
    row["P_wht"] = ""

    # --- Vendor Code (Cxxxxx) ---
    if callable(get_vendor_code):
        code = get_vendor_code(client_tax_id, vendor_tax_id=vendor_tax, vendor_name="tiktok")
        if isinstance(code, str) and re.match(r"^C\d{5}$", code.strip(), re.IGNORECASE):
            row["D_vendor_code"] = code.strip().upper()
        else:
            row["D_vendor_code"] = "Unknown"
    else:
        row["D_vendor_code"] = "Unknown"

    # --- Group hint (final will be decided by post-process using ads hint too) ---
    row["U_group"] = "Advertising Expense" if RE_ADS_HINT.search(t) else "Marketplace Expense"

    # keep description empty => let post-process template fill
    row["L_description"] = ""

    row["T_note"] = ""

    # hard compact C/G
    row["C_reference"] = _compact_ref(row.get("C_reference"))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no"))
    if not row["C_reference"] and row["G_invoice_no"]:
        row["C_reference"] = row["G_invoice_no"]
    if not row["G_invoice_no"] and row["C_reference"]:
        row["G_invoice_no"] = row["C_reference"]

    # ✅ FINALIZE (post-process กลาง): enforce filename ref + account/desc + numeric correctness
    return finalize_row(row, text=t, filename=filename, client_tax_id=client_tax_id, platform="TikTok")
