# backend/app/extractors/shopee.py
from __future__ import annotations

import os
import re
from typing import Any, Dict, Tuple

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
        CLIENT_RABBIT,
        CLIENT_SHD,
        CLIENT_TOPONE,
    )

    VENDOR_MAPPING_AVAILABLE = True
except Exception:  # pragma: no cover
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_SHOPEE = "0105558019581"
    CLIENT_RABBIT = "0105561071873"
    CLIENT_SHD = "0105563022918"
    CLIENT_TOPONE = "0105565027615"

# ============================================================
# Shopee-specific patterns
# ============================================================

# TIxx format (e.g. Shopee-TIV-TRSPEMKP00-00000-251203-0012589)
RE_SHOPEE_DOC_TI_FORMAT = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,})\b",
    re.IGNORECASE,
)

# TRS doc token (e.g. TRSPEMKP00-00000-25  + next line 1203-0012589)
RE_SHOPEE_DOC_TRS_FORMAT = re.compile(
    r"\b(TRS[A-Z0-9\-/]{10,})\b",
    re.IGNORECASE,
)

RE_SHOPEE_DOC_STRICT = re.compile(
    r"\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-/]{10,})\b",
    re.IGNORECASE,
)

# Reference code: MMDD-XXXXXXX
RE_SHOPEE_REFERENCE_CODE_FLEX = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")

# In-text glue: TRS... + whitespace + MMDD-XXXXXXX
RE_SHOPEE_FULL_REFERENCE = re.compile(
    r"\b(TRS[A-Z0-9\-/]{10,})\s+(\d{4})\s*-\s*(\d{7})\b",
    re.IGNORECASE,
)

# Dates
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

# Seller
RE_SHOPEE_SELLER_ID = re.compile(
    r"(?:Seller\s*ID|Shop\s*ID|รหัสร้านค้า)\s*[:#：]?\s*([0-9]{5,20})",
    re.IGNORECASE,
)
RE_SHOPEE_USERNAME = re.compile(
    r"(?:Username|Shop\s*name|User\s*name|ชื่อผู้ใช้|ชื่อร้าน)\s*[:#：]?\s*([A-Za-z0-9_\-\.]{3,50})",
    re.IGNORECASE,
)

# WHT detection only (P_wht must be blank always)
RE_SHOPEE_WHT_THAI = re.compile(
    r"(?:หัก|ภาษี).*?ที่จ่าย.*?(?:อัตรา|ร้อยละ)\s*([0-9]{1,2})\s*%.*?(?:จำนวน|เป็นเงิน)\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_SHOPEE_WHT_EN = re.compile(
    r"withholding\s+tax.*?(\d{1,2})\s*%.*?(?:at|=)\s*([0-9,]+(?:\.[0-9]{2})?)\s*THB",
    re.IGNORECASE | re.DOTALL,
)

# Summary totals
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
RE_SUM_EXCL_AFTER_DISCOUNT = re.compile(
    r"Excluded\s*VAT\)\s*after\s*discount\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE,
)

RE_ALL_WS = re.compile(r"\s+")

# ============================================================
# Description / account template (override by ENV if you want)
# ============================================================

# You can set these in Render/Railway/.env to avoid hardcoding wrong account codes.
# Example:
#   PEAK_ACCOUNT_MARKETPLACE=610200
#   PEAK_DESC_TEMPLATE_SHOPEE=Shopee Marketplace Fee {ref}
DEFAULT_ACCOUNT_BY_GROUP: Dict[str, str] = {
    "Marketplace Expense": os.getenv("PEAK_ACCOUNT_MARKETPLACE", "").strip(),
    "Advertising Expense": os.getenv("PEAK_ACCOUNT_ADS", "").strip(),
    "Selling Expense": os.getenv("PEAK_ACCOUNT_SELLING", "").strip(),
    "COGS": os.getenv("PEAK_ACCOUNT_COGS", "").strip(),
}

DEFAULT_DESC_TEMPLATE_BY_PLATFORM: Dict[str, str] = {
    "shopee": os.getenv("PEAK_DESC_TEMPLATE_SHOPEE", "Shopee Marketplace Fee {ref}").strip(),
}


# ============================================================
# Helpers
# ============================================================

def _money(v: str) -> str:
    try:
        return parse_money(v)
    except Exception:
        return ""


def _digits_only(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())


def _squash_all_ws(s: str) -> str:
    return re.sub(r"\s+", "", s or "")


def _compact_ref(v: Any) -> str:
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


def _vendor_code_fallback_for_shopee(client_tax_id: str) -> str:
    cid = _digits_only(client_tax_id)
    if cid == CLIENT_RABBIT:
        return "C00395"
    if cid == CLIENT_SHD:
        return "C00888"
    if cid == CLIENT_TOPONE:
        return "C00020"
    return "Unknown"


def _get_vendor_code_safe(client_tax_id: str, vendor_tax_id: str) -> str:
    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            code = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=vendor_tax_id,
                vendor_name="Shopee",
            )
            return code or _vendor_code_fallback_for_shopee(client_tax_id)
        except Exception:
            return _vendor_code_fallback_for_shopee(client_tax_id)
    return _vendor_code_fallback_for_shopee(client_tax_id)


def _infer_shopee_reference_from_filename(filename: str) -> str:
    """
    Enforce C_reference/G_invoice_no from filename when possible.

    Supported:
      - Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf  -> that whole token
      - TRSPEMKP00-00000-25 + 1203-0012589 (sometimes in text) is handled elsewhere
    """
    fn = normalize_text(filename or "")
    if not fn:
        return ""

    # prefer TIxx full token in filename
    m = RE_SHOPEE_DOC_TI_FORMAT.search(fn)
    if m:
        return _compact_ref(m.group(1))

    # if filename contains full TRS+yyyymmdd-seq (common in your naming)
    # e.g. TRSPEMKP00-00000-251203-0012589 (NOT MMDD-XXXXXXX, but yymmdd-XXXXXXX)
    m2 = re.search(r"\b(TRS[A-Z0-9\-/]{10,}-\d{5}-\d{6}-\d{7,})\b", fn, flags=re.IGNORECASE)
    if m2:
        return _compact_ref(m2.group(1))

    # last resort: any strict doc token
    m3 = RE_SHOPEE_DOC_STRICT.search(fn)
    if m3:
        return _compact_ref(m3.group(1))

    return ""


# ============================================================
# Seller ID helpers
# ============================================================

def extract_seller_id_shopee(text: str) -> Tuple[str, str]:
    t = normalize_text(text)
    seller_id = ""
    username = ""

    m = RE_SHOPEE_SELLER_ID.search(t)
    if m:
        seller_id = _digits_only(m.group(1))

    m = RE_SHOPEE_USERNAME.search(t)
    if m:
        username = m.group(1).strip()

    if not seller_id:
        seller_info = extract_seller_info(t) or {}
        seller_id = _digits_only(seller_info.get("seller_id", "") or "")
        if not username:
            username = (seller_info.get("username", "") or "").strip()

    return seller_id, username


# ============================================================
# WHT extraction (detection only; P_wht must stay blank)
# ============================================================

def extract_wht_from_shopee_text(text: str) -> Tuple[str, str]:
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
# Reference extraction (NO whitespace allowed; handle newline split)
# ============================================================

def extract_shopee_full_reference(text: str, filename: str = "") -> str:
    """
    FULL reference (compact; no whitespace):
      - TRS + MMDD-XXXXXXX (glue)
      - or TIxx token
    Handles real case:
      "No. TRSPEMKP00-00000-25" then next line "1203-0012589" -> glue.
    """
    t = normalize_text(text or "")
    fn = normalize_text(filename or "")

    # 1) direct pattern (with whitespace)
    m = RE_SHOPEE_FULL_REFERENCE.search(t)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _compact_ref(f"{doc}{ref}")

    # 1.1) aggressive: squash whitespace and match TRS + 1215-0011632 even if newline split
    t_sq = _squash_all_ws(t)
    m_sq = re.search(r"(TRS[A-Z0-9\-/]{10,})(\d{4})-(\d{7})", t_sq, flags=re.IGNORECASE)
    if m_sq:
        doc = m_sq.group(1)
        ref = _clean_ref_code(m_sq.group(2), m_sq.group(3))
        return _compact_ref(f"{doc}{ref}")

    # 2) TIxx full token
    m_doc = RE_SHOPEE_DOC_TI_FORMAT.search(t)
    if m_doc:
        return _compact_ref(m_doc.group(1))

    # 3) TRS doc + ref anywhere
    m_doc = RE_SHOPEE_DOC_TRS_FORMAT.search(t)
    doc_no = m_doc.group(1) if m_doc else ""
    if doc_no:
        ref = _extract_ref_code_anywhere(t)
        if ref:
            return _compact_ref(f"{doc_no}{ref}")
        return _compact_ref(doc_no)

    # 4) strict doc
    m = RE_SHOPEE_DOC_STRICT.search(t)
    if m:
        doc = m.group(1)
        if doc.upper().startswith("TRS"):
            ref = _extract_ref_code_anywhere(t)
            if ref:
                return _compact_ref(f"{doc}{ref}")
        return _compact_ref(doc)

    # -------- filename fallback --------
    enforced = _infer_shopee_reference_from_filename(fn)
    if enforced:
        return _compact_ref(enforced)

    m = RE_SHOPEE_FULL_REFERENCE.search(fn)
    if m:
        doc = m.group(1)
        ref = _clean_ref_code(m.group(2), m.group(3))
        return _compact_ref(f"{doc}{ref}")

    fn_sq = _squash_all_ws(fn)
    m_sq = re.search(r"(TRS[A-Z0-9\-/]{10,})(\d{4})-(\d{7})", fn_sq, flags=re.IGNORECASE)
    if m_sq:
        doc = m_sq.group(1)
        ref = _clean_ref_code(m_sq.group(2), m_sq.group(3))
        return _compact_ref(f"{doc}{ref}")

    m_doc = RE_SHOPEE_DOC_TRS_FORMAT.search(fn)
    if m_doc:
        doc = m_doc.group(1)
        ref = _extract_ref_code_anywhere(fn)
        if ref:
            return _compact_ref(f"{doc}{ref}")
        return _compact_ref(doc)

    m_doc = RE_SHOPEE_DOC_TI_FORMAT.search(fn)
    if m_doc:
        return _compact_ref(m_doc.group(1))

    return ""


# ============================================================
# Amount extraction (summary-first)
# ============================================================

def extract_amounts_shopee_summary(text: str) -> Dict[str, str]:
    t = normalize_text(text or "")

    subtotal = ""
    vat = ""
    total = ""

    # subtotal
    m = RE_SUM_EXCL.search(t)
    if m:
        subtotal = _money(m.group(1))

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

    # withholding detection only (do NOT write to P_wht)
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
# Post-process (shared contract you asked for)
#   ✅ enforce C/G from filename (highest priority)
#   ✅ compact refs (no whitespace)
#   ✅ apply K_account from group mapping (env-driven)
#   ✅ apply description template (env-driven)
# ============================================================

def post_process_peak_row(
    row: Dict[str, Any],
    *,
    filename: str = "",
    client_tax_id: str = "",
    platform: str = "shopee",
) -> Dict[str, Any]:
    # 1) enforce C/G from filename (highest priority)
    enforced_ref = _infer_shopee_reference_from_filename(filename)
    if enforced_ref:
        row["C_reference"] = enforced_ref
        row["G_invoice_no"] = enforced_ref

    # 2) compact (no whitespace)
    row["C_reference"] = _compact_ref(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no", ""))

    # 3) sync fallback between C and G
    if not row.get("C_reference") and row.get("G_invoice_no"):
        row["C_reference"] = row["G_invoice_no"]
    if not row.get("G_invoice_no") and row.get("C_reference"):
        row["G_invoice_no"] = row["C_reference"]

    # 4) apply K_account (env-driven; based on group)
    grp = (row.get("U_group") or row.get("L_description") or "").strip()
    if not row.get("K_account"):
        acct = DEFAULT_ACCOUNT_BY_GROUP.get(grp, "")
        if acct:
            row["K_account"] = acct

    # 5) apply description template (env-driven)
    # Keep your current default labels but allow richer description.
    ref = row.get("C_reference") or row.get("G_invoice_no") or ""
    if not row.get("L_description"):
        row["L_description"] = "Marketplace Expense"
    if not row.get("U_group"):
        row["U_group"] = row["L_description"]

    tpl = DEFAULT_DESC_TEMPLATE_BY_PLATFORM.get(platform.lower(), "").strip()
    if tpl and row.get("L_description"):
        # Put the template into L_description only if it looks like a "generic" label
        # (so you can still override per-file from extractors later).
        if row["L_description"] in ("Marketplace Expense", "Advertising Expense", "Selling Expense", "COGS"):
            row["L_description"] = tpl.format(ref=ref).strip()

    # 6) final hard rules
    row["P_wht"] = ""          # ALWAYS blank
    row["T_note"] = ""         # ALWAYS blank

    return row


# ============================================================
# Main extraction function
#   ✅ signature: (text, client_tax_id, filename)
#   ✅ calls post_process_peak_row()
#   ✅ number policy: N & R = total(incl VAT) as your latest requirement
# ============================================================

def extract_shopee(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Extract Shopee receipt/tax invoice to PEAK A-U.

    Enforced (per your plan):
      1) Extractors accept + pass through filename + client_tax_id
      2) Finish with post_process_peak_row() (enforce C/G from filename, K_account, description template)
      3) Numbers are consistent:
         - N_unit_price = Total Included VAT (main_total)
         - R_paid_amount = Total Included VAT (main_total)
      4) P_wht must stay blank ALWAYS (WHT only used for S_pnd detection)
    """
    t = normalize_text(text)
    row = base_row_dict()

    # Vendor tax + vendor code
    vendor_tax = find_vendor_tax_id(t, "Shopee") or VENDOR_SHOPEE
    row["E_tax_id_13"] = vendor_tax
    row["D_vendor_code"] = _get_vendor_code_safe(client_tax_id, vendor_tax)

    # Branch (Shopee invoices often "Head Office"; you want 00000 for unknown)
    row["F_branch_5"] = find_branch(t) or "00000"

    # Full reference (glued, no whitespace) from text or filename
    full_ref = extract_shopee_full_reference(t, filename=filename)
    if full_ref:
        full_ref = _compact_ref(full_ref)
        row["G_invoice_no"] = full_ref
        row["C_reference"] = full_ref

    # Dates
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

    # Amounts (summary first; fallback later)
    sums = extract_amounts_shopee_summary(t)
    subtotal = sums.get("subtotal", "")
    vat = sums.get("vat", "")
    total = sums.get("total", "")
    wht_amount = sums.get("wht_amount", "")  # detection only

    if not (subtotal or vat or total):
        amounts = extract_amounts(t) or {}
        subtotal = subtotal or (amounts.get("subtotal", "") or "")
        vat = vat or (amounts.get("vat", "") or "")
        total = total or (amounts.get("total", "") or "")
        wht_amount = wht_amount or (amounts.get("wht_amount", "") or "")
        if not wht_amount:
            _wr, wa = extract_wht_from_shopee_text(t)
            if wa:
                wht_amount = wa

    # ----------------------------
    # PEAK mapping policy (numbers)
    # ----------------------------
    # You asked: "การคำนวณตัวเลข ให้ถูกทุกไฟล์" + finish by post-process.
    # For Shopee TIV/TRS invoices: safest is to map Total (Included VAT) as the main expense total.
    row["M_qty"] = "1"
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    main_total = total or ""
    if (not main_total) and subtotal and vat:
        try:
            main_total = f"{(float(subtotal) + float(vat)):.2f}"
        except Exception:
            main_total = ""

    if main_total:
        row["N_unit_price"] = main_total
        row["R_paid_amount"] = main_total
    elif subtotal:
        row["N_unit_price"] = subtotal
        row["R_paid_amount"] = subtotal
    else:
        row["N_unit_price"] = "0"
        row["R_paid_amount"] = "0"

    # WHT policy
    row["P_wht"] = ""  # ALWAYS blank
    row["S_pnd"] = "53" if wht_amount else ""

    # Payment method (wallet mapping can override later in job_worker)
    row["Q_payment_method"] = "หักจากยอดขาย"

    # Default group/desc (post_process will template it if ENV is set)
    row["L_description"] = "Marketplace Expense"
    row["U_group"] = "Marketplace Expense"
    row["T_note"] = ""

    # ----------------------------
    # ✅ Required finishing step
    # ----------------------------
    row = post_process_peak_row(
        row,
        filename=filename,
        client_tax_id=client_tax_id,
        platform="shopee",
    )

    return format_peak_row(row)


__all__ = [
    "extract_shopee",
    "post_process_peak_row",
    "extract_shopee_full_reference",
    "extract_seller_id_shopee",
    "extract_wht_from_shopee_text",
    "extract_amounts_shopee_summary",
]
