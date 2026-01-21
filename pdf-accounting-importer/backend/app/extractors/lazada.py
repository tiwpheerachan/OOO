# backend/app/extractors/lazada.py
"""
Lazada extractor - PEAK A-U format (HARDENED, filename+client_tax_id + post-process)

Fix goals (same class of bugs as TikTok/SPX/Shopee):
  ✅ NEVER let WHT amount overwrite unit_price / paid_amount
  ✅ Separate amounts clearly:
     - total_ex_vat
     - vat_amount
     - total_inc_vat  (PRIMARY)
     - wht_amount_3pct (internal detection only when policy says P_wht blank)
  ✅ Mapping (strict rule):
     row["N_unit_price"]  = total_inc_vat
     row["R_paid_amount"] = total_inc_vat
     row["O_vat_rate"]    = "7%"
  ✅ Prefer totals block lines:
       Total
       7% (VAT)
       Total (Including Tax)
  ✅ Reference/invoice must have NO spaces even across lines
  ✅ T_note MUST be empty
  ✅ Must be resilient on unknown templates / OCR noise / multi-page text

Plan alignment:
  1) ✅ extractor accepts (text, client_tax_id, filename)
  2) ✅ enforce C/G from filename at end (if filename has ref)
  3) ✅ call post-process (optional) for:
        - K_account mapping
        - description template
        - any other global rules
"""

from __future__ import annotations

import re
from typing import Dict, Any, Tuple, Optional

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_date_to_yyyymmdd,
    extract_amounts,          # fallback only
    format_peak_row,
    parse_money,
)

# ========================================
# POLICY FLAGS
# ========================================
# "EMPTY" = P_wht always "" (blank policy)  ✅ recommended
# "RATE"  = P_wht "3%" if WHT exists else ""
WHT_MODE = "EMPTY"

# ========================================
# Optional post-process hook
# ========================================
# You may already have a module like:
#   backend/app/extractors/post_process.py
# with a function:
#   post_process_peak_row(row, platform, filename, client_tax_id, text)
try:
    from .post_process import post_process_peak_row  # type: ignore
except Exception:  # pragma: no cover
    post_process_peak_row = None  # type: ignore

# ========================================
# Import vendor mapping (with fallback)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_LAZADA,
    )
    VENDOR_MAPPING_AVAILABLE = True
except Exception:  # pragma: no cover
    VENDOR_MAPPING_AVAILABLE = False
    get_vendor_code = None  # type: ignore
    VENDOR_LAZADA = "0105555040244"  # Lazada Tax ID (vendor)


# ============================================================
# Lazada-specific patterns (hardened)
# ============================================================

RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")

# invoice no
RE_LAZADA_INVOICE_NO_FIELD = re.compile(
    r"(?:Invoice\s*No\.?|Invoice\s*Number|Tax\s*Invoice\s*No\.?)\s*[:#：]?\s*([A-Z0-9\-/]{8,60})",
    re.IGNORECASE,
)
RE_LAZADA_DOC_THMPTI = re.compile(r"\b(THMPTI\d{16,})\b", re.IGNORECASE)

# date
RE_LAZADA_INVOICE_DATE = re.compile(
    r"(?:Invoice\s*Date|Document\s*Date|Issue\s*Date)\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})",
    re.IGNORECASE,
)

# Totals block (STRICT)
RE_LAZADA_SUBTOTAL_TOTAL = re.compile(
    r"^\s*Total\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_VAT_7 = re.compile(
    r"^\s*(?:7%\s*\(VAT\)|VAT\s*7%|7%\s*VAT)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_TOTAL_INC = re.compile(
    r"^\s*Total\s*\(Including\s*Tax\)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)

# Inline totals fallback
RE_LAZADA_TOTAL_INC_INLINE = re.compile(
    r"(?:Total\s*\(Including\s*Tax\)|Grand\s*Total|Amount\s*Due)\s*[:#：]?\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE
)
RE_LAZADA_SUBTOTAL_INLINE = re.compile(
    r"(?:Subtotal|Total\s*excluding\s*VAT|Total\s*\(Excluding\s*Tax\))\s*[:#：]?\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE
)
RE_LAZADA_VAT_INLINE = re.compile(
    r"(?:VAT|ภาษีมูลค่าเพิ่ม)\s*(?:7%|@?\s*7%)?\s*[:#：]?\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE
)

# WHT (detection only)
RE_LAZADA_WHT_TEXT = re.compile(
    r"หักภาษีณ?\s*ที่จ่าย.*?อัตรา(?:ร้อยละ)?\s*(\d{1,2})\s*%.*?(?:เป็นจำนวน|จำนวน)\s*([0-9,]+(?:\.[0-9]{2})?)\s*บาท",
    re.IGNORECASE | re.DOTALL
)
RE_LAZADA_WHT_EN = re.compile(
    r"(?:withholding|withheld)\s+tax.*?(\d{1,2})\s*%.*?(?:amounting\s*to|at|=)\s*([0-9,]+(?:\.[0-9]{2})?)",
    re.IGNORECASE | re.DOTALL
)

RE_ALL_WS = re.compile(r"\s+")


# ============================================================
# Helpers
# ============================================================

def _safe_money(v: str) -> str:
    try:
        return parse_money(v) or ""
    except Exception:
        return ""

def _safe_float(x: str) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0

def _squash_ws(s: str) -> str:
    """Remove ALL whitespace (space/tab/newline)."""
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)

def _digits_only(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _pick_client_tax_id(text: str) -> str:
    """
    Best-effort: pick a 13-digit tax id that is NOT vendor tax id.
    """
    t = normalize_text(text or "")
    for m in RE_TAX_ID_13.finditer(t):
        tax = m.group(1)
        if tax and tax != VENDOR_LAZADA:
            return tax
    return ""

def _get_vendor_code_safe(client_tax_id: str, vendor_tax_id: str) -> str:
    """
    Prefer vendor_mapping.get_vendor_code if available; otherwise fallback label 'Lazada'.
    Must never raise.
    """
    if VENDOR_MAPPING_AVAILABLE and client_tax_id and callable(get_vendor_code):
        try:
            code = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=vendor_tax_id,
                vendor_name="Lazada",
            )
            # accept only Cxxxxx as vendor code; else fallback "Lazada"
            if isinstance(code, str) and re.match(r"^C\d{5}$", code.strip(), re.IGNORECASE):
                return code.strip().upper()
            return "Lazada"
        except Exception:
            return "Lazada"
    return "Lazada"

def extract_wht_from_text(text: str) -> Tuple[str, str]:
    """Returns (rate, amount) like ('3%', '3219.71') - detection only."""
    t = normalize_text(text or "")

    m = RE_LAZADA_WHT_TEXT.search(t)
    if m:
        rate = f"{(m.group(1) or '').strip()}%"
        amt = _safe_money(m.group(2))
        return (rate, amt)

    m2 = RE_LAZADA_WHT_EN.search(t)
    if m2:
        rate = f"{(m2.group(1) or '').strip()}%"
        amt = _safe_money(m2.group(2))
        return (rate, amt)

    return ("", "")

def extract_totals_block(text: str) -> Tuple[str, str, str]:
    """
    Extract (total_ex_vat, vat_amount, total_inc_vat)
    - Strongest: totals block lines (multiline exact)
    - Fallback: inline forms
    """
    t = normalize_text(text or "")

    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""

    # strict multiline (best)
    m = RE_LAZADA_SUBTOTAL_TOTAL.search(t)
    if m:
        total_ex_vat = _safe_money(m.group(1))

    m = RE_LAZADA_VAT_7.search(t)
    if m:
        vat_amount = _safe_money(m.group(1))

    m = RE_LAZADA_TOTAL_INC.search(t)
    if m:
        total_inc_vat = _safe_money(m.group(1))

    # inline fallback
    if not total_inc_vat:
        m = RE_LAZADA_TOTAL_INC_INLINE.search(t)
        if m:
            total_inc_vat = _safe_money(m.group(1))

    if not total_ex_vat:
        m = RE_LAZADA_SUBTOTAL_INLINE.search(t)
        if m:
            total_ex_vat = _safe_money(m.group(1))

    if not vat_amount:
        m = RE_LAZADA_VAT_INLINE.search(t)
        if m:
            vat_amount = _safe_money(m.group(1))

    return (total_ex_vat, vat_amount, total_inc_vat)

def _derive_total_inc_vat(total_ex_vat: str, vat_amount: str) -> str:
    if not total_ex_vat or not vat_amount:
        return ""
    v = _safe_float(total_ex_vat) + _safe_float(vat_amount)
    if v <= 0:
        return ""
    return f"{v:.2f}"

def _build_reference_no_space(text: str, filename: str = "") -> str:
    """
    Lazada primary reference = THMPTI... token or Invoice No field.
    MUST have NO spaces/newlines (squash).
    """
    t = normalize_text(text or "")
    fn = normalize_text(filename or "")

    # 1) THMPTI token anywhere (squashed)
    m = RE_LAZADA_DOC_THMPTI.search(_squash_ws(t))
    if m:
        return _squash_ws(m.group(1))

    # 2) Invoice No field
    m = RE_LAZADA_INVOICE_NO_FIELD.search(t)
    if m:
        return _squash_ws(m.group(1).strip())

    # 3) filename fallback
    m = RE_LAZADA_DOC_THMPTI.search(_squash_ws(fn))
    if m:
        return _squash_ws(m.group(1))

    m = RE_LAZADA_INVOICE_NO_FIELD.search(fn)
    if m:
        return _squash_ws(m.group(1).strip())

    # 4) common finder fallback
    inv = find_invoice_no(t, "Lazada") or ""
    if inv:
        return _squash_ws(inv)

    return ""

def _enforce_ref_from_filename(row: Dict[str, Any], filename: str) -> None:
    """
    Plan C: enforce C/G from filename at the end.
    If filename contains THMPTI... or an invoice token, overwrite C_reference & G_invoice_no.
    """
    if not filename:
        return
    ref = _build_reference_no_space("", filename=filename)
    if ref:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref


# ============================================================
# Main
# ============================================================

def extract_lazada(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Strict mapping:
      N_unit_price  = total_inc_vat
      R_paid_amount = total_inc_vat
      O_vat_rate    = "7%"

    Guard rails:
      - NEVER map WHT amount into totals
      - P_wht policy:
          WHT_MODE="EMPTY" -> always ""
          WHT_MODE="RATE"  -> "3%" if WHT exists else ""
      - C_reference / G_invoice_no must be identical & NO whitespace
      - T_note must be empty

    Plan assumptions:
      - filename passed in from job_worker / extract_service
      - post_process_peak_row() (if exists) will apply:
          * K_account mapping
          * description template
          * any additional global rules
    """
    try:
        t = normalize_text(text or "")
        row = base_row_dict()

        # --------------------------
        # STEP 1: Vendor tax & code
        # --------------------------
        vendor_tax = find_vendor_tax_id(t, "Lazada") or VENDOR_LAZADA
        row["E_tax_id_13"] = vendor_tax

        # best-effort client tax id
        if not client_tax_id:
            client_tax_id = _pick_client_tax_id(t)

        row["D_vendor_code"] = _get_vendor_code_safe(client_tax_id, vendor_tax)
        row["F_branch_5"] = find_branch(t) or "00000"

        # --------------------------
        # STEP 2: Reference / Invoice No (NO SPACE)
        # --------------------------
        full_ref = _build_reference_no_space(t, filename=filename)
        if full_ref:
            row["G_invoice_no"] = full_ref
            row["C_reference"] = full_ref

        # --------------------------
        # STEP 3: Date (invoice date first)
        # --------------------------
        doc_date = ""
        m = RE_LAZADA_INVOICE_DATE.search(t)
        if m:
            doc_date = parse_date_to_yyyymmdd(m.group(1)) or ""
        if not doc_date:
            doc_date = find_best_date(t) or ""

        if doc_date:
            row["B_doc_date"] = doc_date
            row["H_invoice_date"] = doc_date
            row["I_tax_purchase_date"] = doc_date

        # --------------------------
        # STEP 4: Amounts (STRICT)
        # --------------------------
        total_ex_vat, vat_amount, total_inc_vat = extract_totals_block(t)

        # derive inc vat if missing (ex + vat exists)
        if not total_inc_vat:
            derived = _derive_total_inc_vat(total_ex_vat, vat_amount)
            if derived:
                total_inc_vat = derived

        # WHT detection (separate channel) - NEVER use as total
        wht_rate, wht_amount_3pct = extract_wht_from_text(t)
        has_wht_3 = (wht_rate == "3%" and bool(wht_amount_3pct))

        # FINAL fallback: common extractor, but reject if equals WHT
        if not total_inc_vat:
            a = extract_amounts(t) or {}
            cand_total = (a.get("total", "") or "").strip()
            cand_wht = (a.get("wht_amount", "") or "").strip()

            # never accept if it matches WHT
            if cand_total and cand_total != cand_wht:
                total_inc_vat = cand_total

        # If still missing, fallback to subtotal (still NOT WHT)
        if not total_inc_vat and total_ex_vat:
            total_inc_vat = total_ex_vat

        # --------------------------
        # STEP 5: PEAK mapping (strict)
        # --------------------------
        row["M_qty"] = "1"
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["Q_payment_method"] = "หักจากยอดขาย"

        if total_inc_vat:
            row["N_unit_price"] = total_inc_vat
            row["R_paid_amount"] = total_inc_vat
        else:
            row["N_unit_price"] = row.get("N_unit_price") or "0"
            row["R_paid_amount"] = row.get("R_paid_amount") or "0"

        # --------------------------
        # STEP 6: P_wht policy
        # --------------------------
        if WHT_MODE.upper() == "RATE":
            row["P_wht"] = "3%" if has_wht_3 else ""
            row["S_pnd"] = "53" if has_wht_3 else ""
        else:
            row["P_wht"] = ""   # ✅ blank policy
            row["S_pnd"] = ""   # keep empty unless you explicitly want it

        # --------------------------
        # STEP 7: Base description/group (post-process may override)
        # --------------------------
        row["L_description"] = "Marketplace Expense"
        row["U_group"] = "Marketplace Expense"

        # Notes must be blank
        row["T_note"] = ""

        # K_account template (post-process is preferred)
        row["K_account"] = ""

        # --------------------------
        # STEP 8: Safety sync + strict squash
        # --------------------------
        row["C_reference"] = _squash_ws(row.get("C_reference", ""))
        row["G_invoice_no"] = _squash_ws(row.get("G_invoice_no", ""))

        if not row.get("C_reference") and row.get("G_invoice_no"):
            row["C_reference"] = row["G_invoice_no"]
        if not row.get("G_invoice_no") and row.get("C_reference"):
            row["G_invoice_no"] = row["C_reference"]

        # --------------------------
        # STEP 9: Plan C — enforce C/G from filename at end
        # --------------------------
        _enforce_ref_from_filename(row, filename=filename)

        # after enforce: re-squash
        row["C_reference"] = _squash_ws(row.get("C_reference", ""))
        row["G_invoice_no"] = _squash_ws(row.get("G_invoice_no", ""))

        # --------------------------
        # STEP 10: Post-process (optional)
        # --------------------------
        if callable(post_process_peak_row):
            try:
                row = post_process_peak_row(
                    row=row,
                    platform="lazada",
                    filename=filename or "",
                    client_tax_id=_digits_only(client_tax_id) if client_tax_id else "",
                    text=t,
                ) or row
            except Exception:
                # must never crash extractor
                pass

        return format_peak_row(row)

    except Exception:
        # Fail-safe: never crash, stable output
        row = base_row_dict()
        row["D_vendor_code"] = "Lazada"
        row["E_tax_id_13"] = VENDOR_LAZADA
        row["F_branch_5"] = "00000"
        row["M_qty"] = "1"
        row["J_price_type"] = "1"
        row["O_vat_rate"] = "7%"
        row["P_wht"] = ""       # ✅ blank policy
        row["S_pnd"] = ""
        row["N_unit_price"] = "0"
        row["R_paid_amount"] = "0"
        row["Q_payment_method"] = "หักจากยอดขาย"
        row["U_group"] = "Marketplace Expense"
        row["L_description"] = "Marketplace Expense"
        row["T_note"] = ""
        row["K_account"] = ""

        # enforce C/G from filename even in fail-safe
        _enforce_ref_from_filename(row, filename=filename or "")
        row["C_reference"] = _squash_ws(row.get("C_reference", ""))
        row["G_invoice_no"] = _squash_ws(row.get("G_invoice_no", ""))

        # post-process (optional)
        if callable(post_process_peak_row):
            try:
                row = post_process_peak_row(
                    row=row,
                    platform="lazada",
                    filename=filename or "",
                    client_tax_id=_digits_only(client_tax_id) if client_tax_id else "",
                    text=normalize_text(text or ""),
                ) or row
            except Exception:
                pass

        return format_peak_row(row)


__all__ = [
    "extract_lazada",
    "extract_wht_from_text",
    "extract_totals_block",
]
