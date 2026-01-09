# backend/app/extractors/lazada.py
"""
Lazada extractor - PEAK A-U format (Enhanced v3.2 FIXED)

Fix goals (same class of bugs as TikTok/SPX):
  ✅ NEVER let WHT amount overwrite unit_price / paid_amount
  ✅ Separate amounts clearly:
     - total_ex_vat
     - vat_amount
     - total_inc_vat  (PRIMARY)
     - wht_amount_3pct
  ✅ Mapping (per your strict rule):
     row["N_unit_price"]  = total_inc_vat
     row["R_paid_amount"] = total_inc_vat
     row["O_vat_rate"]    = "7%"
  ✅ Use "Total (Including Tax)" as primary (from Lazada totals block)
  ✅ Fallbacks must avoid "wht_amount" being mistaken as totals

IMPORTANT POLICY (your requirement):
  - P_wht is often blank in your output. If you want it ALWAYS blank, keep WHT_MODE="EMPTY"
  - If later you want "P_wht = 3% when WHT exists", set WHT_MODE="RATE"
  - T_note MUST stay empty (per your pipeline)
  - C_reference and G_invoice_no must have NO spaces even across lines (join tokens)
"""

from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List, Optional

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_date_to_yyyymmdd,
    extract_amounts,          # fallback only
    extract_seller_info,      # fallback only
    format_peak_row,
    parse_money,
)

# ========================================
# POLICY FLAGS (สำคัญ)
# ========================================
# "EMPTY" = P_wht always "0" (blank policy)
# "RATE"  = P_wht "3%" if WHT exists else "0"
WHT_MODE = "EMPTY"

# ========================================
# Import vendor mapping (with fallback)
# ========================================
try:
    from .vendor_mapping import (
        get_vendor_code,
        VENDOR_LAZADA,
        VENDOR_NAME_MAP,
    )
    VENDOR_MAPPING_AVAILABLE = True
except ImportError:
    VENDOR_MAPPING_AVAILABLE = False
    VENDOR_LAZADA = "0105555040244"  # Lazada Tax ID
    VENDOR_NAME_MAP = {}             # safe fallback


# ============================================================
# Lazada-specific patterns (matched to sample PDF)
# ============================================================

RE_LAZADA_SELLER_CODE_TH = re.compile(r"\b(TH[A-Z0-9]{8,12})\b")
RE_LAZADA_DOC_THMPTI = re.compile(r"\b(THMPTI\d{16})\b", re.IGNORECASE)

RE_LAZADA_INVOICE_NO_FIELD = re.compile(
    r"Invoice\s*No\.?\s*[:#：]?\s*([A-Z0-9\-/]{8,40})",
    re.IGNORECASE
)

RE_LAZADA_INVOICE_DATE = re.compile(
    r"Invoice\s*Date\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})",
    re.IGNORECASE
)

RE_LAZADA_PERIOD = re.compile(
    r"Period\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})\s*[-–]\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})",
    re.IGNORECASE
)

RE_TAX_ID_13 = re.compile(r"\b(\d{13})\b")

RE_LAZADA_FEE_LINE = re.compile(
    r"^\s*(\d+)\s+(.{3,120}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$",
    re.MULTILINE
)

# Totals block (STRICT, best source)
RE_LAZADA_SUBTOTAL_TOTAL = re.compile(
    r"^\s*Total\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_VAT_7 = re.compile(
    r"^\s*7%\s*\(VAT\)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)
RE_LAZADA_TOTAL_INC = re.compile(
    r"^\s*Total\s*\(Including\s*Tax\)\s+([0-9,]+\.[0-9]{2})\s*$",
    re.MULTILINE | re.IGNORECASE
)

RE_LAZADA_WHT_TEXT = re.compile(
    r"หักภาษีณ?\s*ที่จ่าย.*?อัตรา(?:ร้อยละ)?\s*(\d+)\s*%.*?เป็นจำนวน\s*([0-9,]+(?:\.[0-9]{2})?)\s*บาท",
    re.IGNORECASE | re.DOTALL
)

RE_LAZADA_FEE_KEYWORDS = re.compile(
    r"(?:Payment\s*Fee|Commission|Premium\s*Package|LazCoins|Sponsored|Voucher|Marketing|Service|Discovery|Participation|Funded)",
    re.IGNORECASE
)

# --- Reference join patterns (สำคัญมาก) ---
# ตัวอย่างที่คุณให้: RCSPXSPB00-00000-25 1218-0001593  (อาจมาคนละบรรทัด)
RE_DOCNO_GENERIC = re.compile(r"\b([A-Z0-9]{6,20}-\d{5}-\d{2})\b", re.IGNORECASE)
RE_MMDD_SEQ = re.compile(r"\b(\d{4})\s*-\s*(\d{7})\b")


# ============================================================
# Helpers
# ============================================================

def _safe_money(v: str) -> str:
    """Return normalized money '1234.56' or ''."""
    try:
        return parse_money(v)
    except Exception:
        return ""


def _pick_client_tax_id(text: str) -> str:
    t = normalize_text(text)
    for m in RE_TAX_ID_13.finditer(t):
        tax = m.group(1)
        if tax and tax != VENDOR_LAZADA:
            return tax
    return ""


def _squash_spaces_and_newlines(s: str) -> str:
    """
    Remove ALL whitespace to prevent 'RC... 1218-...' becoming split.
    Very important for C_reference / G_invoice_no.
    """
    if not s:
        return ""
    return re.sub(r"\s+", "", s)


def _build_full_reference_no_space(text: str) -> str:
    """
    Build reference like:
      DOCNO + MMDD-XXXXXXX
    WITHOUT any spaces, even if OCR splits into multiple lines.

    Example:
      RCSPXSPB00-00000-25
      1218-0001593
    -> RCSPXSPB00-00000-251218-0001593
    """
    t0 = normalize_text(text or "")
    t = _squash_spaces_and_newlines(t0)

    m_doc = RE_DOCNO_GENERIC.search(t)
    m_ref = RE_MMDD_SEQ.search(t)

    if m_doc and m_ref:
        return f"{m_doc.group(1)}{m_ref.group(1)}-{m_ref.group(2)}"

    # fallback: if only invoice_no exists (THMPTI...)
    m_thmpti = RE_LAZADA_DOC_THMPTI.search(t)
    if m_thmpti:
        return m_thmpti.group(1)

    # fallback: invoice field (keep also squashed)
    m_inv = RE_LAZADA_INVOICE_NO_FIELD.search(t0)
    if m_inv:
        return _squash_spaces_and_newlines(m_inv.group(1).strip())

    return ""


def extract_seller_code_lazada(text: str) -> str:
    t = normalize_text(text)

    candidates = []
    for m in RE_LAZADA_SELLER_CODE_TH.finditer(t):
        code = m.group(1)
        if not code.upper().startswith("THMPTI"):
            candidates.append(code)
    if candidates:
        return candidates[0]

    info = extract_seller_info(t)
    if info.get("seller_code"):
        sc = str(info["seller_code"]).strip()
        if sc and not sc.upper().startswith("THMPTI"):
            return sc

    return ""


def extract_lazada_fee_summary(text: str, max_items: int = 10) -> Tuple[str, str, List[Dict[str, str]]]:
    """
    NOTE: kept from your version. Used only for optional notes (but we keep T_note empty by policy).
    """
    t = normalize_text(text)

    fee_items: List[str] = []
    fee_details: List[str] = []
    fee_list: List[Dict[str, str]] = []

    for m in RE_LAZADA_FEE_LINE.finditer(t):
        no = m.group(1)
        desc = m.group(2).strip()
        amt_raw = m.group(3)

        desc_l = desc.lower()

        # skip totals-ish
        if desc_l.startswith("total"):
            continue
        if "including tax" in desc_l or "vat" in desc_l or "tax" in desc_l or "ภาษี" in desc_l:
            continue

        if not RE_LAZADA_FEE_KEYWORDS.search(desc):
            continue

        amt = parse_money(amt_raw)
        if not amt or amt in ("0", "0.00"):
            continue

        name = re.sub(r"\s{2,}", " ", desc).strip()
        if len(name) > 90:
            name = name[:90].rstrip()

        fee_items.append(name)
        fee_details.append(f"{no}. {name}: ฿{amt}")
        fee_list.append({"no": no, "name": name, "amount": amt})

        if len(fee_items) >= max_items:
            break

    if not fee_items:
        return ("", "", [])

    short = "Lazada Fees: " + ", ".join(fee_items[:3])
    if len(fee_items) > 3:
        short += f" (+{len(fee_items) - 3} more)"

    notes = "\n".join(fee_details)
    return (short, notes, fee_list)


def extract_wht_from_text(text: str) -> Tuple[str, str]:
    """
    Returns (rate, amount) like ("3%", "3219.71")
    STRICT: we extract but we will not let it pollute totals.
    """
    t = normalize_text(text)
    m = RE_LAZADA_WHT_TEXT.search(t)
    if not m:
        return ("", "")
    rate = f"{m.group(1)}%"
    amt = parse_money(m.group(2)) or ""
    return (rate, amt)


def extract_totals_block(text: str) -> Tuple[str, str, str]:
    """
    Extract (total_ex_vat, vat_amount, total_inc_vat) from totals area.
    PRIMARY and safest source.
    """
    t = normalize_text(text)

    total_ex_vat = ""
    vat_amount = ""
    total_inc_vat = ""

    m = RE_LAZADA_SUBTOTAL_TOTAL.search(t)
    if m:
        total_ex_vat = parse_money(m.group(1)) or ""

    m = RE_LAZADA_VAT_7.search(t)
    if m:
        vat_amount = parse_money(m.group(1)) or ""

    m = RE_LAZADA_TOTAL_INC.search(t)
    if m:
        total_inc_vat = parse_money(m.group(1)) or ""

    return (total_ex_vat, vat_amount, total_inc_vat)


def _safe_float(x: str) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def _derive_total_inc_vat(total_ex_vat: str, vat_amount: str) -> str:
    if not total_ex_vat or not vat_amount:
        return ""
    v = _safe_float(total_ex_vat) + _safe_float(vat_amount)
    if v <= 0:
        return ""
    return f"{v:.2f}"


# ============================================================
# Main
# ============================================================

def extract_lazada(text: str, client_tax_id: str = "", filename: str = "") -> Dict[str, Any]:
    """
    FIXED mapping rule:

    Separate:
      total_ex_vat
      vat_amount
      total_inc_vat  (PRIMARY)
      wht_amount_3pct

    Mapping:
      row["N_unit_price"]  = total_inc_vat
      row["R_paid_amount"] = total_inc_vat
      row["O_vat_rate"]    = "7%"
      P_wht policy controlled by WHT_MODE:
        - EMPTY: always "0"
        - RATE: "3%" if WHT exists else "0"

    Other:
      - C_reference and G_invoice_no must be identical and NO SPACE
      - T_note must remain empty
    """
    t = normalize_text(text)
    row = base_row_dict()

    # --------------------------
    # STEP 1: Vendor tax & code
    # --------------------------
    vendor_tax = find_vendor_tax_id(t, "Lazada")
    row["E_tax_id_13"] = vendor_tax or VENDOR_LAZADA

    if not client_tax_id:
        client_tax_id = _pick_client_tax_id(t)

    if VENDOR_MAPPING_AVAILABLE and client_tax_id:
        try:
            row["D_vendor_code"] = get_vendor_code(
                client_tax_id=client_tax_id,
                vendor_tax_id=row["E_tax_id_13"],
                vendor_name="Lazada",
            ) or "Lazada"
        except Exception:
            row["D_vendor_code"] = "Lazada"
    else:
        row["D_vendor_code"] = "Lazada"

    br = find_branch(t)
    row["F_branch_5"] = br if br else "00000"

    # --------------------------
    # STEP 2: Reference / Invoice No (NO SPACE)
    # --------------------------
    # Prefer your joined reference format (DOCNO + MMDD-XXXXXXX), fallback to find_invoice_no
    full_ref = _build_full_reference_no_space(t)
    if not full_ref:
        inv = find_invoice_no(t, "Lazada")
        full_ref = _squash_spaces_and_newlines(inv or "")

    if full_ref:
        row["G_invoice_no"] = full_ref
        row["C_reference"] = full_ref

    # --------------------------
    # STEP 3: Date & Period
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

    # Derive total_inc_vat if missing but ex+vat exist
    if not total_inc_vat:
        derived = _derive_total_inc_vat(total_ex_vat, vat_amount)
        if derived:
            total_inc_vat = derived

    # Extract WHT (separate channel)
    wht_rate, wht_amount_3pct = extract_wht_from_text(t)

    # FINAL fallback for totals: use extract_amounts BUT NEVER allow wht_amount to become total
    if not total_inc_vat:
        a = extract_amounts(t)
        cand_total = (a.get("total", "") or "").strip()
        cand_wht = (a.get("wht_amount", "") or "").strip()

        # reject if it equals WHT or looks too small compared to ex_vat (heuristic)
        if cand_total and cand_total != cand_wht:
            total_inc_vat = cand_total

    # If still missing: fallback to total_ex_vat (still NOT WHT)
    if not total_inc_vat and total_ex_vat:
        total_inc_vat = total_ex_vat

    # --------------------------
    # STEP 5: PEAK mapping (strict)
    # --------------------------
    row["M_qty"] = "1"

    # Primary rule: unit_price & paid_amount = total_inc_vat
    if total_inc_vat:
        row["N_unit_price"] = total_inc_vat
        row["R_paid_amount"] = total_inc_vat
    else:
        # never use WHT here
        row["N_unit_price"] = row.get("N_unit_price") or "0"
        row["R_paid_amount"] = row.get("R_paid_amount") or "0"

    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"
    row["Q_payment_method"] = "หักจากยอดขาย"

    # --------------------------
    # STEP 6: P_wht policy
    # --------------------------
    has_wht_3 = (wht_rate == "3%" and bool(wht_amount_3pct))

    if WHT_MODE.upper() == "RATE":
        row["P_wht"] = "3%" if has_wht_3 else "0"
        row["S_pnd"] = "53" if has_wht_3 else ""
    else:
        # EMPTY policy (your latest requirement)
        row["P_wht"] = "0"
        row["S_pnd"] = ""

    # --------------------------
    # STEP 7: Description / Group
    # --------------------------
    row["L_description"] = "Marketplace Expense"
    row["U_group"] = "Marketplace Expense"

    # --------------------------
    # STEP 8: Notes must be blank (ตามระบบคุณ)
    # --------------------------
    row["T_note"] = ""

    # Safety sync C/G
    if not row.get("C_reference") and row.get("G_invoice_no"):
        row["C_reference"] = row["G_invoice_no"]
    if not row.get("G_invoice_no") and row.get("C_reference"):
        row["G_invoice_no"] = row["C_reference"]

    row["K_account"] = ""

    return format_peak_row(row)


__all__ = [
    "extract_lazada",
    "extract_seller_code_lazada",
    "extract_lazada_fee_summary",
    "extract_wht_from_text",
    "extract_totals_block",
]
