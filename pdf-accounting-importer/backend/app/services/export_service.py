# backend/app/services/export_service.py
from __future__ import annotations

import csv
import io
import re
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side, numbers
from openpyxl.utils import get_column_letter


# =========================
# Columns
# =========================
# ✅ เพิ่ม A_company_name ต่อจาก A_seq
COLUMNS: List[Tuple[str, str]] = [
    ("A_seq", "ลำดับที่*"),
    ("A_company_name", "ชื่อบริษัท"),
    ("B_doc_date", "วันที่เอกสาร"),
    ("C_reference", "อ้างอิงถึง"),
    ("D_vendor_code", "ผู้รับเงิน/คู่ค้า"),
    ("E_tax_id_13", "เลขทะเบียน 13 หลัก"),
    ("F_branch_5", "เลขสาขา 5 หลัก"),
    ("G_invoice_no", "เลขที่ใบกำกับฯ (ถ้ามี)"),
    ("H_invoice_date", "วันที่ใบกำกับฯ (ถ้ามี)"),
    ("I_tax_purchase_date", "วันที่บันทึกภาษีซื้อ (ถ้ามี)"),
    ("J_price_type", "ประเภทราคา"),
    ("K_account", "บัญชี"),
    ("L_description", "คำอธิบาย"),
    ("M_qty", "จำนวน"),
    ("N_unit_price", "ราคาต่อหน่วย"),
    ("O_vat_rate", "อัตราภาษี"),
    ("P_wht", "หัก ณ ที่จ่าย (ถ้ามี)"),
    ("Q_payment_method", "ชำระโดย"),
    ("R_paid_amount", "จำนวนเงินที่ชำระ"),
    ("S_pnd", "ภ.ง.ด. (ถ้ามี)"),
    ("T_note", "หมายเหตุ"),
    ("U_group", "กลุ่มจัดประเภท"),
]

# Columns that MUST be TEXT in Excel (preserve leading zeros, exact strings)
TEXT_COL_KEYS = {
    "A_seq",            # keep as text is fine for PEAK import
    "A_company_name",
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "J_price_type",     # "1"/"2"/"3"
    "O_vat_rate",       # "7%" or "NO"
    "S_pnd",            # "53" etc.
    "Q_payment_method", # wallet code e.g. EWL001
}

# Numeric-like columns (write as numbers when safe)
NUM_COL_KEYS = {
    "M_qty",
    "N_unit_price",
    "R_paid_amount",
    # NOTE: P_wht เราจะบังคับให้เป็น "" เสมอ (ไม่ต้องเขียนเป็นตัวเลข)
}

# Date columns are strings "YYYYMMDD" (keep as text)
DATE_COL_KEYS = {"B_doc_date", "H_invoice_date", "I_tax_purchase_date"}

# CSV/Excel injection prevention
EXCEL_INJECTION_PREFIXES = ("=", "+", "-", "@")

RE_YYYYMMDD = re.compile(r"^\d{8}$")
RE_DECIMAL = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
RE_ALL_WS = re.compile(r"\s+")


# =========================
# Helpers
# =========================
def _s(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _escape_excel_formula(s: str) -> str:
    if not s:
        return s
    if s[0] in EXCEL_INJECTION_PREFIXES:
        return "'" + s
    return s


def _as_decimal_str(s: str) -> str:
    if not s:
        return ""
    x = s.replace(",", "").replace("฿", "").replace("THB", "").strip()
    try:
        d = Decimal(x)
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""


def _compact_no_ws(v: Any) -> str:
    """
    ✅ ตัด whitespace ทั้งหมด (space/newline/tab) ให้ token ติดกัน
    เช่น "RCSPX...-25 1218-0001" -> "RCSPX...-251218-0001"
    """
    s = _s(v)
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _preprocess_rows_for_export(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    ✅ รวมกฎที่คุณต้องการก่อน export ให้ “ชัวร์” แม้ upstream พลาด:
    1) A_seq = 1..n ใหม่ตามลำดับปัจจุบัน
    2) P_wht = "" เสมอ
    3) C_reference / G_invoice_no ติดกันไม่มีช่องว่าง
    """
    out: List[Dict[str, Any]] = []
    seq = 1

    for r in rows or []:
        rr = dict(r)

        rr["A_seq"] = str(seq)  # keep as text in Excel too
        seq += 1

        # force blank WHT
        rr["P_wht"] = ""

        # compact reference/invoice tokens
        rr["C_reference"] = _compact_no_ws(rr.get("C_reference", ""))
        rr["G_invoice_no"] = _compact_no_ws(rr.get("G_invoice_no", ""))

        # normalize company name if missing
        rr["A_company_name"] = _s(rr.get("A_company_name", ""))

        out.append(rr)

    return out


def _to_number_or_text(key: str, raw: Any) -> Tuple[Any, str]:
    s = _s(raw)

    # Always treat these as text (including dates YYYYMMDD)
    if key in TEXT_COL_KEYS or key in DATE_COL_KEYS:
        return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

    # For numeric fields, try numeric, else text
    if key in NUM_COL_KEYS:
        if key == "M_qty":
            norm = _as_decimal_str(s)
            if norm:
                try:
                    f = float(norm)
                    if abs(f - int(f)) < 1e-9:
                        return (int(f), "0")
                    return (f, numbers.FORMAT_NUMBER_00)
                except Exception:
                    pass
            return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

        # money
        norm = _as_decimal_str(s)
        if norm:
            return (float(norm), numbers.FORMAT_NUMBER_00)
        return (_escape_excel_formula(s), numbers.FORMAT_TEXT)

    # Default: plain text
    return (_escape_excel_formula(s), numbers.FORMAT_TEXT)


def _auto_fit_columns(ws, max_width: int = 60, min_width: int = 10) -> None:
    for col_idx, (_key, label) in enumerate(COLUMNS, start=1):
        col_letter = get_column_letter(col_idx)
        max_len = len(str(label))

        for row_idx in range(2, ws.max_row + 1):
            v = ws.cell(row=row_idx, column=col_idx).value
            if v is None:
                continue
            s = str(v)
            if "\n" in s:
                s = s.split("\n", 1)[0]
            max_len = max(max_len, len(s))

        width = int(min(max(max_len + 2, min_width), max_width))
        ws.column_dimensions[col_letter].width = width


# =========================
# CSV Export
# =========================
def export_rows_to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    rows2 = _preprocess_rows_for_export(rows)

    out = io.StringIO()
    wri = csv.writer(out, quoting=csv.QUOTE_MINIMAL)

    wri.writerow([label for _, label in COLUMNS])

    for r in rows2:
        row_out: List[str] = []
        for k, _label in COLUMNS:
            s = _s(r.get(k, ""))
            s = _escape_excel_formula(s)
            row_out.append(s)
        wri.writerow(row_out)

    return out.getvalue().encode("utf-8-sig")


# =========================
# XLSX Export
# =========================
def export_rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    rows2 = _preprocess_rows_for_export(rows)

    wb = Workbook()
    ws = wb.active
    ws.title = "PEAK_IMPORT"

    # Header row
    headers = [label for _, label in COLUMNS]
    ws.append(headers)

    # Header styling
    header_fill = PatternFill("solid", fgColor="E8F1FF")
    header_font = Font(bold=True)
    header_align = Alignment(vertical="center", horizontal="center", wrap_text=True)

    thin = Side(style="thin", color="D0D7E2")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for col_idx in range(1, len(COLUMNS) + 1):
        c = ws.cell(row=1, column=col_idx)
        c.fill = header_fill
        c.font = header_font
        c.alignment = header_align
        c.border = border

    # Data rows
    for r in rows2:
        values: List[Any] = []
        formats: List[str] = []

        for k, _label in COLUMNS:
            v, fmt = _to_number_or_text(k, r.get(k, ""))
            values.append(v)
            formats.append(fmt)

        ws.append(values)

        current_row = ws.max_row
        for col_idx, fmt in enumerate(formats, start=1):
            cell = ws.cell(row=current_row, column=col_idx)
            if fmt:
                cell.number_format = fmt
            # wrap long text: L_description, T_note
            cell.alignment = Alignment(vertical="top", wrap_text=(col_idx in {13, 21}))
            cell.border = border

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLUMNS))}1"

    # Force TEXT formatting for critical columns (even if empty)
    col_index = {k: i + 1 for i, (k, _) in enumerate(COLUMNS)}
    for key in (TEXT_COL_KEYS | DATE_COL_KEYS):
        ci = col_index.get(key)
        if not ci:
            continue
        for row_i in range(2, 2 + len(rows2)):
            cell = ws.cell(row=row_i, column=ci)
            cell.number_format = numbers.FORMAT_TEXT

    _auto_fit_columns(ws)

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


__all__ = [
    "COLUMNS",
    "export_rows_to_csv_bytes",
    "export_rows_to_xlsx_bytes",
]
