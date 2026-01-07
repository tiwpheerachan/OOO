# backend/app/services/export_service.py
from __future__ import annotations
import csv
import io
from typing import List, Dict, Any
from openpyxl import Workbook
from openpyxl.styles import numbers

COLUMNS = [
    ("A_seq","ลำดับที่*"),
    ("B_doc_date","วันที่เอกสาร"),
    ("C_reference","อ้างอิงถึง"),
    ("D_vendor_code","ผู้รับเงิน/คู่ค้า"),
    ("E_tax_id_13","เลขทะเบียน 13 หลัก"),
    ("F_branch_5","เลขสาขา 5 หลัก"),
    ("G_invoice_no","เลขที่ใบกำกับฯ (ถ้ามี)"),
    ("H_invoice_date","วันที่ใบกำกับฯ (ถ้ามี)"),
    ("I_tax_purchase_date","วันที่บันทึกภาษีซื้อ (ถ้ามี)"),
    ("J_price_type","ประเภทราคา"),
    ("K_account","บัญชี"),
    ("L_description","คำอธิบาย"),
    ("M_qty","จำนวน"),
    ("N_unit_price","ราคาต่อหน่วย"),
    ("O_vat_rate","อัตราภาษี"),
    ("P_wht","หัก ณ ที่จ่าย (ถ้ามี)"),
    ("Q_payment_method","ชำระโดย"),
    ("R_paid_amount","จำนวนเงินที่ชำระ"),
    ("S_pnd","ภ.ง.ด. (ถ้ามี)"),
    ("T_note","หมายเหตุ"),
    ("U_group","กลุ่มจัดประเภท"),
]

TEXT_COL_KEYS = {"C_reference","D_vendor_code","E_tax_id_13","F_branch_5","G_invoice_no"}

def _s(v: Any) -> str:
    if v is None:
        return ""
    # keep exact string (prevent Excel auto-cast issues in XLSX)
    return str(v).strip()

def export_rows_to_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    out = io.StringIO()
    wri = csv.writer(out, quoting=csv.QUOTE_MINIMAL)

    wri.writerow([label for _, label in COLUMNS])
    for r in rows:
        wri.writerow([_s(r.get(k, "")) for k,_ in COLUMNS])

    return out.getvalue().encode("utf-8-sig")

def export_rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "PEAK_IMPORT"
    ws.append([label for _, label in COLUMNS])

    # write rows as text where needed
    for r in rows:
        ws.append([_s(r.get(k, "")) for k,_ in COLUMNS])

    # force TEXT format for critical columns to preserve leading zeros
    # columns are 1-indexed in openpyxl
    col_index = {k: i+1 for i,(k,_) in enumerate(COLUMNS)}
    for key in TEXT_COL_KEYS:
        ci = col_index.get(key)
        if not ci:
            continue
        for row_i in range(2, 2+len(rows)):
            cell = ws.cell(row=row_i, column=ci)
            cell.number_format = numbers.FORMAT_TEXT

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()
