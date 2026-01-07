from __future__ import annotations
from typing import Dict, Any
from .common import (
    base_row_dict, find_tax_id, find_branch, find_first_date, find_invoice_no, find_total_amount
)

def extract_generic(text: str) -> Dict[str, Any]:
    row = base_row_dict()
    row["E_tax_id_13"] = find_tax_id(text)
    row["F_branch_5"] = find_branch(text)
    row["B_doc_date"] = find_first_date(text)
    row["G_invoice_no"] = find_invoice_no(text)
    row["H_invoice_date"] = row["B_doc_date"]
    amt = find_total_amount(text)
    if amt:
        row["R_paid_amount"] = amt
        row["N_unit_price"] = amt
    row["L_description"] = "นำเข้าจากเอกสาร (ต้องตรวจสอบ)"
    return row
