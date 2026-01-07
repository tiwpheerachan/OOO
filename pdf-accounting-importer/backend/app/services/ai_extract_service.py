from __future__ import annotations
import os, json
from typing import Dict, Any, Optional

from openai import OpenAI

PEAK_FIELDS = [
    "A_seq","B_doc_date","C_reference","D_vendor_code","E_tax_id_13","F_branch_5",
    "G_invoice_no","H_invoice_date","I_tax_purchase_date","J_price_type","K_account",
    "L_description","M_qty","N_unit_price","O_vat_rate","P_wht","Q_payment_method",
    "R_paid_amount","S_pnd","T_note","U_group"
]

SYSTEM = """You are an expert Thai accounting document extractor.
Extract fields into the target template. Return STRICT JSON only.
Follow rules:
- Dates must be YYYYMMDD
- Tax id must be 13 digits (no spaces)
- Branch must be 5 digits (pad with leading zeros)
- Money fields must be plain numbers with 2 decimals if possible
- If unknown, return empty string not null
- Use vendor_code among: Shopee, Lazada, TikTok, Other
- J_price_type: 1=แยก VAT, 2=รวม VAT, 3=ไม่มี VAT
- O_vat_rate: '7%' or 'NO'
- U_group: choose from Marketplace Expense, Advertising Expense, Inventory/COGS, Other Expense
"""

def _blank_row() -> Dict[str, Any]:
    return {k: "" for k in PEAK_FIELDS} | {"A_seq":"1","M_qty":"1","J_price_type":"1","O_vat_rate":"7%","P_wht":"0","R_paid_amount":"0","N_unit_price":"0"}

def extract_with_ai(full_text: str, filename: str = "") -> Dict[str, Any]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    user_prompt = {
        "filename": filename,
        "template_fields": PEAK_FIELDS,
        "document_text": full_text[:120000],  # กันยาวเกิน
    }

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role":"system","content": SYSTEM},
            {"role":"user","content": json.dumps(user_prompt, ensure_ascii=False)}
        ],
        response_format={"type":"json_object"},
    )

    data = json.loads(resp.choices[0].message.content or "{}")
    row = _blank_row()

    for k in PEAK_FIELDS:
        v = data.get(k, "")
        row[k] = "" if v is None else str(v).strip()

    # basic normalization
    row["E_tax_id_13"] = "".join([c for c in row["E_tax_id_13"] if c.isdigit()])[:13]
    row["F_branch_5"] = row["F_branch_5"].zfill(5)[:5] if row["F_branch_5"].isdigit() else (row["F_branch_5"] or "00000")

    return row
