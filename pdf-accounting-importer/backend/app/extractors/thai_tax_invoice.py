"""
Thai Tax Invoice Extractor
Extracts data from Thai tax invoices (ใบเสร็จรับเงิน/ใบกำกับภาษี)
"""
from __future__ import annotations
from typing import Dict, Any
import re


def extract_thai_tax_invoice(text: str, filename: str = "", client_tax_id: str = "") -> Dict[str, Any]:
    """
    Extract PEAK row from Thai Tax Invoice
    
    Pattern:
    - ใบเสร็จรับเงิน / ใบกำกับภาษี
    - เลขที่: xxx
    - ใบเสร็จวันที่: 17/12/2568
    - เลขประจำตัวผู้เสียภาษี (vendor): 0107567000414
    - รวมยอดที่ต้ระ: 1,841.00
    """
    
    row: Dict[str, Any] = {}
    
    # ============================================================
    # Extract invoice/receipt number
    # ============================================================
    # Pattern: "เลขที่: 0518520251217000011" or "เลขที่ :"
    invoice_match = re.search(r"เลขที่\s*[:\s]+([0-9]+)", text)
    if invoice_match:
        row["C_reference"] = invoice_match.group(1)
        row["G_invoice_no"] = invoice_match.group(1)
    else:
        row["C_reference"] = ""
        row["G_invoice_no"] = ""
    
    # ============================================================
    # Extract date
    # ============================================================
    # Pattern: "17/12/2568" (BE format) or "17/12/2025"
    date_match = re.search(r"(?:ใบเสร็จวันที่|วันที่)[:\s]*(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if date_match:
        day = date_match.group(1).zfill(2)
        month = date_match.group(2).zfill(2)
        year = date_match.group(3)
        
        # Convert BE to AD if needed
        year_int = int(year)
        if year_int > 2500:
            year_int -= 543  # BE to AD
        
        row["B_doc_date"] = f"{year_int}{month}{day}"
    else:
        row["B_doc_date"] = ""
    
    # ============================================================
    # Extract vendor tax ID
    # ============================================================
    # Pattern: "เลขประจำตัวผู้เสียภาษี : 0107567000414"
    # Look for 13-digit tax ID (first occurrence = vendor)
    tax_ids = re.findall(r"(?:เลขประจำตัวผู้เสียภาษี|Tax ID)[:\s]*(\d{13})", text)
    if tax_ids:
        # First = vendor, Second = buyer (if exists)
        row["E_tax_id_13"] = tax_ids[0]
    else:
        row["E_tax_id_13"] = ""
    
    # ============================================================
    # Extract vendor name
    # ============================================================
    # Try to find company name before first tax ID
    if tax_ids:
        vendor_match = re.search(
            r"(บริษัท[^0-9\n]+?)(?=เลขประจำตัวผู้เสียภาษี)",
            text,
            re.DOTALL
        )
        if vendor_match:
            vendor_name = vendor_match.group(1).strip()
            # Clean up
            vendor_name = re.sub(r"\s+", " ", vendor_name)
            vendor_name = vendor_name[:100]  # limit length
            row["D_vendor_code"] = vendor_name
        else:
            row["D_vendor_code"] = ""
    else:
        row["D_vendor_code"] = ""
    
    # ============================================================
    # Extract branch
    # ============================================================
    branch_match = re.search(r"สาขาที่[:\s]*(\d{5})", text)
    if branch_match:
        row["F_branch_5"] = branch_match.group(1)
    else:
        row["F_branch_5"] = "00000"
    
    # ============================================================
    # Extract total amount
    # ============================================================
    # Pattern: "รวมยอดที่ต้ระ: 1,841.00" or "รวมทั้งสิ้น"
    amount_patterns = [
        r"รวมยอดที่ต้?ระ[:\s]*([\d,]+\.?\d*)",
        r"รวมทั้งสิ้น[:\s]*([\d,]+\.?\d*)",
        r"ยอดรวม[:\s]*([\d,]+\.?\d*)",
        r"รวม[:\s]*([\d,]+\.?\d*)"
    ]
    
    for pattern in amount_patterns:
        amount_match = re.search(pattern, text)
        if amount_match:
            amount_str = amount_match.group(1).replace(",", "")
            row["R_paid_amount"] = amount_str
            row["N_unit_price"] = amount_str
            break
    
    if "R_paid_amount" not in row:
        row["R_paid_amount"] = ""
        row["N_unit_price"] = ""
    
    # ============================================================
    # Extract VAT
    # ============================================================
    # Pattern: "ภาษีมูลค่าเพิ่ม" or "VAT"
    vat_match = re.search(r"ภาษีมูลค่าเพิ่ม[:\s]*([\d,]+\.?\d*)", text)
    if vat_match:
        vat_str = vat_match.group(1).replace(",", "")
        if float(vat_str) > 0:
            row["O_vat_rate"] = "7%"
        else:
            row["O_vat_rate"] = "NO"
    else:
        # Default to 7% for Thai invoices
        row["O_vat_rate"] = "7%"
    
    # ============================================================
    # Description
    # ============================================================
    row["L_description"] = "Thai Tax Invoice"
    
    # ============================================================
    # Defaults
    # ============================================================
    row["A_seq"] = ""
    row["A_company_name"] = ""
    row["H_invoice_date"] = row["B_doc_date"]
    row["I_tax_purchase_date"] = ""
    row["J_price_type"] = "1"  # รวม VAT
    row["K_account"] = ""
    row["M_qty"] = "1"
    row["P_wht"] = ""
    row["Q_payment_method"] = ""
    row["S_pnd"] = ""
    row["T_note"] = ""
    row["U_group"] = ""
    
    return row