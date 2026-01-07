# backend/app/extractors/shopee.py
"""
Shopee extractor - PEAK A-U format
Supports: Marketplace service fees with 7% VAT and 3% WHT
"""
from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_date_to_yyyymmdd,
    extract_amounts,
    extract_seller_info,
    format_peak_row,
    parse_money,
)

RE_SHOPEE_DOC_STRICT = re.compile(
    r'\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-/]{10,})\b',
    re.IGNORECASE
)

RE_SHOPEE_DOC_DATE = re.compile(
    r'(?:วันที่(?:เอกสาร|ออกเอกสาร)?|Date\s*(?:of\s*issue)?|Issue\s*date|Document\s*date)\s*[:#：]?\s*'
    r'(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})',
    re.IGNORECASE
)

RE_SHOPEE_INVOICE_DATE = re.compile(
    r'(?:วันที่ใบกำกับ(?:ภาษี)?|Invoice\s*date|Tax\s*Invoice\s*date)\s*[:#：]?\s*'
    r'(\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{4}|\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2})',
    re.IGNORECASE
)

RE_SHOPEE_FEE_LINE = re.compile(
    r'^\s*(.{10,80}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$',
    re.MULTILINE | re.IGNORECASE
)

RE_SHOPEE_FEE_KEYWORDS = re.compile(
    r'(?:ค่าธรรมเนียม|ค่าบริการ|คอมมิชชั่น|Commission|Service\s*fee|Transaction\s*fee|Platform\s*fee'
    r'|Payment\s*fee|Shipping\s*fee|Marketing\s*fee|Voucher|ภาษีมูลค่าเพิ่ม|VAT)',
    re.IGNORECASE
)


def extract_shopee_fee_summary(text: str, max_items: int = 8) -> Tuple[str, str]:
    """Extract fee breakdown from Shopee document"""
    t = normalize_text(text)
    
    fee_items = []
    fee_details = []
    
    for m in RE_SHOPEE_FEE_LINE.finditer(t):
        line_text = m.group(1).strip()
        amount = m.group(2)
        
        if not RE_SHOPEE_FEE_KEYWORDS.search(line_text):
            continue
        
        if any(kw in line_text.lower() for kw in ['total', 'รวม', 'sum', 'grand']):
            continue
        
        parsed_amount = parse_money(amount)
        if not parsed_amount or parsed_amount == '0.00':
            continue
        
        fee_name = re.sub(r'\s{2,}', ' ', line_text)
        fee_name = fee_name[:60]
        
        fee_items.append(fee_name)
        fee_details.append(f"- {fee_name}: ฿{parsed_amount}")
        
        if len(fee_items) >= max_items:
            break
    
    if not fee_items:
        return ('', '')
    
    short = 'Shopee Fees: ' + ', '.join(fee_items[:3])
    if len(fee_items) > 3:
        short += f' (+{len(fee_items)-3} more)'
    
    notes = '\n'.join(fee_details)
    
    return (short, notes)


def extract_shopee(text: str) -> Dict[str, Any]:
    """
    Extract Shopee Tax Invoice/Statement
    
    Returns PEAK A-U formatted dict
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    row['D_vendor_code'] = 'Shopee'
    row['U_group'] = 'Marketplace Expense'
    
    vendor_tax = find_vendor_tax_id(t, 'Shopee')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    
    m = RE_SHOPEE_DOC_STRICT.search(t)
    if m:
        doc_no = m.group(1)
        row['G_invoice_no'] = doc_no
        row['C_reference'] = doc_no
    else:
        doc_no = find_invoice_no(t, 'Shopee')
        if doc_no:
            row['G_invoice_no'] = doc_no
            row['C_reference'] = doc_no
    
    date = ''
    m = RE_SHOPEE_DOC_DATE.search(t)
    if m:
        date = parse_date_to_yyyymmdd(m.group(1))
    
    if not date:
        m = RE_SHOPEE_INVOICE_DATE.search(t)
        if m:
            date = parse_date_to_yyyymmdd(m.group(1))
    
    if not date:
        date = find_best_date(t)
    
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    seller_info = extract_seller_info(t)
    seller_id = seller_info['seller_id']
    username = seller_info['username']
    
    amounts = extract_amounts(t)
    
    subtotal = amounts['subtotal']
    vat = amounts['vat']
    total = amounts['total']
    
    row['M_qty'] = '1'
    
    if subtotal:
        row['N_unit_price'] = subtotal
    elif total:
        row['N_unit_price'] = total
    
    if total:
        row['R_paid_amount'] = total
    elif subtotal:
        row['R_paid_amount'] = subtotal
    
    row['J_price_type'] = '1'
    row['O_vat_rate'] = '7%'
    
    wht_rate = amounts['wht_rate'] or '3%'
    wht_amount = amounts['wht_amount']
    
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    short_desc, fee_notes = extract_shopee_fee_summary(t)
    
    desc_parts = []
    desc_parts.append('Shopee - Marketplace Service Fees')
    
    if short_desc:
        desc_parts.append(short_desc)
    
    if seller_id:
        desc_parts.append(f"Seller: {seller_id}")
    
    if subtotal and total:
        desc_parts.append(f"Subtotal ฿{subtotal} + VAT ฿{vat} = Total ฿{total}")
    elif total:
        desc_parts.append(f"Total ฿{total}")
    
    if wht_amount:
        desc_parts.append(f"WHT {wht_rate}: ฿{wht_amount}")
    
    row['L_description'] = ' | '.join(desc_parts)
    
    note_parts = []
    
    if seller_id:
        note_parts.append(f"Seller ID: {seller_id}")
    if username:
        note_parts.append(f"Username: {username}")
    
    if fee_notes:
        note_parts.append('\nFee Breakdown:')
        note_parts.append(fee_notes)
    
    if wht_rate and wht_amount:
        note_parts.append(f'\nWithholding Tax {wht_rate}: ฿{wht_amount}')
    
    if note_parts:
        row['T_note'] = '\n'.join(note_parts)
    
    row['Q_payment_method'] = 'หักจากยอดขาย'
    row['K_account'] = ''
    
    return format_peak_row(row)