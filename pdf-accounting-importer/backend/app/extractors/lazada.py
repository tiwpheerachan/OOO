# backend/app/extractors/lazada.py
"""
Lazada extractor - PEAK A-U format
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

RE_LAZADA_DOC_STRICT = re.compile(
    r'\b(THMPTI\d{16}|TH[A-Z0-9]{8,12})\b',
    re.IGNORECASE
)

RE_LAZADA_INVOICE_NO = re.compile(
    r'Invoice\s*No\.?\s*[:#：]?\s*([A-Z0-9\-/]{10,})',
    re.IGNORECASE
)

RE_LAZADA_INVOICE_DATE = re.compile(
    r'Invoice\s*Date\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})',
    re.IGNORECASE
)

RE_LAZADA_PERIOD = re.compile(
    r'Period\s*[:#：]?\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})\s*[-–]\s*(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})',
    re.IGNORECASE
)

RE_LAZADA_SELLER_CODE = re.compile(
    r'^([A-Z0-9]{8,15})$',
    re.MULTILINE
)

RE_LAZADA_FEE_LINE = re.compile(
    r'^\s*(\d+)\s+(.{10,80}?)\s+([0-9,]+(?:\.[0-9]{1,2})?)\s*$',
    re.MULTILINE | re.IGNORECASE
)

RE_LAZADA_FEE_KEYWORDS = re.compile(
    r'(?:Payment\s*Fee|Commission|Premium\s*Package|LazCoins|Sponsored|Voucher|Marketing|Service)',
    re.IGNORECASE
)


def extract_lazada_fee_summary(text: str, max_items: int = 8) -> Tuple[str, str]:
    """Extract fee breakdown from Lazada invoice"""
    t = normalize_text(text)
    
    fee_items = []
    fee_details = []
    
    for m in RE_LAZADA_FEE_LINE.finditer(t):
        line_no = m.group(1)
        fee_desc = m.group(2).strip()
        amount = m.group(3)
        
        if not RE_LAZADA_FEE_KEYWORDS.search(fee_desc):
            continue
        
        if any(kw in fee_desc.lower() for kw in ['total', 'รวม', 'sum', 'grand', 'including', 'vat']):
            continue
        
        parsed_amount = parse_money(amount)
        if not parsed_amount or parsed_amount == '0.00':
            continue
        
        fee_name = re.sub(r'\s{2,}', ' ', fee_desc)
        fee_name = fee_name[:60]
        
        fee_items.append(fee_name)
        fee_details.append(f"{line_no}. {fee_name}: ฿{parsed_amount}")
        
        if len(fee_items) >= max_items:
            break
    
    if not fee_items:
        return ('', '')
    
    short = 'Lazada Fees: ' + ', '.join(fee_items[:3])
    if len(fee_items) > 3:
        short += f' (+{len(fee_items)-3} more)'
    
    notes = '\n'.join(fee_details)
    
    return (short, notes)


def extract_lazada(text: str) -> Dict[str, Any]:
    """
    Extract Lazada Tax Invoice/Receipt
    
    Returns PEAK A-U formatted dict
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    row['D_vendor_code'] = 'Lazada'
    row['U_group'] = 'Marketplace Expense'
    
    vendor_tax = find_vendor_tax_id(t, 'Lazada')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    
    m = RE_LAZADA_DOC_STRICT.search(t)
    if m:
        invoice_no = m.group(1)
        row['G_invoice_no'] = invoice_no
        row['C_reference'] = invoice_no
    else:
        m = RE_LAZADA_INVOICE_NO.search(t)
        if m:
            invoice_no = m.group(1)
            row['G_invoice_no'] = invoice_no
            row['C_reference'] = invoice_no
        else:
            invoice_no = find_invoice_no(t, 'Lazada')
            if invoice_no:
                row['G_invoice_no'] = invoice_no
                row['C_reference'] = invoice_no
    
    date = ''
    m = RE_LAZADA_INVOICE_DATE.search(t)
    if m:
        date = parse_date_to_yyyymmdd(m.group(1))
    
    if not date:
        date = find_best_date(t)
    
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    period_text = ''
    m = RE_LAZADA_PERIOD.search(t)
    if m:
        period_start = parse_date_to_yyyymmdd(m.group(1))
        period_end = parse_date_to_yyyymmdd(m.group(2))
        if period_start and period_end:
            period_text = f"Period: {period_start[:4]}-{period_start[4:6]}-{period_start[6:]} to {period_end[:4]}-{period_end[4:6]}-{period_end[6:]}"
    
    seller_info = extract_seller_info(t)
    seller_code = seller_info['seller_code']
    
    if not seller_code:
        for m in RE_LAZADA_SELLER_CODE.finditer(t):
            code = m.group(1)
            if len(code) >= 8 and not code.isdigit():
                seller_code = code
                break
    
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
    
    short_desc, fee_notes = extract_lazada_fee_summary(t)
    
    desc_parts = []
    desc_parts.append('Lazada - Marketplace Service Fees')
    
    if short_desc:
        desc_parts.append(short_desc)
    
    if seller_code:
        desc_parts.append(f"Seller: {seller_code}")
    
    if period_text:
        desc_parts.append(period_text)
    
    if subtotal and total:
        desc_parts.append(f"Subtotal ฿{subtotal} + VAT ฿{vat} = Total ฿{total}")
    elif total:
        desc_parts.append(f"Total ฿{total}")
    
    if wht_amount:
        desc_parts.append(f"WHT {wht_rate}: ฿{wht_amount}")
    
    row['L_description'] = ' | '.join(desc_parts)
    
    note_parts = []
    
    if seller_code:
        note_parts.append(f"Seller Code: {seller_code}")
    
    if period_text:
        note_parts.append(period_text)
    
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