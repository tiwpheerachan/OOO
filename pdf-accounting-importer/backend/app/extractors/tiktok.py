# backend/app/extractors/tiktok.py
"""
TikTok Shop extractor - PEAK A-U format
Supports: Platform service fees with 7% VAT and 3% WHT
"""
from __future__ import annotations

import re
from typing import Dict, Any, List, Tuple

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    parse_en_date,
    extract_amounts,
    format_peak_row,
    parse_money,
)

# TikTok-specific patterns
RE_TIKTOK_INVOICE_NO = re.compile(
    r'Invoice\s*number\s*[:#：]?\s*(TTSTH\d{14,})',
    re.IGNORECASE
)

RE_TIKTOK_INVOICE_DATE = re.compile(
    r'Invoice\s*date\s*[:#：]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)

RE_TIKTOK_PERIOD = re.compile(
    r'Period\s*[:#：]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\s*[-–]\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)

RE_TIKTOK_CLIENT_NAME = re.compile(
    r'Client\s*Name\s*[:#：]?\s*(.+?)(?:\s*\(Head\s*Office\))?$',
    re.IGNORECASE | re.MULTILINE
)

RE_TIKTOK_TAX_ID = re.compile(
    r'Tax\s*ID\s*[:#：]?\s*([0-9]{13})',
    re.IGNORECASE
)

RE_TIKTOK_FEE_LINE = re.compile(
    r'^\s*-\s*(.+?)\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s+฿([0-9,]+\.[0-9]{2})\s*$',
    re.MULTILINE | re.IGNORECASE
)

RE_TIKTOK_SUBTOTAL = re.compile(
    r'Subtotal\s*\(excluding\s*VAT\)\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

RE_TIKTOK_VAT = re.compile(
    r'Total\s*VAT\s*7%\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

RE_TIKTOK_TOTAL = re.compile(
    r'Total\s*amount\s*[（(]?\s*including\s*VAT\s*[）)]?\s*฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE
)

RE_TIKTOK_WHT = re.compile(
    r'withheld\s+tax\s+at\s+the\s+rate\s+of\s+(\d+)%.*?amounting\s+to\s+฿?\s*([0-9,]+\.[0-9]{2})',
    re.IGNORECASE | re.DOTALL
)


def extract_tiktok_fee_summary(text: str, max_lines: int = 10) -> Tuple[str, str]:
    """Extract fee breakdown from TikTok invoice"""
    t = normalize_text(text)
    
    fee_items = []
    fee_details = []
    
    for m in RE_TIKTOK_FEE_LINE.finditer(t):
        fee_name = m.group(1).strip()
        ex_vat = m.group(2)
        vat = m.group(3)
        inc_vat = m.group(4)
        
        fee_items.append(fee_name)
        fee_details.append(f"- {fee_name}: ฿{ex_vat} + VAT ฿{vat} = ฿{inc_vat}")
        
        if len(fee_items) >= max_lines:
            break
    
    if not fee_items:
        return ('', '')
    
    short = 'Platform Fees: ' + ', '.join(fee_items[:3])
    if len(fee_items) > 3:
        short += f' (+{len(fee_items)-3} more)'
    
    notes = '\n'.join(fee_details)
    
    return (short, notes)


def extract_tiktok(text: str) -> Dict[str, Any]:
    """
    Extract TikTok Shop Tax Invoice/Receipt
    
    Returns PEAK A-U formatted dict
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    row['D_vendor_code'] = 'TikTok'
    row['U_group'] = 'Marketplace Expense'
    
    vendor_tax = find_vendor_tax_id(t, 'TikTok')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    
    m = RE_TIKTOK_INVOICE_NO.search(t)
    if m:
        invoice_no = m.group(1)
        row['G_invoice_no'] = invoice_no
        row['C_reference'] = invoice_no
    else:
        invoice_no = find_invoice_no(t, 'TikTok')
        if invoice_no:
            row['G_invoice_no'] = invoice_no
            row['C_reference'] = invoice_no
    
    m = RE_TIKTOK_INVOICE_DATE.search(t)
    if m:
        date_str = m.group(1)
        date = parse_en_date(date_str)
        if date:
            row['B_doc_date'] = date
            row['H_invoice_date'] = date
            row['I_tax_purchase_date'] = date
    
    if not row['B_doc_date']:
        date = find_best_date(t)
        if date:
            row['B_doc_date'] = date
            row['H_invoice_date'] = date
            row['I_tax_purchase_date'] = date
    
    period_text = ''
    m = RE_TIKTOK_PERIOD.search(t)
    if m:
        period_start = m.group(1)
        period_end = m.group(2)
        period_text = f"Period: {period_start} - {period_end}"
    
    subtotal = ''
    vat = ''
    total = ''
    
    m = RE_TIKTOK_SUBTOTAL.search(t)
    if m:
        subtotal = parse_money(m.group(1))
    
    m = RE_TIKTOK_VAT.search(t)
    if m:
        vat = parse_money(m.group(1))
    
    m = RE_TIKTOK_TOTAL.search(t)
    if m:
        total = parse_money(m.group(1))
    
    if not subtotal or not total:
        amounts = extract_amounts(t)
        subtotal = subtotal or amounts['subtotal']
        vat = vat or amounts['vat']
        total = total or amounts['total']
    
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
    
    wht_rate = ''
    wht_amount = ''
    
    m = RE_TIKTOK_WHT.search(t)
    if m:
        wht_rate = f"{m.group(1)}%"
        wht_amount = parse_money(m.group(2))
    
    if not wht_amount:
        amounts = extract_amounts(t)
        wht_amount = amounts['wht_amount']
        wht_rate = amounts['wht_rate'] or '3%'
    
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    short_desc, fee_notes = extract_tiktok_fee_summary(t)
    
    desc_parts = []
    desc_parts.append('TikTok Shop - Platform Service Fees')
    
    if short_desc:
        desc_parts.append(short_desc)
    
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
    
    m = RE_TIKTOK_CLIENT_NAME.search(t)
    if m:
        client_name = m.group(1).strip()
        note_parts.append(f"Client: {client_name}")
    
    m = RE_TIKTOK_TAX_ID.search(t)
    if m:
        tax_id = m.group(1)
        if tax_id != row['E_tax_id_13']:
            note_parts.append(f"Client Tax ID: {tax_id}")
    
    if fee_notes:
        note_parts.append('\nFee Breakdown:')
        note_parts.append(fee_notes)
    
    if period_text:
        note_parts.append(f'\n{period_text}')
    
    if wht_rate and wht_amount:
        note_parts.append(f'\nWithholding Tax {wht_rate}: ฿{wht_amount}')
    
    if note_parts:
        row['T_note'] = '\n'.join(note_parts)
    
    row['Q_payment_method'] = 'หักจากยอดขาย'
    row['K_account'] = ''
    
    return format_peak_row(row)