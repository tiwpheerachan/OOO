# backend/app/extractors/spx.py
"""
SPX Express extractor - PEAK A-U format
Supports: Shipping fee receipts with 1% WHT
"""
from __future__ import annotations

import re
from typing import Dict, Any

from .common import (
    base_row_dict,
    normalize_text,
    find_vendor_tax_id,
    find_branch,
    find_invoice_no,
    find_best_date,
    extract_seller_info,
    extract_amounts,
    find_payment_method,
    format_peak_row,
    parse_money,
)

# SPX-specific patterns
RE_SPX_RECEIPT_NO = re.compile(
    r'(?:เลขที่|No\.?)\s*[:#：]?\s*(RCS[A-Z0-9\-/]+)',
    re.IGNORECASE
)

RE_SPX_SELLER_ID = re.compile(
    r'Seller\s*ID\s*[:#：]?\s*(\d+)',
    re.IGNORECASE
)

RE_SPX_USERNAME = re.compile(
    r'Username\s*[:#：]?\s*([A-Za-z0-9_\-]+)',
    re.IGNORECASE
)

RE_SPX_SHIPPING_FEE = re.compile(
    r'Shipping\s*fee\s+\d+\s+([0-9.]+)\s+([0-9.]+)',
    re.IGNORECASE
)

RE_SPX_TOTAL_AMOUNT = re.compile(
    r'(?:จำนวนเงินรวม|Total\s*amount)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE
)

RE_SPX_WHT = re.compile(
    r'หักภาษีเงินได้\s*ณ\s*ที่จ่าย(?:ใน)?อัตรา(?:ร้อย)?ละ\s*(\d+)\s*%\s*เป็นจำนวนเงิน\s*([0-9.]+)',
    re.IGNORECASE
)

RE_SPX_WHT_EN = re.compile(
    r'deducted\s+(\d+)%\s+withholding\s+tax.*?at\s+([0-9.]+)\s+THB',
    re.IGNORECASE
)


def extract_spx(text: str) -> Dict[str, Any]:
    """
    Extract SPX Express receipt/invoice
    
    SPX Express is a shipping service provider
    Documents are typically shipping fee receipts with 1% WHT
    
    Returns PEAK A-U formatted dict
    """
    t = normalize_text(text)
    row = base_row_dict()
    
    # ========== Vendor Information ==========
    row['D_vendor_code'] = 'SPX'
    row['U_group'] = 'Shipping Expense'
    
    # Vendor Tax ID (SPX Express)
    vendor_tax = find_vendor_tax_id(t, 'SPX')
    if vendor_tax:
        row['E_tax_id_13'] = vendor_tax
    
    # Branch (usually Head Office)
    branch = find_branch(t)
    if branch:
        row['F_branch_5'] = branch
    
    # ========== Document Information ==========
    # Receipt number
    m = RE_SPX_RECEIPT_NO.search(t)
    if m:
        receipt_no = m.group(1)
        row['G_invoice_no'] = receipt_no
        row['C_reference'] = receipt_no
    else:
        invoice_no = find_invoice_no(t, 'SPX')
        if invoice_no:
            row['G_invoice_no'] = invoice_no
            row['C_reference'] = invoice_no
    
    # Date
    date = find_best_date(t)
    if date:
        row['B_doc_date'] = date
        row['H_invoice_date'] = date
        row['I_tax_purchase_date'] = date
    
    # ========== Seller/Shop Information ==========
    seller_info = extract_seller_info(t)
    
    seller_id = seller_info['seller_id']
    if not seller_id:
        m = RE_SPX_SELLER_ID.search(t)
        if m:
            seller_id = m.group(1)
    
    username = seller_info['username']
    if not username:
        m = RE_SPX_USERNAME.search(t)
        if m:
            username = m.group(1)
    
    # ========== Amounts ==========
    m = RE_SPX_SHIPPING_FEE.search(t)
    if m:
        unit_price = parse_money(m.group(1))
        amount = parse_money(m.group(2))
        if unit_price:
            row['N_unit_price'] = unit_price
        if amount:
            row['R_paid_amount'] = amount
    
    m = RE_SPX_TOTAL_AMOUNT.search(t)
    if m:
        total = parse_money(m.group(1))
        if total:
            if not row['R_paid_amount'] or row['R_paid_amount'] == '0':
                row['R_paid_amount'] = total
            if not row['N_unit_price'] or row['N_unit_price'] == '0':
                row['N_unit_price'] = total
    
    if not row['N_unit_price'] or row['N_unit_price'] == '0':
        amounts = extract_amounts(t)
        if amounts['subtotal']:
            row['N_unit_price'] = amounts['subtotal']
        elif amounts['total']:
            row['N_unit_price'] = amounts['total']
        
        if amounts['total']:
            row['R_paid_amount'] = amounts['total']
    
    # ========== WHT (Withholding Tax) ==========
    wht_rate = ''
    wht_amount = ''
    
    m = RE_SPX_WHT.search(t)
    if m:
        wht_rate = f"{m.group(1)}%"
        wht_amount = parse_money(m.group(2))
    
    if not wht_amount:
        m = RE_SPX_WHT_EN.search(t)
        if m:
            wht_rate = f"{m.group(1)}%"
            wht_amount = parse_money(m.group(2))
    
    if not wht_amount:
        amounts = extract_amounts(t)
        if amounts['wht_amount']:
            wht_amount = amounts['wht_amount']
        if amounts['wht_rate']:
            wht_rate = amounts['wht_rate']
    
    if wht_amount:
        row['P_wht'] = wht_amount
        row['S_pnd'] = '53'
    
    # ========== VAT ==========
    amounts = extract_amounts(t)
    if amounts['vat']:
        row['O_vat_rate'] = '7%'
        row['J_price_type'] = '1'
    else:
        row['O_vat_rate'] = 'NO'
        row['J_price_type'] = '3'
    
    # ========== Description ==========
    desc_parts = ['SPX Express - Shipping Fee']
    
    if seller_id:
        desc_parts.append(f"Seller ID: {seller_id}")
    
    if username:
        desc_parts.append(f"Shop: {username}")
    
    if wht_rate:
        desc_parts.append(f"WHT {wht_rate}: {wht_amount}")
    
    row['L_description'] = ' | '.join(desc_parts)
    
    # ========== Notes ==========
    note_parts = []
    
    if seller_id:
        note_parts.append(f"Seller ID: {seller_id}")
    if username:
        note_parts.append(f"Username: {username}")
    
    if wht_rate and wht_amount:
        note_parts.append(f"Withholding Tax {wht_rate}: ฿{wht_amount}")
    
    if note_parts:
        row['T_note'] = '\n'.join(note_parts)
    
    # ========== Payment Method ==========
    row['Q_payment_method'] = 'หักจากยอดขาย'
    
    # ========== Other Fields ==========
    row['M_qty'] = '1'
    row['K_account'] = ''
    
    return format_peak_row(row)