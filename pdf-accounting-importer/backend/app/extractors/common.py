# backend/app/extractors/common.py
"""
Common utilities and patterns for invoice extraction
Enhanced with AI-ready patterns for better accuracy
"""
from __future__ import annotations

import re
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime
from decimal import Decimal, InvalidOperation

# ============================================================
# Text normalization utilities
# ============================================================

def normalize_text(text: str) -> str:
    """Normalize text for pattern matching"""
    if not text:
        return ""
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove zero-width characters
    text = re.sub(r'[\u200b-\u200f\ufeff]', '', text)
    return text.strip()

def fmt_tax_13(raw: str) -> str:
    """Format to 13-digit tax ID (0105561071873)"""
    if not raw:
        return ""
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 13:
        return digits
    return ""

def fmt_branch_5(raw: str) -> str:
    """Format to 5-digit branch code (00000)"""
    if not raw:
        return "00000"
    digits = re.sub(r'\D', '', raw)
    if digits == "":
        return "00000"
    return digits.zfill(5)[:5]

def parse_date_to_yyyymmdd(date_str: str) -> str:
    """
    Parse various date formats to YYYYMMDD
    Supports: DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, YYYYMMDD, etc.
    """
    if not date_str:
        return ""
    
    date_str = date_str.strip()
    
    # Try YYYYMMDD format first
    if re.match(r'^\d{8}$', date_str):
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return date_str
        except:
            pass
    
    # Try various formats
    formats = [
        '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
        '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
        '%d/%m/%y', '%d-%m-%y',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Handle 2-digit years
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.strftime('%Y%m%d')
        except:
            continue
    
    return ""

def parse_en_date(date_str: str) -> str:
    """Parse English date formats like 'Dec 9, 2025' to YYYYMMDD"""
    if not date_str:
        return ""
    
    date_str = date_str.strip()
    formats = ['%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y']
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y%m%d')
        except:
            continue
    
    return ""

def parse_money(value: str) -> str:
    """
    Parse money string to decimal format
    Removes commas, handles Thai baht symbol
    Returns string with 2 decimal places or empty if invalid
    """
    if not value:
        return ""
    
    # Remove currency symbols and whitespace
    value = str(value).replace('฿', '').replace('THB', '').replace(',', '').strip()
    
    try:
        amount = Decimal(value)
        if amount < 0:
            return ""
        return f"{amount:.2f}"
    except (InvalidOperation, ValueError):
        return ""

# ============================================================
# Core patterns (enhanced)
# ============================================================

# Tax ID patterns
RE_TAX13 = re.compile(r'(\d[\d\s-]{11,20}\d)')
RE_TAX13_STRICT = re.compile(r'\b([0-9]{13})\b')

# Branch patterns
RE_BRANCH_HEAD_OFFICE = re.compile(
    r'(?:สำนักงานใหญ่|Head\s*Office|HeadOffice|本社)',
    re.IGNORECASE
)
RE_BRANCH_NUM = re.compile(
    r'(?:สาขา(?:ที่)?\s*|Branch\s*(?:No\.?|Number)?\s*:?\s*)(\d{1,5})',
    re.IGNORECASE
)

# Date patterns
RE_DATE_DMYYYY = re.compile(r'(\d{1,2})[\-/\. ](\d{1,2})[\-/\. ](\d{4})')
RE_DATE_YYYYMD = re.compile(r'(\d{4})[\-/\. ](\d{1,2})[\-/\. ](\d{1,2})')
RE_DATE_8DIGIT = re.compile(r'\b(\d{8})\b')
RE_DATE_EN = re.compile(
    r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})\b',
    re.IGNORECASE
)

# Invoice/Document number patterns
RE_INVOICE_GENERIC = re.compile(
    r'(?:ใบกำกับ(?:ภาษี)?|Tax\s*Invoice|Invoice|เลขที่(?:เอกสาร)?|Document\s*(?:No\.?|Number)|Doc\s*No\.?|Receipt\s*No\.?)'
    r'\s*[:#：]?\s*\"?\s*([A-Za-z0-9\-/_.]+)',
    re.IGNORECASE
)

# Platform-specific document numbers
RE_SPX_DOC = re.compile(
    r'\b(RCS[A-Z0-9\-/]{10,})\b',
    re.IGNORECASE
)
RE_SHOPEE_DOC = re.compile(
    r'\b((?:Shopee-)?TI[VR]-[A-Z0-9]+-\d{5}-\d{6}-\d{7,}|TRS[A-Z0-9\-_/]{8,})\b',
    re.IGNORECASE
)
RE_LAZADA_DOC = re.compile(
    r'\b(THMPTI\d{16}|(?:LAZ|LZD)[A-Z0-9\-_/.]{6,}|INV[A-Z0-9\-_/.]{6,})\b',
    re.IGNORECASE
)
RE_TIKTOK_DOC = re.compile(
    r'\b(TTSTH\d{14,})\b',
    re.IGNORECASE
)

# Seller ID / Wallet code patterns
RE_SELLER_ID = re.compile(
    r'(?:Seller\s*ID|Shop\s*ID|Store\s*ID|รหัสร้านค้า)\s*[:#：]?\s*([A-Z0-9_\-]+)',
    re.IGNORECASE
)
RE_USERNAME = re.compile(
    r'(?:Username|User\s*name|ชื่อผู้ใช้)\s*[:#：]?\s*([A-Za-z0-9_\-]+)',
    re.IGNORECASE
)
RE_SELLER_CODE = re.compile(
    r'\b([A-Z0-9]{8,15})\b'
)

# Amount patterns (enhanced)
RE_TOTAL_INC_VAT = re.compile(
    r'(?:Total\s*(?:amount)?\s*(?:\()?(?:Including|incl\.?|รวม)\s*(?:VAT|Tax|ภาษี)(?:\))?'
    r'|Grand\s*Total|Amount\s*Due|Total\s*Due|ยอด(?:ที่)?ชำระ|ยอดรวมทั้งสิ้น)'
    r'\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE
)

RE_TOTAL_EX_VAT = re.compile(
    r'(?:Total\s*(?:amount)?\s*(?:\()?(?:Excluding|excl\.?|ก่อน|ไม่รวม)\s*(?:VAT|Tax|ภาษี)(?:\))?'
    r'|Subtotal|รวม(?:เงิน)?ก่อน(?:VAT|ภาษี)|จำนวนเงิน(?:รวม)?)'
    r'\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE
)

RE_VAT_AMOUNT = re.compile(
    r'(?:Total\s*VAT|VAT\s*(?:7%)?|ภาษีมูลค่าเพิ่ม(?:\s*7%)?)'
    r'\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE
)

RE_WHT_AMOUNT = re.compile(
    r'(?:ภาษี(?:เงินได้)?(?:หัก)?(?:\s*ณ\s*ที่จ่าย)?|Withholding\s*Tax|WHT|Withheld\s*Tax)'
    r'(?:.*?(?:อัตรา|rate|ร้อยละ)\s*([0-9]{1,2})\s*%)?'
    r'(?:.*?(?:จำนวน(?:เงิน)?|amounting\s*to|เป็นจำนวน))?\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    re.IGNORECASE | re.DOTALL
)

# Payment method patterns
RE_PAYMENT_METHOD = re.compile(
    r'\b(EWL\d{2,6}|TRF\d{2,6}|CSH\d{2,6}|QR|CASH|CARD|TRANSFER|BANK\s*TRANSFER|CREDIT\s*CARD'
    r'|หักจาก(?:ยอด)?ขาย|โอน|เงินสด)\b',
    re.IGNORECASE
)

# Vendor detection patterns
RE_VENDOR_SHOPEE = re.compile(r'(?:Shopee|ช็อปปี้)', re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r'(?:Lazada|ลาซาด้า)', re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r'(?:TikTok|ติ๊กต๊อก)', re.IGNORECASE)
RE_VENDOR_SPX = re.compile(r'(?:SPX\s*Express|Standard\s*Express)', re.IGNORECASE)

# ============================================================
# Row template (PEAK A-U format)
# ============================================================

def base_row_dict() -> Dict[str, Any]:
    """
    PEAK A-U format template
    ใช้สำหรับ import เข้าระบบบัญชี PEAK
    """
    return {
        'A_seq': '1',
        'B_doc_date': '',
        'C_reference': '',
        'D_vendor_code': '',
        'E_tax_id_13': '',
        'F_branch_5': '',
        'G_invoice_no': '',
        'H_invoice_date': '',
        'I_tax_purchase_date': '',
        'J_price_type': '1',
        'K_account': '',
        'L_description': '',
        'M_qty': '1',
        'N_unit_price': '0',
        'O_vat_rate': '7%',
        'P_wht': '0',
        'Q_payment_method': '',
        'R_paid_amount': '0',
        'S_pnd': '',
        'T_note': '',
        'U_group': '',
    }

# ============================================================
# Enhanced extraction functions
# ============================================================

def detect_platform_vendor(text: str) -> Tuple[str, str]:
    """
    Detect platform/vendor from document text
    Returns: (vendor_full_name, vendor_code)
    """
    t = normalize_text(text)
    
    if RE_VENDOR_SPX.search(t):
        return ('SPX Express (Thailand) Co., Ltd.', 'SPX')
    if RE_VENDOR_SHOPEE.search(t):
        return ('Shopee (Thailand) Co., Ltd.', 'Shopee')
    if RE_VENDOR_LAZADA.search(t):
        return ('Lazada Limited', 'Lazada')
    if RE_VENDOR_TIKTOK.search(t):
        return ('TikTok Shop (Thailand) Ltd.', 'TikTok')
    
    return ('', '')

def find_vendor_tax_id(text: str, vendor_code: str = '') -> str:
    """Extract vendor/seller tax ID (not customer tax ID)"""
    t = normalize_text(text)
    
    # Vendor-specific patterns
    if vendor_code == 'SPX':
        m = re.search(r'Tax\s*ID\s*No\.?\s*([0-9]{13})', t, re.IGNORECASE)
        if m:
            return m.group(1)
    
    elif vendor_code == 'TikTok':
        m = re.search(r'Tax\s*Registration\s*Number\s*[:#：]?\s*([0-9]{13})', t, re.IGNORECASE)
        if m:
            return m.group(1)
    
    elif vendor_code in ('Shopee', 'Lazada'):
        patterns = [
            r'(?:เลขประจำตัวผู้เสียภาษี(?:อากร)?|Tax\s*(?:ID|Registration)\s*(?:No\.?|Number)?)\s*[:#：]?\s*([0-9]{13})',
        ]
        
        for pattern in patterns:
            m = re.search(pattern, t, re.IGNORECASE)
            if m:
                tax_id = m.group(1)
                # Exclude known customer tax IDs
                if tax_id not in ['0105561071873', '0105565027615']:
                    return tax_id
    
    # Generic fallback
    m = RE_TAX13_STRICT.search(t)
    if m:
        return m.group(1)
    
    return ""

def find_branch(text: str) -> str:
    """Extract branch code (00000 for head office)"""
    t = normalize_text(text)
    
    if RE_BRANCH_HEAD_OFFICE.search(t):
        return "00000"
    
    m = RE_BRANCH_NUM.search(t)
    if m:
        return fmt_branch_5(m.group(1))
    
    return "00000"

def find_invoice_no(text: str, platform: str = '') -> str:
    """Extract invoice/document number"""
    t = normalize_text(text)
    
    # Platform-specific patterns first
    if platform == 'SPX':
        m = RE_SPX_DOC.search(t)
        if m:
            return m.group(1)
    elif platform == 'Shopee':
        m = RE_SHOPEE_DOC.search(t)
        if m:
            return m.group(1)
    elif platform == 'Lazada':
        m = RE_LAZADA_DOC.search(t)
        if m:
            return m.group(1)
    elif platform == 'TikTok':
        m = RE_TIKTOK_DOC.search(t)
        if m:
            return m.group(1)
    
    # Try all platform patterns
    for pattern in [RE_SPX_DOC, RE_SHOPEE_DOC, RE_LAZADA_DOC, RE_TIKTOK_DOC]:
        m = pattern.search(t)
        if m:
            return m.group(1)
    
    # Generic invoice pattern
    m = RE_INVOICE_GENERIC.search(t)
    if m:
        return m.group(1).strip('"')
    
    return ""

def find_best_date(text: str) -> str:
    """Find best date from text - Returns YYYYMMDD format"""
    t = normalize_text(text)
    
    candidates = []
    
    # English date format
    for m in RE_DATE_EN.finditer(t):
        date_str = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
        yyyymmdd = parse_en_date(date_str)
        if yyyymmdd:
            candidates.append(yyyymmdd)
    
    # YYYY-MM-DD format
    for m in RE_DATE_YYYYMD.finditer(t):
        date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        yyyymmdd = parse_date_to_yyyymmdd(date_str)
        if yyyymmdd:
            candidates.append(yyyymmdd)
    
    # DD/MM/YYYY format
    for m in RE_DATE_DMYYYY.finditer(t):
        date_str = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
        yyyymmdd = parse_date_to_yyyymmdd(date_str)
        if yyyymmdd:
            candidates.append(yyyymmdd)
    
    # 8-digit format
    for m in RE_DATE_8DIGIT.finditer(t):
        yyyymmdd = parse_date_to_yyyymmdd(m.group(1))
        if yyyymmdd:
            candidates.append(yyyymmdd)
    
    if candidates:
        return max(candidates)
    
    return ""

def extract_seller_info(text: str) -> Dict[str, str]:
    """Extract seller/shop information"""
    t = normalize_text(text)
    info = {'seller_id': '', 'username': '', 'seller_code': ''}
    
    m = RE_SELLER_ID.search(t)
    if m:
        info['seller_id'] = m.group(1)
    
    m = RE_USERNAME.search(t)
    if m:
        info['username'] = m.group(1)
    
    for m in RE_SELLER_CODE.finditer(t):
        code = m.group(1)
        if not code.isdigit() and len(code) >= 8:
            info['seller_code'] = code
            break
    
    return info

def extract_amounts(text: str) -> Dict[str, str]:
    """Extract financial amounts from document"""
    t = normalize_text(text)
    amounts = {
        'subtotal': '',
        'vat': '',
        'total': '',
        'wht_rate': '',
        'wht_amount': '',
    }
    
    m = RE_TOTAL_INC_VAT.search(t)
    if m:
        amounts['total'] = parse_money(m.group(1))
    
    m = RE_TOTAL_EX_VAT.search(t)
    if m:
        amounts['subtotal'] = parse_money(m.group(1))
    
    m = RE_VAT_AMOUNT.search(t)
    if m:
        amounts['vat'] = parse_money(m.group(1))
    
    m = RE_WHT_AMOUNT.search(t)
    if m:
        if m.lastindex >= 2:
            if m.group(1):
                amounts['wht_rate'] = f"{m.group(1)}%"
            amounts['wht_amount'] = parse_money(m.group(2))
        else:
            amounts['wht_amount'] = parse_money(m.group(1) if m.lastindex == 1 else m.group(2))
    
    # Calculate missing values
    if amounts['subtotal'] and amounts['vat'] and not amounts['total']:
        try:
            sub = Decimal(amounts['subtotal'])
            vat = Decimal(amounts['vat'])
            amounts['total'] = f"{(sub + vat):.2f}"
        except:
            pass
    
    if amounts['total'] and amounts['vat'] and not amounts['subtotal']:
        try:
            total = Decimal(amounts['total'])
            vat = Decimal(amounts['vat'])
            amounts['subtotal'] = f"{(total - vat):.2f}"
        except:
            pass
    
    if amounts['subtotal'] and not amounts['vat']:
        try:
            sub = Decimal(amounts['subtotal'])
            vat = sub * Decimal('0.07')
            amounts['vat'] = f"{vat:.2f}"
            if not amounts['total']:
                amounts['total'] = f"{(sub + vat):.2f}"
        except:
            pass
    
    return amounts

def find_payment_method(text: str, platform: str = '') -> str:
    """Extract payment method"""
    t = normalize_text(text)
    
    if platform in ('Shopee', 'Lazada', 'TikTok'):
        if 'หักจาก' in t or 'deduct' in t.lower():
            return 'หักจากยอดขาย'
    
    m = RE_PAYMENT_METHOD.search(t)
    if m:
        return m.group(1).upper().replace(' ', '')
    
    return ''

# ============================================================
# Validation and formatting
# ============================================================

def validate_tax_id(tax_id: str) -> bool:
    """Validate 13-digit tax ID"""
    if not tax_id or len(tax_id) != 13:
        return False
    return tax_id.isdigit()

def validate_date(date_str: str) -> bool:
    """Validate YYYYMMDD format"""
    if not date_str or len(date_str) != 8:
        return False
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except:
        return False

def compute_wht_from_rate(subtotal: str, rate_str: str) -> str:
    """Calculate WHT amount from subtotal and rate"""
    if not subtotal or not rate_str:
        return ""
    
    try:
        amount = Decimal(subtotal)
        rate = rate_str.replace('%', '').strip()
        rate_decimal = Decimal(rate)
        
        if rate_decimal > 1:
            rate_decimal = rate_decimal / 100
        
        wht = amount * rate_decimal
        return f"{wht:.2f}"
    except:
        return ""

def format_peak_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Final formatting and validation for PEAK import"""
    formatted = base_row_dict()
    formatted.update(row)
    
    # Format numbers
    for key in ['N_unit_price', 'R_paid_amount']:
        if formatted[key] and formatted[key] != '0':
            try:
                val = Decimal(str(formatted[key]).replace(',', ''))
                formatted[key] = f"{val:.2f}"
            except:
                formatted[key] = '0'
    
    # Format WHT
    if formatted['P_wht'] and formatted['P_wht'] != '0':
        wht = formatted['P_wht']
        if '%' in str(wht) and formatted['N_unit_price'] != '0':
            formatted['P_wht'] = compute_wht_from_rate(
                formatted['N_unit_price'], 
                str(wht)
            )
    
    # Set PND for WHT
    if formatted['P_wht'] and formatted['P_wht'] != '0' and not formatted['S_pnd']:
        formatted['S_pnd'] = '53'
    
    # Validate dates
    for date_field in ['B_doc_date', 'H_invoice_date', 'I_tax_purchase_date']:
        if formatted[date_field] and not validate_date(formatted[date_field]):
            formatted[date_field] = ''
    
    # Sync G and C if empty
    if not formatted['C_reference'] and formatted['G_invoice_no']:
        formatted['C_reference'] = formatted['G_invoice_no']
    if not formatted['G_invoice_no'] and formatted['C_reference']:
        formatted['G_invoice_no'] = formatted['C_reference']
    
    return formatted

# ============================================================
# Backward Compatibility Functions (for generic.py)
# ============================================================

def find_tax_id(text: str) -> str:
    """
    Backward compatibility wrapper for find_vendor_tax_id
    Used by generic.py extractor
    """
    # Try to find any 13-digit tax ID
    t = normalize_text(text)
    m = RE_TAX13_STRICT.search(t)
    if m:
        return m.group(1)
    return ""

def find_first_date(text: str) -> str:
    """
    Backward compatibility wrapper for find_best_date
    Used by generic.py extractor
    """
    return find_best_date(text)

def find_total_amount(text: str) -> str:
    """
    Extract total amount from text
    Used by generic.py extractor
    """
    t = normalize_text(text)
    
    # Try total including VAT first
    m = RE_TOTAL_INC_VAT.search(t)
    if m:
        return parse_money(m.group(1))
    
    # Try total excluding VAT
    m = RE_TOTAL_EX_VAT.search(t)
    if m:
        return parse_money(m.group(1))
    
    # Generic amount pattern
    patterns = [
        r'(?:Total|รวม|Grand\s*Total|Amount\s*Due|จำนวนเงิน)\s*[:#：]?\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
        r'฿\s*([0-9,]+\.[0-9]{2})',
    ]
    
    for pattern in patterns:
        m = re.search(pattern, t, re.IGNORECASE)
        if m:
            amount = parse_money(m.group(1))
            if amount and amount != '0.00':
                return amount
    
    return ""