"""
Invoice Extractor - Fixed Version
Supports: SPX Express, TikTok Shop, Shopee, Lazada

Fixed import issues - all imports now use relative imports (.common)
Includes backward compatibility for generic.py extractor
"""
from .common import (
    detect_platform_vendor,
    extract_amounts,
    # Backward compatibility functions
    find_tax_id,
    find_branch,
    find_first_date,
    find_invoice_no,
    find_total_amount,
    base_row_dict,
)
from .spx import extract_spx
from .tiktok import extract_tiktok
from .shopee import extract_shopee
from .lazada import extract_lazada

__version__ = '2.0.2'
__all__ = [
    'detect_platform_vendor',
    'extract_amounts',
    # Backward compatibility
    'find_tax_id',
    'find_branch',
    'find_first_date',
    'find_invoice_no',
    'find_total_amount',
    'base_row_dict',
    # Extractors
    'extract_spx',
    'extract_tiktok',
    'extract_shopee',
    'extract_lazada',
]