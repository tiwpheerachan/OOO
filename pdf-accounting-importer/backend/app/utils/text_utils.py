# backend/app/utils/text_utils.py
"""
Text utilities for extractors
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

# Thai digit mapping
THAI_DIGITS = "๐๑๒๓๔๕๖๗๘๙"
ARABIC_DIGITS = "0123456789"
THAI_TO_ARABIC = str.maketrans(THAI_DIGITS, ARABIC_DIGITS)


def normalize_text(text: Optional[str]) -> str:
    """
    Normalize text for extraction:
    - Convert Thai digits to Arabic
    - Normalize Unicode
    - Fix common OCR errors
    - Preserve structure (newlines)
    
    Args:
        text: Raw text from PDF
    
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    # Convert to string
    text = str(text)
    
    # Thai digits → Arabic
    text = text.translate(THAI_TO_ARABIC)
    
    # Normalize Unicode (NFC form)
    text = unicodedata.normalize('NFC', text)
    
    # Fix common OCR errors
    text = text.replace('ๆ', '')  # Thai repeat character
    text = text.replace('\x00', '')  # Null bytes
    
    # Normalize whitespace (but keep newlines)
    lines = text.split('\n')
    normalized_lines = []
    for line in lines:
        # Collapse multiple spaces
        line = re.sub(r'[ \t]+', ' ', line)
        # Trim line
        line = line.strip()
        if line:  # Skip empty lines
            normalized_lines.append(line)
    
    return '\n'.join(normalized_lines)


def clean_number_string(s: str) -> str:
    """
    Clean number string: remove commas, spaces, currency symbols
    
    Args:
        s: Number string like "1,234.56" or "฿1234"
    
    Returns:
        Clean number string like "1234.56"
    """
    if not s:
        return ""
    
    s = str(s).strip()
    
    # Remove common symbols
    s = s.replace(',', '')
    s = s.replace(' ', '')
    s = s.replace('฿', '')
    s = s.replace('THB', '')
    s = s.replace('Baht', '')
    
    # Keep only digits and decimal point
    s = re.sub(r'[^\d.]', '', s)
    
    return s


def extract_thai_text(text: str) -> str:
    """
    Extract only Thai characters from text
    
    Args:
        text: Mixed text
    
    Returns:
        Thai text only
    """
    if not text:
        return ""
    
    # Thai Unicode range: \u0E00-\u0E7F
    thai_chars = re.findall(r'[\u0E00-\u0E7F\s]+', text)
    
    return ' '.join(thai_chars).strip()


def is_thai_text(text: str, threshold: float = 0.3) -> bool:
    """
    Check if text contains significant Thai content
    
    Args:
        text: Text to check
        threshold: Minimum ratio of Thai characters (0.0-1.0)
    
    Returns:
        True if text has enough Thai characters
    """
    if not text:
        return False
    
    text = str(text).strip()
    if len(text) < 3:
        return False
    
    # Count Thai characters
    thai_count = len(re.findall(r'[\u0E00-\u0E7F]', text))
    total_chars = len([c for c in text if not c.isspace()])
    
    if total_chars == 0:
        return False
    
    ratio = thai_count / total_chars
    return ratio >= threshold


__all__ = [
    'normalize_text',
    'clean_number_string',
    'extract_thai_text',
    'is_thai_text',
]