# backend/app/services/platform_constants.py
"""
Platform Constants - Shared across all services

✅ Purpose:
- Single source of truth for platform definitions
- Prevent circular imports between job_service ↔ job_worker
- Used by: classifier, router, AI service, export, job_service, job_worker

✅ Features:
- 8 platforms (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
- Platform groups for expense categorization
- Legacy platform mapping for backward compatibility
"""
from typing import Set, Dict

# ============================================================
# Valid Platforms (8 platforms)
# ============================================================

VALID_PLATFORMS: Set[str] = {
    "META",
    "GOOGLE",
    "SHOPEE",
    "LAZADA",
    "TIKTOK",
    "SPX",
    "THAI_TAX",
    "UNKNOWN",
}

# ============================================================
# Platform Groups (for expense categorization)
# ============================================================

PLATFORM_GROUPS: Dict[str, str] = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Delivery/Logistics Expense",
    "THAI_TAX": "General Expense",
    "UNKNOWN": "Other Expense",
}

# ============================================================
# Legacy Platform Mapping (backward compatibility)
# ============================================================

LEGACY_PLATFORM_MAP: Dict[str, str] = {
    "shopee": "SHOPEE",
    "lazada": "LAZADA",
    "tiktok": "TIKTOK",
    "spx": "SPX",
    "ads": "UNKNOWN",  # Generic ads (not specific)
    "other": "UNKNOWN",
    "unknown": "UNKNOWN",
}

# ============================================================
# Platform Vendors (for auto-correction)
# ============================================================

PLATFORM_VENDORS: Dict[str, str] = {
    "META": "Meta Platforms Ireland",
    "GOOGLE": "Google Asia Pacific",
    "SHOPEE": "Shopee",
    "LAZADA": "Lazada",
    "TIKTOK": "TikTok",
    "SPX": "Shopee Express",
    "THAI_TAX": "",  # Variable
    "UNKNOWN": "",
}

# ============================================================
# Platform VAT Rules (for auto-correction)
# ============================================================

PLATFORM_VAT_RULES: Dict[str, Dict[str, str]] = {
    "META": {
        "J_price_type": "3",
        "O_vat_rate": "NO",
    },
    "GOOGLE": {
        "J_price_type": "3",
        "O_vat_rate": "NO",
    },
    "SHOPEE": {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    },
    "LAZADA": {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    },
    "TIKTOK": {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    },
    "SPX": {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    },
    "THAI_TAX": {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    },
}

# ============================================================
# Helper Functions
# ============================================================

def normalize_platform(p: str) -> str:
    """
    Normalize platform to valid UPPERCASE platform
    
    Args:
        p: Platform name (any case)
    
    Returns:
        Valid platform (UPPERCASE) or empty string if invalid
    
    Examples:
        - "shopee" → "SHOPEE"
        - "meta" → "META"
        - "ads" → "UNKNOWN"
        - "invalid" → ""
    """
    p_raw = str(p or "").strip()
    if not p_raw:
        return ""
    
    # Try uppercase first (exact match)
    p_upper = p_raw.upper()
    if p_upper in VALID_PLATFORMS:
        return p_upper
    
    # Try legacy mapping
    p_lower = p_raw.lower()
    if p_lower in LEGACY_PLATFORM_MAP:
        return LEGACY_PLATFORM_MAP[p_lower]
    
    # Invalid platform
    return ""


def is_valid_platform(p: str) -> bool:
    """
    Check if platform is valid
    
    Args:
        p: Platform name
    
    Returns:
        True if valid, False otherwise
    """
    normalized = normalize_platform(p)
    return bool(normalized)


def get_platform_group(platform: str) -> str:
    """
    Get platform group for expense categorization
    
    Args:
        platform: Platform name
    
    Returns:
        Platform group name or "Other Expense" if unknown
    """
    p = normalize_platform(platform)
    return PLATFORM_GROUPS.get(p, "Other Expense")


def get_platform_vendor(platform: str) -> str:
    """
    Get platform vendor name
    
    Args:
        platform: Platform name
    
    Returns:
        Vendor name or empty string
    """
    p = normalize_platform(platform)
    return PLATFORM_VENDORS.get(p, "")


def get_platform_vat_rules(platform: str) -> Dict[str, str]:
    """
    Get platform VAT rules
    
    Args:
        platform: Platform name
    
    Returns:
        Dict with J_price_type and O_vat_rate
    """
    p = normalize_platform(platform)
    return PLATFORM_VAT_RULES.get(p, {
        "J_price_type": "1",
        "O_vat_rate": "7%",
    })


__all__ = [
    "VALID_PLATFORMS",
    "PLATFORM_GROUPS",
    "LEGACY_PLATFORM_MAP",
    "PLATFORM_VENDORS",
    "PLATFORM_VAT_RULES",
    "normalize_platform",
    "is_valid_platform",
    "get_platform_group",
    "get_platform_vendor",
    "get_platform_vat_rules",
]