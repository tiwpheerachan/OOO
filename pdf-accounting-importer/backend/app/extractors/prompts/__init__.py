# -*- coding: utf-8 -*-
"""
prompts package

We store prompt text in .txt files so it's editable without touching code.
This __init__ loads them safely.
"""

from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)


def _read(name: str) -> str:
    """Read prompt file safely"""
    base = os.path.dirname(__file__)
    path = os.path.join(base, name)
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                logger.warning(f"Prompt file {name} is empty")
            return content
    except FileNotFoundError:
        logger.warning(f"Prompt file not found: {name}")
        return ""
    except Exception as e:
        logger.error(f"Error reading prompt {name}: {e}")
        return ""


# ============================================================
# English Prompts
# ============================================================

ROUTER_SYSTEM = _read("router_system.txt")
ROUTER_USER = _read("router_user.txt")

META_ADS_USER = _read("meta_ads_user.txt")
GOOGLE_ADS_USER = _read("google_ads_user.txt")
GENERIC_DOC_USER = _read("generic_doc_user.txt")


# ============================================================
# ✅ Thai Prompts (เพิ่มใหม่)
# ============================================================

UNKNOWN_GENERAL_TH = _read("unknown_general_th.txt")
GENERIC_INVOICE_TH = _read("generic_invoice_th.txt")
PLATFORM_GENERIC_TH = _read("platform_generic_th.txt")
META_ADS_STATEMENT_TH = _read("meta_ads_statement_th.txt")


# ============================================================
# Prompt Selection Helper
# ============================================================

def get_prompt_for_route(route_name: str, lang: str = "en") -> str:
    """
    Get appropriate prompt based on route and language
    
    Args:
        route_name: meta_ads | google_ads | generic | marketplace
        lang: en | th
    
    Returns:
        Prompt text
    """
    
    if lang == "th":
        # Thai prompts
        if route_name == "meta_ads":
            return META_ADS_STATEMENT_TH or META_ADS_USER
        elif route_name == "google_ads":
            return GOOGLE_ADS_USER  # No Thai version yet
        elif route_name == "marketplace":
            return PLATFORM_GENERIC_TH or GENERIC_DOC_USER
        else:  # generic / unknown
            return UNKNOWN_GENERAL_TH or GENERIC_DOC_USER
    
    else:  # English (default)
        if route_name == "meta_ads":
            return META_ADS_USER
        elif route_name == "google_ads":
            return GOOGLE_ADS_USER
        else:
            return GENERIC_DOC_USER


__all__ = [
    # English
    "ROUTER_SYSTEM",
    "ROUTER_USER",
    "META_ADS_USER",
    "GOOGLE_ADS_USER",
    "GENERIC_DOC_USER",
    
    # Thai
    "UNKNOWN_GENERAL_TH",
    "GENERIC_INVOICE_TH",
    "PLATFORM_GENERIC_TH",
    "META_ADS_STATEMENT_TH",
    
    # Helper
    "get_prompt_for_route",
]