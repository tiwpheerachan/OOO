# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Optional

CLIENT_SHD     = "0105563022918"
CLIENT_RABBIT  = "0105561071873"
CLIENT_TOPONE  = "0105565027615"

# ========== GL CODE MATRIX (ตามรูป) ==========
GL_MATRIX: Dict[str, Dict[str, str]] = {
    # Marketplace expense
    "marketplace_shopee": {CLIENT_SHD:"520317", CLIENT_RABBIT:"520315", CLIENT_TOPONE:"520314"},
    "marketplace_lazada": {CLIENT_SHD:"520318", CLIENT_RABBIT:"520316", CLIENT_TOPONE:"520315"},
    "marketplace_tiktok": {CLIENT_SHD:"520319", CLIENT_RABBIT:"520317", CLIENT_TOPONE:"520316"},

    # Ads
    "ads_google":         {CLIENT_SHD:"520201", CLIENT_RABBIT:"520201", CLIENT_TOPONE:"520201"},
    "ads_meta":           {CLIENT_SHD:"520202", CLIENT_RABBIT:"520202", CLIENT_TOPONE:"520202"},
    "ads_tiktok":         {CLIENT_SHD:"520223", CLIENT_RABBIT:"520223", CLIENT_TOPONE:"520221"},
    "ads_canva":          {CLIENT_SHD:"520224"},  # rabbit/topone = na ตามรูป (ไม่ใส่)

    # Other
    "online_other":       {CLIENT_SHD:"520203", CLIENT_RABBIT:"520203", CLIENT_TOPONE:"520203"},
}

DESC_TEMPLATE: Dict[str, str] = {
    "marketplace_shopee": "Record Marketplace Expense - Shopee - Seller ID {seller_id} - {username} - {file}",
    "marketplace_lazada": "Record Marketplace Expense - Lazada - {username} - {period} - {file}",
    "marketplace_tiktok": "Record Marketplace Expense - Tiktok - {username} - {period} - {file}",

    "ads_google":         "Record Ads - Google - {brand} - Payment number {payment_number} - Payment method {payment_method}",
    "ads_meta":           "Record Ads - Meta - {brand} - {account_id} - Transaction ID {transaction_id} - Payment Method {payment_method}",
    "ads_tiktok":         "Record Ads - Tiktok - {brand} - Contract No.{contract_no}",
    "ads_canva":          "Record Ads - Canva Ads",

    "online_other":       "Record online expense",
}

def pick_gl_code(rule_key: str, client_tax_id: str) -> str:
    m = GL_MATRIX.get(rule_key) or {}
    return str(m.get(client_tax_id) or "")

def build_description(rule_key: str, **kw) -> str:
    tpl = DESC_TEMPLATE.get(rule_key) or ""
    try:
        return tpl.format(**kw).strip()
    except Exception:
        return tpl.strip()
