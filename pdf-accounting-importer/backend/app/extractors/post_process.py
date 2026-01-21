# -*- coding: utf-8 -*-
# backend/app/extractors/post_process.py
from __future__ import annotations

import os
import re
from typing import Dict, Any, Tuple

from .common import (
    format_peak_row,
    parse_money,
)

# ============================================================
# GL Code mapping (จากรูปของคุณ)
# ============================================================
# key: platform_key -> { client_tax_id -> gl_code }
# platform_key ที่ใช้ใน post-process:
# - "shopee_mkp"  (Marketplace Expense - Shopee)
# - "lazada_mkp"
# - "tiktok_mkp"
# - "spx_mkp"
# - "google_ads"
# - "meta_ads"
# - "tiktok_ads"
# - "canva_ads"
GL_CODE_MAP: Dict[str, Dict[str, str]] = {
    # Marketplace Expense
    "shopee_mkp": {
        "0105563022918": "520317",  # SHD
        "0105561071873": "520315",  # Rabbit
        "0105565027615": "520314",  # TopOne
    },
    "lazada_mkp": {
        "0105563022918": "520318",
        "0105561071873": "520316",
        "0105565027615": "520315",
    },
    "tiktok_mkp": {
        "0105563022918": "520319",
        "0105561071873": "520317",
        "0105565027615": "520316",
    },

    # Ads
    "google_ads": {
        "0105563022918": "520201",
        "0105561071873": "520201",
        "0105565027615": "520201",
    },
    "meta_ads": {
        "0105563022918": "520202",
        "0105561071873": "520202",
        "0105565027615": "520202",
    },
    "tiktok_ads": {
        "0105563022918": "520223",
        "0105561071873": "520223",
        "0105565027615": "520221",
    },
    "canva_ads": {
        "0105563022918": "520224",
        "0105561071873": "na",
        "0105565027615": "na",
    },

    # SPX (ส่วนใหญ่ GL เป็น Marketplace Expense เหมือนกัน)
    "spx_mkp": {
        "0105563022918": "520317",
        "0105561071873": "520315",
        "0105565027615": "520314",
    },
}

# ============================================================
# Description templates (ตามรูปของคุณ)
# ============================================================

def _desc_marketplace(platform_label: str, seller_id: str, username: str, file_full: str) -> str:
    # Record Marketplace Expense - Shopee - Seller ID <<xxxxxx>> - <<Username>> - <<File Name>>
    sid = (seller_id or "").strip() or "UNKNOWN_SELLER"
    un = (username or "").strip() or "UNKNOWN_USERNAME"
    return f"Record Marketplace Expense - {platform_label} - Seller ID {sid} - {un} - {file_full}"

def _desc_ads(platform_label: str, brand_or_hint: str, payment_no: str, payment_method: str, file_full: str) -> str:
    # Record Ads - Google - <<Brand>> - Payment number <<...>> - Payment method <<...>> - <<File>>
    b = (brand_or_hint or "").strip() or "UNKNOWN"
    pn = (payment_no or "").strip() or "UNKNOWN"
    pm = (payment_method or "").strip() or "UNKNOWN"
    return f"Record Ads - {platform_label} - {b} - Payment number {pn} - Payment method {pm} - {file_full}"

def _desc_ads_meta(platform_label: str, brand_or_hint: str, account_id: str, transaction_id: str, file_full: str) -> str:
    b = (brand_or_hint or "").strip() or "UNKNOWN"
    aid = (account_id or "").strip() or "UNKNOWN"
    tid = (transaction_id or "").strip() or "UNKNOWN"
    return f"Record Ads - {platform_label} - {b} - Account ID {aid} - Transaction ID {tid} - {file_full}"

def _desc_ads_tiktok(platform_label: str, brand_or_hint: str, contract_no: str, file_full: str) -> str:
    b = (brand_or_hint or "").strip() or "UNKNOWN"
    cn = (contract_no or "").strip() or "UNKNOWN"
    return f"Record Ads - {platform_label} - {b} - Contract No. {cn} - {file_full}"


# ============================================================
# Filename → reference extraction
# ============================================================
# ตัวอย่าง filename:
#   Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf -> TRSPEMKP00-00000-251203-0012589
#   SPX Express-RCT-RCSPXSPR00-00000-251220-0001652.pdf -> RCSPXSPR00-00000-251220-0001652
#   TTSTH20250008665805.pdf -> TTSTH20250008665805
#   Lazada...THMPTIxxxxxxxxxxxxxxxx.pdf -> THMPTI...
_RE_REF_TRS    = re.compile(r"(TRS[A-Z0-9]+-\d{5}-\d{6}-\d{7,})", re.IGNORECASE)
_RE_REF_RCS    = re.compile(r"(RCS[A-Z0-9]+-\d{5}-\d{6}-\d{7,})", re.IGNORECASE)
_RE_REF_TTSTH  = re.compile(r"(TTSTH\d{10,})", re.IGNORECASE)
_RE_REF_THMPTI = re.compile(r"(THMPTI\d{16,})", re.IGNORECASE)

_RE_ALL_WS = re.compile(r"\s+")


def extract_reference_from_filename(filename: str) -> str:
    """
    บังคับ C_reference / G_invoice_no ให้ตรงกัน โดยยึด "เลขอ้างอิงหลัก" จากชื่อไฟล์
    - คืนค่า ref แบบไม่มี whitespace
    """
    if not filename:
        return ""
    base = os.path.basename(filename)
    stem, _ext = os.path.splitext(base)

    # พยายามจับ pattern หลักก่อน
    for rx in (_RE_REF_TRS, _RE_REF_RCS, _RE_REF_TTSTH, _RE_REF_THMPTI):
        m = rx.search(stem)
        if m:
            return _RE_ALL_WS.sub("", m.group(1))

    # fallback: คืน stem ทั้งก้อน (ตัด whitespace)
    return _RE_ALL_WS.sub("", stem)


def infer_doc_date_from_reference(ref: str) -> str:
    """
    ดึงวันที่จาก ref แบบ ...-YYMMDD-...  -> YYYYMMDD
    ตัวอย่าง: TRSPE...-251203-0012589 -> 20251203
    """
    if not ref:
        return ""
    m = re.search(r"-(\d{6})-", ref)
    if not m:
        return ""
    yymmdd = m.group(1)
    try:
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
    except Exception:
        return ""
    if not (1 <= mm <= 12 and 1 <= dd <= 31):
        return ""
    yyyy = 2000 + yy
    return f"{yyyy:04d}{mm:02d}{dd:02d}"


# ============================================================
# Platform key inference (บังคับ K_account + description)
# ============================================================

def infer_platform_key(platform: str, group: str, filename: str) -> str:
    """
    platform: ค่า label จาก job_worker/classifier เช่น "shopee", "lazada", "tiktok", "spx", "ads", ...
    group: row["U_group"] เช่น "Marketplace Expense" / "Advertising Expense"
    filename: ใช้ช่วยเดาอีกชั้น
    """
    p = (platform or "").strip().lower()
    g = (group or "").strip().lower()
    name = (filename or "").lower()

    # marketplace group
    if "marketplace" in g:
        if "spx" in p or "spx" in name:
            return "spx_mkp"
        if "shopee" in p or "shopee" in name:
            return "shopee_mkp"
        if "lazada" in p or "lazada" in name:
            return "lazada_mkp"
        if "tiktok" in p or "tiktok" in name:
            return "tiktok_mkp"

    # ads group
    if "advertising" in g or "ads" in g:
        if "google" in p or "adwords" in name or "google" in name:
            return "google_ads"
        if "meta" in p or "facebook" in name or "meta" in name:
            return "meta_ads"
        if "tiktok" in p or "tiktok" in name:
            return "tiktok_ads"
        if "canva" in p or "canva" in name:
            return "canva_ads"

    # fallback by platform text
    if "spx" in p:
        return "spx_mkp"
    if "shopee" in p:
        return "shopee_mkp"
    if "lazada" in p:
        return "lazada_mkp"
    if "tiktok" in p:
        return "tiktok_mkp"

    return ""


# ============================================================
# GL / Amount / Description enforcers
# ============================================================

def apply_gl_code(row: Dict[str, Any], client_tax_id: str, platform_key: str) -> None:
    """
    apply K_account from GL_CODE_MAP
    """
    cid = re.sub(r"\D+", "", client_tax_id or "")
    if not platform_key or not cid:
        return
    mp = GL_CODE_MAP.get(platform_key) or {}
    gl = mp.get(cid) or ""
    if gl and gl.lower() != "na":
        row["K_account"] = gl


def _safe_money_str(v: Any) -> str:
    try:
        return parse_money(v or "") or ""
    except Exception:
        return ""


def enforce_amounts(row: Dict[str, Any]) -> None:
    """
    กัน “ตัวเลขมั่ว/เลื่อนคอลัมน์”:
    - บังคับ N_unit_price / R_paid_amount เป็น format เงินถูก
    - ถ้ามี R แต่ N ว่าง -> set N=R
    - ถ้ามี N แต่ R ว่าง -> set R=N
    - ถ้าว่างทั้งคู่ -> 0
    """
    n = _safe_money_str(row.get("N_unit_price", ""))
    r = _safe_money_str(row.get("R_paid_amount", ""))

    if not n and r:
        n = r
    if not r and n:
        r = n

    row["N_unit_price"] = n or "0"
    row["R_paid_amount"] = r or "0"


def apply_description_template(
    row: Dict[str, Any],
    platform: str,
    platform_key: str,
    filename: str,
) -> None:
    """
    บังคับ L_description เป็น pattern ตามรูป:
    - Shopee/Lazada/TikTok/SPX marketplace: Record Marketplace Expense - ...
    - Ads: Record Ads - ...
    """
    file_full = os.path.basename(filename or "").strip() or (filename or "").strip() or "UNKNOWN_FILE"

    # meta ที่ extractor อาจยัดมาไว้ให้ (แล้วเราจะ pop ออก ไม่ให้หลุดไป CSV)
    seller_id = (row.pop("_seller_id", "") or "").strip()
    username = (row.pop("_username", "") or "").strip()

    # Ads meta (ถ้ามี)
    brand = (row.pop("_brand", "") or "").strip()
    payment_no = (row.pop("_payment_no", "") or "").strip()
    payment_method = (row.pop("_payment_method", "") or "").strip()
    account_id = (row.pop("_account_id", "") or "").strip()
    transaction_id = (row.pop("_transaction_id", "") or "").strip()
    contract_no = (row.pop("_contract_no", "") or "").strip()

    # marketplace
    if platform_key in ("shopee_mkp", "lazada_mkp", "tiktok_mkp", "spx_mkp"):
        label = (
            "Shopee" if platform_key == "shopee_mkp" else
            "Lazada" if platform_key == "lazada_mkp" else
            "TikTok" if platform_key == "tiktok_mkp" else
            "SPX" if platform_key == "spx_mkp" else
            (platform or "UNKNOWN")
        )
        row["L_description"] = _desc_marketplace(label, seller_id, username, file_full)
        return

    # ads
    if platform_key == "google_ads":
        row["L_description"] = _desc_ads("Google", brand, payment_no, payment_method, file_full)
        return
    if platform_key == "meta_ads":
        row["L_description"] = _desc_ads_meta("Meta", brand, account_id, transaction_id, file_full)
        return
    if platform_key == "tiktok_ads":
        row["L_description"] = _desc_ads_tiktok("TikTok", brand, contract_no, file_full)
        return
    if platform_key == "canva_ads":
        b = brand or "UNKNOWN"
        row["L_description"] = f"Record Ads - Canva - {b} - {file_full}"
        return

    # fallback: ถ้าไม่มี template ให้คงเดิม แต่ห้ามว่าง
    if not (row.get("L_description") or "").strip():
        row["L_description"] = (row.get("U_group") or "Expense")


def _enforce_reference(row: Dict[str, Any], filename: str) -> str:
    """
    enforce C_reference/G_invoice_no from filename (Plan C)
    and infer doc dates if missing.
    """
    ref = extract_reference_from_filename(filename)
    if ref:
        row["C_reference"] = ref
        row["G_invoice_no"] = ref

        d = infer_doc_date_from_reference(ref)
        if d:
            if not (row.get("B_doc_date") or "").strip():
                row["B_doc_date"] = d
            if not (row.get("H_invoice_date") or "").strip():
                row["H_invoice_date"] = d
            if not (row.get("I_tax_purchase_date") or "").strip():
                row["I_tax_purchase_date"] = d
    return ref


# ============================================================
# Public API
# ============================================================

def post_process_peak_row(
    row: Dict[str, Any],
    *,
    platform: str,
    filename: str,
    client_tax_id: str,
    text: str = "",
) -> Dict[str, Any]:
    """
    ✅ จุดเดียวจบสำหรับบังคับ:
    - C_reference / G_invoice_no = reference จาก filename (Plan C)
    - K_account = GL ตาม client + platform_key
    - L_description = template
    - enforce amounts (N/R)
    - enforce defaults (U_group/L_description)
    - ตัด key meta (_seller_id, _username, ...) ออกไม่ให้หลุดไป output
    - format_peak_row() ปิดท้าย

    หมายเหตุ:
    - ไม่ไปยุ่ง policy P_wht โดยตรง (ให้ extractor/ระบบหลักกำหนด)
    - ปลอดภัย: ไม่มี exception หลุดออก
    """
    try:
        row = row or {}

        # 0) sanitize client_tax_id
        cid = re.sub(r"\D+", "", client_tax_id or "")

        # 1) enforce reference + dates from filename
        _enforce_reference(row, filename)

        # 2) infer platform_key (เพื่อ GL + description)
        platform_key = infer_platform_key(platform, row.get("U_group", "") or "", filename)

        # 3) GL code
        apply_gl_code(row, cid, platform_key)

        # 4) description template
        apply_description_template(row, platform, platform_key, filename)

        # 5) enforce amounts safe
        enforce_amounts(row)

        # 6) enforce group defaults (กันหลุดว่าง)
        if not (row.get("U_group") or "").strip():
            # ถ้าดันไม่รู้ group ให้ default เป็น Marketplace Expense
            row["U_group"] = "Marketplace Expense"
        if not (row.get("L_description") or "").strip():
            row["L_description"] = row["U_group"]

        # 7) hard rules: whitespace-free C/G (แม้ filename ไม่มีช่องว่าง ก็กันไว้)
        row["C_reference"] = _RE_ALL_WS.sub("", str(row.get("C_reference", "") or ""))
        row["G_invoice_no"] = _RE_ALL_WS.sub("", str(row.get("G_invoice_no", "") or ""))

        # 8) ensure C/G both present if either present
        if not row.get("C_reference") and row.get("G_invoice_no"):
            row["C_reference"] = row["G_invoice_no"]
        if not row.get("G_invoice_no") and row.get("C_reference"):
            row["G_invoice_no"] = row["C_reference"]

        # 9) final
        return format_peak_row(row)

    except Exception:
        # fail-safe: never crash, still format
        try:
            row = row or {}
            row["C_reference"] = _RE_ALL_WS.sub("", str(row.get("C_reference", "") or ""))
            row["G_invoice_no"] = _RE_ALL_WS.sub("", str(row.get("G_invoice_no", "") or ""))
            enforce_amounts(row)
            if not (row.get("U_group") or "").strip():
                row["U_group"] = "Marketplace Expense"
            if not (row.get("L_description") or "").strip():
                row["L_description"] = row["U_group"]
            return format_peak_row(row)
        except Exception:
            return format_peak_row({})


__all__ = [
    "post_process_peak_row",
    "extract_reference_from_filename",
    "infer_platform_key",
    "apply_gl_code",
    "apply_description_template",
    "enforce_amounts",
]
