from __future__ import annotations
from typing import Dict, Any, Tuple, List
import os

from .classifier import classify_platform

from ..extractors.generic import extract_generic
from ..extractors.shopee import extract_shopee
from ..extractors.lazada import extract_lazada
from ..extractors.tiktok import extract_tiktok

from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

# ✅ AI extractor
from .ai_extract_service import extract_with_ai


# -------------------------------------------------
# helpers
# -------------------------------------------------
def _merge_rows(
    base: Dict[str, Any],
    ai: Dict[str, Any],
    fill_missing: bool = True,
) -> Dict[str, Any]:
    """
    merge AI result into base result
    - fill_missing=True → เติมเฉพาะช่องว่าง / 0
    - fill_missing=False → AI ทับทั้งหมด
    """
    if not ai:
        return base

    if not fill_missing:
        return ai

    for k, v in ai.items():
        if v in ("", None):
            continue
        if k not in base:
            base[k] = v
        else:
            if base[k] in ("", "0", "0.00", None):
                base[k] = v
    return base


def _validate_row(row: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if not validate_yyyymmdd(row.get("B_doc_date", "")):
        errors.append("วันที่เอกสารรูปแบบไม่ถูกต้อง")
    if row.get("H_invoice_date") and not validate_yyyymmdd(row.get("H_invoice_date", "")):
        errors.append("วันที่ใบกำกับฯรูปแบบไม่ถูกต้อง")
    if row.get("I_tax_purchase_date") and not validate_yyyymmdd(row.get("I_tax_purchase_date", "")):
        errors.append("วันที่ภาษีซื้อรูปแบบไม่ถูกต้อง")

    if row.get("F_branch_5") and not validate_branch5(row.get("F_branch_5", "")):
        errors.append("เลขสาขาไม่ใช่ 5 หลัก")

    if row.get("E_tax_id_13") and not validate_tax13(row.get("E_tax_id_13", "")):
        errors.append("เลขภาษีไม่ใช่ 13 หลัก")

    if not validate_price_type(row.get("J_price_type", "")):
        errors.append("ประเภทราคาไม่ถูกต้อง")

    if not validate_vat_rate(row.get("O_vat_rate", "")):
        errors.append("อัตราภาษีไม่ถูกต้อง")

    return errors


# -------------------------------------------------
# 🔥 MAIN ENTRY
# -------------------------------------------------
def extract_row_from_text(text: str, filename: str = "") -> Tuple[str, Dict[str, Any], List[str]]:
    """
    return:
      platform, row, errors
    """

    # ---------------------------------------------
    # 1) classify platform
    # ---------------------------------------------
    platform = classify_platform(text)

    # ---------------------------------------------
    # 2) rule-based extractor (เร็ว + baseline)
    # ---------------------------------------------
    if platform == "shopee":
        row = extract_shopee(text)
    elif platform == "lazada":
        row = extract_lazada(text)
    elif platform == "tiktok":
        row = extract_tiktok(text)
    else:
        row = extract_generic(text)

    # ---------------------------------------------
    # 3) AI ENHANCEMENT (อ่านทั้งเอกสาร)
    # ---------------------------------------------
    if os.getenv("ENABLE_AI_EXTRACT", "0") == "1":
        try:
            ai_row = extract_with_ai(text, filename=filename)

            fill_missing = os.getenv("AI_FILL_MISSING", "1") == "1"
            row = _merge_rows(row, ai_row, fill_missing=fill_missing)

        except Exception as e:
            # ❗ AI พังไม่ให้ระบบล่ม
            row.setdefault("T_note", "")
            row["T_note"] += f" | AI error: {str(e)}"

    # ---------------------------------------------
    # 4) validation
    # ---------------------------------------------
    errors = _validate_row(row)

    # ---------------------------------------------
    # 5) AI REPAIR PASS (ถ้ามี error)
    # ---------------------------------------------
    if errors and os.getenv("AI_REPAIR_PASS", "0") == "1":
        try:
            ai_fixed = extract_with_ai(
                text + "\n\n# VALIDATION_ERRORS\n" + "\n".join(errors),
                filename=filename,
            )
            row = _merge_rows(row, ai_fixed, fill_missing=False)
            errors = _validate_row(row)
        except Exception as e:
            row.setdefault("T_note", "")
            row["T_note"] += f" | AI repair error: {str(e)}"

    return platform, row, errors
