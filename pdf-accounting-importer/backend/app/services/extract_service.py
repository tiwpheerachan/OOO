from __future__ import annotations

from typing import Dict, Any, Tuple, List, Callable
import os
import logging
import inspect
import re

from .classifier import classify_platform

from ..extractors.generic import extract_generic
from ..extractors.shopee import extract_shopee
from ..extractors.lazada import extract_lazada
from ..extractors.tiktok import extract_tiktok

# ✅ spx optional
try:
    from ..extractors.spx import extract_spx  # type: ignore
except Exception:  # pragma: no cover
    extract_spx = None  # type: ignore

# ✅ vendor code mapping (Cxxxxx)
try:
    from ..extractors.vendor_mapping import get_vendor_code, detect_client_from_context
    _VENDOR_MAPPING_OK = True
except Exception:  # pragma: no cover
    get_vendor_code = None  # type: ignore
    detect_client_from_context = None  # type: ignore
    _VENDOR_MAPPING_OK = False

from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

from .ai_extract_service import extract_with_ai

logger = logging.getLogger(__name__)

# ============================================================
# 🔥 PEAK columns lock (A-U) — อย่าให้คอลัมน์เลื่อนอีก
# ============================================================
# ✅ เพิ่ม A_company_name ต่อจาก A_seq
PEAK_KEYS_ORDER: List[str] = [
    "A_seq",
    "A_company_name",
    "B_doc_date",
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "H_invoice_date",
    "I_tax_purchase_date",
    "J_price_type",
    "K_account",
    "L_description",
    "M_qty",
    "N_unit_price",
    "O_vat_rate",
    "P_wht",
    "Q_payment_method",
    "R_paid_amount",
    "S_pnd",
    "T_note",
    "U_group",
]

# กัน AI / extractor เขียนทับ key ที่ทำให้ “เลื่อนช่อง”
_AI_BLACKLIST_KEYS = {"T_note", "U_group", "K_account"}

# คีย์ภายในที่อนุญาตให้ผ่านได้ (metadata)
_INTERNAL_OK_PREFIXES = ("_",)

# ✅ whitespace compact for ref/invoice
_RE_ALL_WS = re.compile(r"\s+")


# ============================================================
# helpers: safe merge + sanitize
# ============================================================

def _sanitize_incoming_row(d: Any) -> Dict[str, Any]:
    """รับเฉพาะ dict เท่านั้น"""
    return d if isinstance(d, dict) else {}


def _compact_no_ws(v: Any) -> str:
    """
    ✅ ตัด whitespace ทั้งหมด (space/newline/tab) ให้ token ติดกัน
    เช่น "RCSPX...-25 1218-0001" -> "RCSPX...-251218-0001"
    """
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return _RE_ALL_WS.sub("", s)


def _sanitize_ai_row(ai: Dict[str, Any]) -> Dict[str, Any]:
    """
    - ตัด key ต้องห้าม: T_note, U_group, K_account
    - ตัด None/"" ออก
    - รับเฉพาะ key ที่อยู่ใน PEAK_KEYS_ORDER หรือเป็น _meta
    """
    if not ai:
        return {}

    cleaned: Dict[str, Any] = {}
    for k, v in ai.items():
        if not k:
            continue
        if k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        # allow PEAK keys (รวม A_company_name ด้วยแล้ว)
        if k in PEAK_KEYS_ORDER:
            cleaned[k] = v
            continue

        # allow internal meta keys
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            cleaned[k] = v
            continue

        # ignore everything else
        continue

    return cleaned


def _merge_rows(
    base: Dict[str, Any],
    patch: Dict[str, Any],
    *,
    fill_missing: bool = True,
) -> Dict[str, Any]:
    """
    merge patch into base:
    - fill_missing=True  -> เติมเฉพาะช่องว่าง / 0 / 0.00
    - fill_missing=False -> ทับเฉพาะ key ที่ patch ส่งมา (แต่ไม่ทับ blacklist)
    """
    if not patch:
        return base

    out = dict(base)

    for k, v in patch.items():
        if not k:
            continue
        if k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        if fill_missing:
            cur = out.get(k, "")
            if cur in ("", None, "0", "0.00"):
                out[k] = v
        else:
            out[k] = v

    return out


# ============================================================
# helpers: validation
# ============================================================

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


# ============================================================
# helpers: extractor call (backward compatible)
# ============================================================

def _safe_call_extractor(
    fn: Callable[..., Dict[str, Any]],
    text: str,
    *,
    filename: str = "",
    client_tax_id: str = "",
) -> Dict[str, Any]:
    """
    เรียก extractor แบบ backward-compatible:
    - ถ้ารองรับ filename/client_tax_id -> ส่งให้
    - ถ้าไม่รองรับ -> fallback fn(text)
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters

        kwargs: Dict[str, Any] = {}
        if "filename" in params:
            kwargs["filename"] = filename
        if "client_tax_id" in params and client_tax_id:
            kwargs["client_tax_id"] = client_tax_id

        if kwargs:
            return fn(text, **kwargs)  # type: ignore[arg-type]
    except Exception:
        pass

    # fallback tries
    if client_tax_id:
        try:
            return fn(text, client_tax_id=client_tax_id)  # type: ignore
        except TypeError:
            pass

    return fn(text)  # type: ignore


# ============================================================
# ✅ Vendor code mapping pass (force D_vendor_code = Cxxxxx)
# ============================================================

def _apply_vendor_code_mapping(row: Dict[str, Any], text: str, client_tax_id: str) -> Dict[str, Any]:
    """
    Force D_vendor_code to be Cxxxxx using vendor_mapping.py
    """
    if not isinstance(row, dict):
        return row

    if not _VENDOR_MAPPING_OK or get_vendor_code is None:
        return row

    ctax = (client_tax_id or "").strip()
    if not ctax and detect_client_from_context is not None:
        try:
            ctax = detect_client_from_context(text) or ""
        except Exception:
            ctax = ""

    if not ctax:
        return row

    vtax = str(row.get("E_tax_id_13") or "").strip()
    vname = str(row.get("D_vendor_code") or "").strip()

    try:
        code = get_vendor_code(client_tax_id=ctax, vendor_tax_id=vtax, vendor_name=vname)
    except Exception:
        return row

    if isinstance(code, str) and code.startswith("C") and len(code) >= 5:
        row["D_vendor_code"] = code
        if os.getenv("STORE_VENDOR_MAPPING_META", "1") == "1":
            row["_client_tax_id_used"] = ctax
            row["_vendor_tax_id_used"] = vtax or ""
            row["_vendor_code_resolved"] = code

    return row


# ============================================================
# 🔥 FINAL LOCK: force PEAK schema + fix Marketplace group
# ============================================================

def lock_peak_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    1) ล็อกให้มีแต่คอลัมน์ PEAK + _meta
    2) ใส่คีย์ที่ขาดให้ครบด้วยค่า "" (กัน CSV/Excel เลื่อน)
    3) ห้ามเอา key แปลก ๆ มาปน
    """
    safe = _sanitize_incoming_row(row)

    out: Dict[str, Any] = {}

    # Keep internal meta first
    for k, v in safe.items():
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            out[k] = v

    # Lock PEAK keys (รวม A_company_name แล้ว)
    for k in PEAK_KEYS_ORDER:
        out[k] = safe.get(k, "")

    return out


def enforce_marketplace_group(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """
    ✅ บังคับท้ายสุด:
    - U_group ต้องเป็น Marketplace Expense สำหรับ marketplace docs
    - ห้ามไปอยู่ K_account
    """
    p = (platform or "").lower().strip()
    desc = str(row.get("L_description", "") or "").strip()

    is_marketplace = p in ("shopee", "lazada", "tiktok", "spx") or (desc == "Marketplace Expense")
    if is_marketplace:
        row["U_group"] = "Marketplace Expense"

        if str(row.get("K_account", "") or "").strip() == "Marketplace Expense":
            row["K_account"] = ""

    return row


def _finalize_row(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """
    Final sanitize:
    - P_wht ว่าง (คุณไม่ต้องการบันทึก)
    - T_note ว่าง
    - sync C/G + compact no whitespace
    - force U_group rule
    - lock schema
    """
    # ✅ WHT must be empty (requirement #5)
    row["P_wht"] = ""

    # ✅ notes must be empty
    if os.getenv("FORCE_EMPTY_NOTE", "1") == "1":
        row["T_note"] = ""

    # ✅ sync references
    if not row.get("C_reference") and row.get("G_invoice_no"):
        row["C_reference"] = row.get("G_invoice_no", "")
    if not row.get("G_invoice_no") and row.get("C_reference"):
        row["G_invoice_no"] = row.get("C_reference", "")

    # ✅ compact reference / invoice (requirement #6)
    row["C_reference"] = _compact_no_ws(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_no_ws(row.get("G_invoice_no", ""))

    # ✅ enforce marketplace group last
    row = enforce_marketplace_group(row, platform)

    # ✅ lock columns last (กันเลื่อน)
    row = lock_peak_columns(row)

    return row


def _record_ai_error(row: Dict[str, Any], stage: str, exc: Exception) -> None:
    if os.getenv("STORE_AI_ERROR_META", "1") != "1":
        return
    msg = f"{stage}: {type(exc).__name__}: {str(exc)}"
    msg = msg[:500]
    arr = row.get("_ai_errors")
    if not isinstance(arr, list):
        arr = []
    arr.append(msg)
    row["_ai_errors"] = arr


# ============================================================
# 🔥 MAIN ENTRY
# ============================================================

def extract_row_from_text(
    text: str,
    filename: str = "",
    client_tax_id: str = "",
) -> Tuple[str, Dict[str, Any], List[str]]:
    """
    return: platform, row, errors
    """

    # 1) classify
    try:
        platform = classify_platform(text)
    except Exception as e:
        logger.exception("classify_platform failed: %s", e)
        platform = "generic"

    # 2) extractor baseline
    try:
        if platform == "shopee":
            row = _safe_call_extractor(extract_shopee, text, filename=filename, client_tax_id=client_tax_id)
        elif platform == "lazada":
            row = _safe_call_extractor(extract_lazada, text, filename=filename, client_tax_id=client_tax_id)
        elif platform == "tiktok":
            row = _safe_call_extractor(extract_tiktok, text, filename=filename, client_tax_id=client_tax_id)
        elif platform == "spx" and extract_spx is not None:
            row = _safe_call_extractor(extract_spx, text, filename=filename, client_tax_id=client_tax_id)
        else:
            row = _safe_call_extractor(extract_generic, text, filename=filename, client_tax_id=client_tax_id)
    except Exception as e:
        logger.exception("Extractor error (platform=%s, file=%s)", platform, filename)
        row = extract_generic(text)
        row["_extractor_error"] = f"{type(e).__name__}: {str(e)}"[:500]

    row = _sanitize_incoming_row(row)

    # 3) AI ENHANCEMENT
    if os.getenv("ENABLE_AI_EXTRACT", "0") == "1":
        try:
            try:
                ai_raw = extract_with_ai(text, filename=filename, client_tax_id=client_tax_id)
            except TypeError:
                ai_raw = extract_with_ai(text, filename=filename)

            ai_row = _sanitize_ai_row(_sanitize_incoming_row(ai_raw))
            fill_missing = os.getenv("AI_FILL_MISSING", "1") == "1"
            row = _merge_rows(row, ai_row, fill_missing=fill_missing)
        except Exception as e:
            logger.warning("AI extract failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_extract", e)

    # 4) validate
    errors = _validate_row(row)

    # 5) AI REPAIR PASS
    if errors and os.getenv("AI_REPAIR_PASS", "0") == "1":
        try:
            prompt = text + "\n\n# VALIDATION_ERRORS\n" + "\n".join(errors)
            try:
                ai_fix_raw = extract_with_ai(prompt, filename=filename, client_tax_id=client_tax_id)
            except TypeError:
                ai_fix_raw = extract_with_ai(prompt, filename=filename)

            ai_fix = _sanitize_ai_row(_sanitize_incoming_row(ai_fix_raw))
            row = _merge_rows(row, ai_fix, fill_missing=False)
            errors = _validate_row(row)
        except Exception as e:
            logger.warning("AI repair failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_repair", e)

    # 6) ✅ Vendor code mapping pass (ก่อน finalize/lock)
    row = _apply_vendor_code_mapping(row, text, client_tax_id)

    # 7) FINALIZE (กันเลื่อน + compact + P_wht empty)
    row = _finalize_row(row, platform)

    return platform, row, errors
