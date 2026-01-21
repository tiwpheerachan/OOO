# backend/app/services/ai_service.py
"""
AI Service - Enhanced Version (Multi-Platform + HARD LOCK rules)

✅ Core intent:
- ใช้ AI ช่วยเติมข้อมูลในเอกสาร "ที่ rule-based extractor ไม่รู้จัก" หรือ OCR มั่ว
- แต่ "ค่าที่ต้องล็อก" ต้องถูกบังคับหลัง AI เสมอ (post-normalization / post-lock)

✅ HARD LOCK rules from user:
1) ห้ามเดา date จากชื่อไฟล์ (เช่น 251203) -> ต้องยึด Invoice date ในเอกสารเท่านั้น
2) WHT ต้องคำนวณจาก Subtotal (ไม่ใช่ Total)
3) N_unit_price ห้ามเอา WHT ไปใส่ผิดช่อง
4) C_reference/G_invoice_no ต้องเป็น “doc+ref” และ normalize แบบ no-space
5) ต้องใช้ชื่อไฟล์เต็มๆ (basename no ext) เช่น TRSPEMKP00-00000-251203-0012589
6) L_description ต้องเป็นแพทเทิร์นตาม platform (Shopee: Record Marketplace Expense - Shopee - Seller ID ... - Username ... - File Name ...)
7) K_account ต้องล็อกตามบริษัท (client_tax_id) เช่น SHD = 520317

หมายเหตุ:
- ไฟล์นี้ "ไม่ควร" เป็นผู้คำนวณบัญชีทั้งหมดแทน extractors
  แต่สามารถทำ post-lock ให้ถูกกฎได้ เมื่อมี subtotal/total ที่ส่งมาจาก partial_row
"""

from __future__ import annotations

import json
import os
import re
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple, Set, List

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Constants / Schema (aligned with export_service.py)
# ---------------------------------------------------------------------

PEAK_FIELDS = [
    "A_seq", "A_company_name", "B_doc_date", "C_reference", "D_vendor_code",
    "E_tax_id_13", "F_branch_5", "G_invoice_no", "H_invoice_date",
    "I_tax_purchase_date", "J_price_type", "K_account", "L_description",
    "M_qty", "N_unit_price", "O_vat_rate", "P_wht", "Q_payment_method",
    "R_paid_amount", "S_pnd", "T_note", "U_group",
]

# ✅ Platform-specific "vendor label" (NOT vendor code mapping; vendor_code should come from vendor_mapping.py ideally)
PLATFORM_VENDORS = {
    "META": "Meta Platforms Ireland",
    "GOOGLE": "Google Asia Pacific",
    "SHOPEE": "Shopee",
    "LAZADA": "Lazada",
    "TIKTOK": "TikTok",
    "SPX": "Shopee Express",
    "THAI_TAX": "",   # Variable (from document)
    "UNKNOWN": "Other",
}

VENDOR_CODES: Set[str] = set(PLATFORM_VENDORS.values()) | {"Other"}

PLATFORM_VAT_RULES = {
    "META": {"J_price_type": "3", "O_vat_rate": "NO"},
    "GOOGLE": {"J_price_type": "3", "O_vat_rate": "NO"},
    "SHOPEE": {"J_price_type": "1", "O_vat_rate": "7%"},
    "LAZADA": {"J_price_type": "1", "O_vat_rate": "7%"},
    "TIKTOK": {"J_price_type": "1", "O_vat_rate": "7%"},
    "SPX": {"J_price_type": "1", "O_vat_rate": "7%"},
    "THAI_TAX": {"J_price_type": "1", "O_vat_rate": "7%"},
    "UNKNOWN": {"J_price_type": "1", "O_vat_rate": "7%"},
}

VAT_RATES: Set[str] = {"7%", "NO"}
PRICE_TYPES: Set[str] = {"1", "2", "3"}
PND_ALLOWED: Set[str] = {"", "1", "2", "3", "53"}

PLATFORM_GROUPS = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Delivery/Logistics Expense",
    "THAI_TAX": "General Expense",
    "UNKNOWN": "Other Expense",
}

GROUPS: Set[str] = {
    "Marketplace Expense",
    "Advertising Expense",
    "Delivery/Logistics Expense",
    "General Expense",
    "Inventory/COGS",
    "Other Expense",
}

CLIENT_TAX_IDS: Set[str] = {
    "0105563022918",  # SHD
    "0105561071873",  # Rabbit
    "0105565027615",  # TopOne
}

# ---------------------------------------------------------------------
# HARD-LOCK account mapping (can override by ENV JSON)
# ---------------------------------------------------------------------
DEFAULT_ACCOUNT_BY_CLIENT: Dict[str, str] = {
    "0105563022918": "520317",  # SHD GL Code
    # TODO: เติมของ Rabbit/TopOne ให้ตรงรูปคุณ (ถ้ามี)
    # "0105561071873": "xxxxx",
    # "0105565027615": "xxxxx",
}

def _load_account_by_client() -> Dict[str, str]:
    """
    Allow override via env:
      PEAK_ACCOUNT_BY_CLIENT_JSON='{"0105563022918":"520317","0105561071873":"..."}'
    """
    raw = os.getenv("PEAK_ACCOUNT_BY_CLIENT_JSON", "").strip()
    if not raw:
        return dict(DEFAULT_ACCOUNT_BY_CLIENT)
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            out = dict(DEFAULT_ACCOUNT_BY_CLIENT)
            for k, v in obj.items():
                kk = "".join(c for c in str(k) if c.isdigit())
                vv = str(v).strip()
                if kk and vv:
                    out[kk] = vv
            return out
    except Exception:
        pass
    return dict(DEFAULT_ACCOUNT_BY_CLIENT)

ACCOUNT_BY_CLIENT = _load_account_by_client()

# ---------------------------------------------------------------------
# Platform Detection Regex
# ---------------------------------------------------------------------

RE_VENDOR_META = re.compile(r"(meta\s*platforms?\s*ireland|facebook|fb\s*ads|instagram\s*ads)", re.IGNORECASE)
RE_VENDOR_GOOGLE = re.compile(r"(google\s*(?:asia|advertising|ads|adwords)|google\s*payment)", re.IGNORECASE)
RE_VENDOR_SHOPEE = re.compile(r"(shopee|ช็อปปี้|ช้อปปี้)", re.IGNORECASE)
RE_VENDOR_LAZADA = re.compile(r"(lazada|ลาซาด้า)", re.IGNORECASE)
RE_VENDOR_TIKTOK = re.compile(r"(tiktok|ติ๊กต๊อก)", re.IGNORECASE)
RE_VENDOR_SPX = re.compile(r"(spx\s*express|shopee\s*express|standard\s*express)", re.IGNORECASE)

RE_THAI_TAX = re.compile(r"(ใบเสร็จรับเงิน|ใบกำกับภาษี|tax\s*invoice|receipt)", re.IGNORECASE)

RE_TAX13 = re.compile(r"\b(\d{13})\b")
RE_BRANCH5 = re.compile(r"(?:branch|สาขา)\s*[:#]?\s*(\d{5})", re.IGNORECASE)
RE_INVOICE_NO = re.compile(r"(?:invoice|inv|เลขที่)\s*[:#]?\s*([A-Z0-9\-/]{4,})", re.IGNORECASE)

RE_HAS_VAT7 = re.compile(r"(vat\s*7%|ภาษีมูลค่าเพิ่ม\s*7%|total\s*vat|vat\s*amount)", re.IGNORECASE)
RE_NO_VAT = re.compile(r"(no\s*vat|ไม่มี\s*vat|vat\s*exempt|ยกเว้นภาษี|reverse\s*charge)", re.IGNORECASE)

RE_PAYMENT_DEDUCT = re.compile(r"(หักจากยอดขาย|deduct(?:ed)?\s*from\s*(?:sales|revenue))", re.IGNORECASE)
RE_PAYMENT_TRANSFER = re.compile(r"(โอน|transfer|bank\s*transfer)", re.IGNORECASE)
RE_PAYMENT_CARD = re.compile(r"(card|credit\s*card|visa|mastercard)", re.IGNORECASE)
RE_PAYMENT_CASH = re.compile(r"(cash|เงินสด)", re.IGNORECASE)

RE_WHT_RATE = re.compile(r"(?:อัตรา|rate|ร้อยละ)\s*([0-9]{1,2})\s*%", re.IGNORECASE)
RE_WHT_ANY = re.compile(r"(withholding|wht|หักภาษี|ณ\s*ที่จ่าย)", re.IGNORECASE)
RE_PND_HINT = re.compile(r"(ภ\.ง\.ด\.?\s*53|pnd\s*53)", re.IGNORECASE)

RE_DATE_YYYY_MM_DD = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
RE_DATE_DD_MM_YYYY = re.compile(r"(\d{2})/(\d{2})/(\d{4})")

# ชื่อไฟล์แบบที่คุณย้ำ เช่น ...-251203-...
RE_FILENAME_YYMMDD = re.compile(r"(?:^|[-_])(\d{2})(\d{2})(\d{2})(?:[-_]|$)")


# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _normalize_text(text: str) -> str:
    if not text:
        return ""
    try:
        t = str(text).replace("\r\n", "\n").replace("\r", "\n")
        t = re.sub(r"[\u200b-\u200f\ufeff]", "", t)
        lines = [re.sub(r"[ \t\f\v]+", " ", ln).strip() for ln in t.split("\n")]
        return "\n".join(lines).strip()
    except Exception as e:
        logger.warning("Text normalization error: %s", e)
        return str(text or "")

def _digits_only(v: Any) -> str:
    try:
        return "".join(c for c in str(v or "") if c.isdigit())
    except Exception:
        return ""

def _to_tax13(v: Any) -> str:
    d = _digits_only(v)
    return d[:13] if len(d) >= 13 else ""

def _to_branch5(v: Any) -> str:
    d = _digits_only(v)
    if not d:
        return "00000"
    return d.zfill(5)[:5]

def _to_money_2(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
        if not s:
            return ""
        s = s.replace("฿", "").replace("THB", "").replace("$", "").replace(",", "").strip()
        d = Decimal(s)
        if d < 0:
            return ""
        return f"{d:.2f}"
    except (InvalidOperation, ValueError):
        return ""

def _money_decimal(v: Any) -> Decimal:
    s = _to_money_2(v)
    if not s:
        return Decimal("0")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")

def _clamp_choice(v: Any, allowed: Set[str], fallback: str) -> str:
    try:
        s = "" if v is None else str(v).strip()
        return s if s in allowed else fallback
    except Exception:
        return fallback

def _first_json_object(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        s = s.strip()
        if s.startswith("{") and s.endswith("}"):
            return s
        m = re.search(r"\{[\s\S]*\}", s)
        return m.group(0) if m else None
    except Exception:
        return None

def _basename_no_ext(path: str) -> str:
    """
    Return file basename without extension. Keep FULL name.
    Example:
      Shopee-TIV-TRSPEMKP00-00000-251201-0013100.pdf
      -> Shopee-TIV-TRSPEMKP00-00000-251201-0013100
    """
    try:
        p = str(path or "").replace("\\", "/").strip()
        name = p.split("/")[-1] if p else ""
        if not name:
            return ""
        # remove last extension
        if "." in name:
            return ".".join(name.split(".")[:-1])
        return name
    except Exception:
        return ""

def _normalize_ref_no_space(s: str) -> str:
    """
    Normalize reference/invoice string:
    - remove spaces, tabs, newlines
    - keep hyphen and slash
    """
    try:
        x = (s or "").strip()
        x = re.sub(r"\s+", "", x)
        return x
    except Exception:
        return (s or "").strip()

def _build_doc_ref(source_filename: str) -> str:
    """
    Must use FULL filename (basename without ext) as doc id.
    """
    return _normalize_ref_no_space(_basename_no_ext(source_filename))

def _detect_platform(text: str, hint: str = "") -> str:
    try:
        t = _normalize_text(text)
        h = (hint or "").strip().upper()

        if h in PLATFORM_VENDORS:
            return h

        if RE_VENDOR_META.search(t):
            return "META"
        if RE_VENDOR_GOOGLE.search(t):
            return "GOOGLE"
        if RE_VENDOR_SPX.search(t):
            return "SPX"
        if RE_VENDOR_SHOPEE.search(t):
            return "SHOPEE"
        if RE_VENDOR_LAZADA.search(t):
            return "LAZADA"
        if RE_VENDOR_TIKTOK.search(t):
            return "TIKTOK"
        if RE_THAI_TAX.search(t) and RE_TAX13.search(t):
            return "THAI_TAX"
        return "UNKNOWN"
    except Exception as e:
        logger.error("Platform detection error: %s", e)
        return "UNKNOWN"

def _guess_vat(platform: str, text: str) -> Tuple[str, str]:
    try:
        if platform in PLATFORM_VAT_RULES:
            rules = PLATFORM_VAT_RULES[platform]
            return rules["J_price_type"], rules["O_vat_rate"]

        t = _normalize_text(text)
        if RE_NO_VAT.search(t):
            return "3", "NO"
        if RE_HAS_VAT7.search(t):
            return "1", "7%"
        return "1", "7%"
    except Exception:
        return "1", "7%"

def _guess_payment_method(platform: str, text: str) -> str:
    try:
        t = _normalize_text(text)
        if platform in {"META", "GOOGLE"}:
            return "CARD"
        if RE_PAYMENT_DEDUCT.search(t):
            return "หักจากยอดขาย"
        if RE_PAYMENT_TRANSFER.search(t):
            return "โอน"
        if RE_PAYMENT_CARD.search(t):
            return "CARD"
        if RE_PAYMENT_CASH.search(t):
            return "เงินสด"
        return ""
    except Exception:
        return ""

def _guess_vendor_tax_id(text: str) -> str:
    try:
        t = _normalize_text(text)
        for m in RE_TAX13.finditer(t):
            tax = m.group(1)
            if tax not in CLIENT_TAX_IDS:
                return tax
        return ""
    except Exception:
        return ""

def _guess_pnd(text: str, wht: str) -> str:
    try:
        t = _normalize_text(text)
        w = _to_money_2(wht)
        if w and w not in ("0.00", ""):
            if RE_PND_HINT.search(t):
                return "53"
            return "53"
        return ""
    except Exception:
        return ""

def _truncate_text_smart(text: str, max_len: int) -> str:
    try:
        t = (text or "").strip()
        if len(t) <= max_len:
            return t
        head = int(max_len * 0.65)
        tail = max_len - head - 40
        return t[:head] + "\n\n...<TRUNCATED>...\n\n" + t[-tail:]
    except Exception:
        return text or ""

# ---------------------------------------------------------------------
# Amount helpers for HARD RULE: WHT from SUBTOTAL
# ---------------------------------------------------------------------
def _extract_wht_rate_from_text(text: str) -> Decimal:
    """
    Find WHT rate like 3% from text. Default 0.
    """
    try:
        t = _normalize_text(text)
        # Strong hint: any wht mention
        if not RE_WHT_ANY.search(t):
            return Decimal("0")
        m = RE_WHT_RATE.search(t)
        if not m:
            return Decimal("0")
        r = Decimal(m.group(1)) / Decimal("100")
        if r < 0 or r > Decimal("0.2"):
            return Decimal("0")
        return r
    except Exception:
        return Decimal("0")

def _calc_wht_amount_from_subtotal(subtotal: Decimal, rate: Decimal) -> Decimal:
    if subtotal <= 0 or rate <= 0:
        return Decimal("0")
    amt = (subtotal * rate)
    # round 2 decimals
    return amt.quantize(Decimal("0.01"))

# ---------------------------------------------------------------------
# OpenAI API
# ---------------------------------------------------------------------
def _openai_chat_json(system: str, user: str, model: str) -> Dict[str, Any]:
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("Missing OPENAI_API_KEY")

        base_url = os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
        url = base_url.rstrip("/") + "/chat/completions"

        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

        payload: Dict[str, Any] = {
            "model": model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
        }

        timeout = float(os.getenv("OPENAI_TIMEOUT", "90") or "90")
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        content = ""
        try:
            content = data["choices"][0]["message"]["content"]
        except Exception:
            content = ""

        js = _first_json_object(content) or "{}"
        try:
            obj = json.loads(js)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    except Exception as e:
        logger.error("OpenAI API error: %s", e)
        return {}

# ---------------------------------------------------------------------
# Platform prompt (with HARD LOCK instructions)
# ---------------------------------------------------------------------
def _build_platform_specific_prompt(platform: str) -> str:
    """
    These are instruction hints; final locking is done in post-processing.
    """
    if platform == "META":
        return (
            "This is a Meta/Facebook Ads receipt.\n"
            "Rules:\n"
            "- Vendor label: Meta Platforms Ireland\n"
            "- VAT: NO, price_type=3\n"
        )
    if platform == "GOOGLE":
        return (
            "This is a Google Ads payment receipt.\n"
            "Rules:\n"
            "- Vendor label: Google Asia Pacific\n"
            "- VAT: NO, price_type=3\n"
        )
    if platform == "SPX":
        return (
            "This is Shopee Express (SPX) document.\n"
            "Rules:\n"
            "- Group: Delivery/Logistics Expense\n"
        )
    if platform == "THAI_TAX":
        return (
            "This is a Thai Tax Invoice (ใบกำกับภาษี).\n"
            "Rules:\n"
            "- Extract vendor tax id (not client) and 5-digit branch\n"
            "- Dates MUST come from the document, not filename\n"
        )
    if platform in {"SHOPEE", "LAZADA", "TIKTOK"}:
        return (
            f"This is a {platform} marketplace document.\n"
            "Rules:\n"
            "- Group: Marketplace Expense\n"
            "- Dates MUST come from the document, not filename\n"
        )
    return (
        "Unknown document.\n"
        "Rules:\n"
        "- Do not guess dates from filename\n"
    )

# ---------------------------------------------------------------------
# HARD-LOCK post processing
# ---------------------------------------------------------------------
def _lock_doc_ref_fields(cleaned: Dict[str, Any], source_filename: str) -> None:
    """
    HARD RULE:
    - C_reference/G_invoice_no ต้องเป็น “doc+ref” และ normalize no-space
    - ต้องใช้ชื่อไฟล์เต็มๆ (basename no ext) เป็น doc id หลัก
    """
    doc = _build_doc_ref(source_filename)  # FULL basename no ext, no spaces

    # existing ref/inv (AI might extract)
    c0 = _normalize_ref_no_space(str(cleaned.get("C_reference", "") or ""))
    g0 = _normalize_ref_no_space(str(cleaned.get("G_invoice_no", "") or ""))

    # doc+ref policy:
    # - If extracted ref exists and differs from doc: combine doc + ref (concatenate) (no spaces)
    # - Else use doc only
    # This ensures uniqueness and alignment, and prevents mismatch between C vs G.
    ref = c0 or g0
    if ref and doc and ref != doc:
        combined = _normalize_ref_no_space(doc + ref)
    else:
        combined = doc or ref  # fallback if filename missing

    combined = combined[:64] if combined else ""

    cleaned["C_reference"] = combined
    cleaned["G_invoice_no"] = combined

def _lock_k_account(cleaned: Dict[str, Any], client_tax_id: str) -> None:
    """
    HARD RULE: K_account locked by client tax id (company).
    - SHD: 520317
    - Others can be configured via ENV JSON
    """
    cid = _to_tax13(client_tax_id)
    acc = ACCOUNT_BY_CLIENT.get(cid, "")
    if acc:
        cleaned["K_account"] = acc

def _lock_description_pattern(cleaned: Dict[str, Any], platform: str, source_filename: str, text: str, partial_row: Dict[str, Any]) -> None:
    """
    HARD RULE: L_description pattern by platform.
    Example (Shopee):
      Record Marketplace Expense - Shopee - Seller ID <<xxxx>> - <<Username>> - <<File Name>>

    We try get seller_id/username from partial_row first, else regex from text.
    """
    p = (platform or "UNKNOWN").upper()
    fn = (source_filename or "").strip()
    if not fn:
        fn = _basename_no_ext(source_filename)

    # helper: seller id from partial_row or text
    seller_id = str(partial_row.get("seller_id") or partial_row.get("shop_id") or "").strip()
    username = str(partial_row.get("username") or partial_row.get("shop_name") or "").strip()

    if not seller_id:
        # common patterns: "Seller ID 1234567890"
        m = re.search(r"\bSeller\s*ID\s*[:#]?\s*(\d{6,20})\b", text, re.I)
        if m:
            seller_id = m.group(1).strip()

    if not username:
        # weak guess: "Username xxx" / "Shop xxx"
        m = re.search(r"\b(?:Username|User\s*Name|Shop)\s*[:#]?\s*([A-Za-z0-9_.-]{2,})\b", text, re.I)
        if m:
            username = m.group(1).strip()

    if p == "SHOPEE":
        sid = seller_id or "UNKNOWN"
        un = username or "UNKNOWN"
        cleaned["L_description"] = f"Record Marketplace Expense - Shopee - Seller ID {sid} - {un} - {fn}"
        return

    if p == "LAZADA":
        cleaned["L_description"] = f"Record Marketplace Expense - Lazada - {fn}"
        return

    if p == "TIKTOK":
        cleaned["L_description"] = f"Record Marketplace Expense - TikTok - {fn}"
        return

    if p == "SPX":
        cleaned["L_description"] = f"Record Delivery/Logistics Expense - SPX - {fn}"
        return

    if p == "META":
        cleaned["L_description"] = f"Record Advertising Expense - Meta - {fn}"
        return

    if p == "GOOGLE":
        cleaned["L_description"] = f"Record Advertising Expense - Google - {fn}"
        return

    # fallback
    if not str(cleaned.get("L_description", "") or "").strip():
        cleaned["L_description"] = f"Record Expense - {p} - {fn}"

def _guard_dates_not_from_filename(cleaned: Dict[str, Any], source_filename: str, full_text: str, notes: List[str]) -> None:
    """
    HARD RULE:
    - ห้ามเดาวันที่จากชื่อไฟล์ เช่น ...-251203-...
    เราจะตรวจว่ามี yymmdd ใน filename และถ้า AI ให้วันที่ที่ "ตรงกับ filename"
    แต่ "ไม่พบวันที่นั้นใน text" จะล้างทิ้งเพื่อบังคับให้ rule-based/AI รอบถัดไปแก้จากเอกสารจริง
    """
    base = _basename_no_ext(source_filename)
    m = RE_FILENAME_YYMMDD.search(base or "")
    if not m:
        return

    yy, mm, dd = m.group(1), m.group(2), m.group(3)
    yymmdd = f"{yy}{mm}{dd}"
    # assume 20yy...
    yyyymmdd = f"20{yymmdd}"

    # check if that date appears in doc text (either 20yymmdd or dd/mm/20yy or yyyy-mm-dd)
    t = _normalize_text(full_text)
    appears = (yyyymmdd in t) or (yymmdd in t)
    appears = appears or bool(re.search(rf"\b{dd}/{mm}/20{yy}\b", t))
    appears = appears or bool(re.search(rf"\b20{yy}-{mm}-{dd}\b", t))

    # if not appears -> wipe any matching date fields (only if equals filename date)
    for dk in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        v = str(cleaned.get(dk, "") or "").strip()
        if v == yyyymmdd and not appears:
            cleaned[dk] = ""
            notes.append(f"Date '{dk}' removed (matched filename YYMMDD but not found in document text)")

def _enforce_wht_from_subtotal(cleaned: Dict[str, Any], full_text: str, partial_row: Dict[str, Any], notes: List[str]) -> None:
    """
    HARD RULE:
    - WHT ต้องคำนวณจาก Subtotal ไม่ใช่ Total
    - N_unit_price ห้ามยัด WHT
    Implementation:
    - If partial_row provides _subtotal, use it as base for WHT amount
    - Try get rate from partial_row['_wht_rate'] else from text (3%, 5%)
    - Only override P_wht when we have a reliable subtotal base
    """
    subtotal = _money_decimal(partial_row.get("_subtotal") or partial_row.get("subtotal") or partial_row.get("amount_before_vat"))
    if subtotal <= 0:
        return

    # rate
    rate = Decimal("0")
    pr = partial_row.get("_wht_rate") or partial_row.get("wht_rate")
    if pr:
        try:
            rate = Decimal(str(pr).replace("%", "").strip()) / Decimal("100")
        except Exception:
            rate = Decimal("0")
    if rate <= 0:
        rate = _extract_wht_rate_from_text(full_text)

    if rate <= 0:
        return

    wht_amt = _calc_wht_amount_from_subtotal(subtotal, rate)
    if wht_amt > 0:
        cleaned["P_wht"] = f"{wht_amt:.2f}"
        notes.append("WHT recalculated from subtotal (not total)")

    # Ensure N_unit_price is NOT WHT
    # If N_unit_price equals P_wht suspiciously and we have subtotal -> set N_unit_price to subtotal
    try:
        nu = _money_decimal(cleaned.get("N_unit_price"))
        pw = _money_decimal(cleaned.get("P_wht"))
        if pw > 0 and nu == pw:
            cleaned["N_unit_price"] = f"{subtotal:.2f}"
            notes.append("N_unit_price corrected (was equal to WHT)")
    except Exception:
        pass

# ---------------------------------------------------------------------
# Main AI function
# ---------------------------------------------------------------------
def ai_fill_peak_row(
    text: str,
    platform_hint: str = "",
    partial_row: Optional[Dict[str, Any]] = None,
    source_filename: str = "",
) -> Dict[str, Any]:
    """
    LLM step: fill PEAK columns from OCR/text.

    Args:
      - text: OCR text
      - platform_hint: from classifier/extractors
      - partial_row: rule-based extracted fields + optional helpers:
          * client_tax_id
          * seller_id / username (for L_description pattern)
          * _subtotal / subtotal (for WHT base)
          * _wht_rate (optional)
      - source_filename: original filename (FULL)

    Returns:
      A-U fields + metadata:
        _ai_confidence, _ai_notes, _platform_detected, _model_used, _extraction_method
    """

    if not _env_bool("ENABLE_LLM", default=False):
        logger.info("LLM disabled (ENABLE_LLM=0)")
        return {}

    if not os.getenv("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set")
        return {}

    partial_row = partial_row or {}
    full_text = _normalize_text(text or "")

    try:
        model = os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
        max_len = int(os.getenv("OPENAI_TEXT_MAX", "22000") or "22000")
        t = _truncate_text_smart(full_text, max_len)

        # detect platform
        platform = _detect_platform(full_text, hint=platform_hint)

        # guesses
        vendor_label = PLATFORM_VENDORS.get(platform, "Other")
        jp_guess, vr_guess = _guess_vat(platform, full_text)
        pay_guess = _guess_payment_method(platform, full_text)
        vendor_tax_guess = _guess_vendor_tax_id(full_text)

        platform_prompt = _build_platform_specific_prompt(platform)

        schema = {
            "B_doc_date": "YYYYMMDD (from document only; do NOT use filename codes)",
            "C_reference": "string <=64 (will be overwritten by hard-lock doc+ref rule)",
            "D_vendor_code": "string (vendor name/label; final may be overwritten by mapping elsewhere)",
            "E_tax_id_13": "13 digits or empty (vendor's tax ID, NOT client's)",
            "F_branch_5": "5 digits (00000 allowed) or empty",
            "G_invoice_no": "string (will be overwritten by hard-lock doc+ref rule)",
            "H_invoice_date": "YYYYMMDD (MUST come from document, never from filename)",
            "I_tax_purchase_date": "YYYYMMDD (optional)",
            "J_price_type": "1=VAT separated, 2=VAT included, 3=no VAT",
            "K_account": "string (may be overwritten by client hard-lock mapping)",
            "L_description": "string (may be overwritten by platform description pattern hard-lock)",
            "M_qty": "number-as-string (default 1)",
            "N_unit_price": "money 2dp (do NOT put WHT here)",
            "O_vat_rate": "7%|NO",
            "P_wht": "money 2dp (WHT amount; base is SUBTOTAL not TOTAL when provided)",
            "Q_payment_method": "string",
            "R_paid_amount": "money 2dp",
            "S_pnd": "1|2|3|53|empty",
            "T_note": "string (notes only)",
            "U_group": f"one of {sorted(GROUPS)}",
            "_ai_confidence": "0..1",
            "_ai_notes": "Thai explanation",
        }

        system = (
            "You are a meticulous Thai accounting document extraction engine for PEAK A–U import.\n"
            "Return STRICT JSON ONLY (no markdown).\n"
            "\n"
            "HARD RULES:\n"
            "1) NEVER infer any date from filename codes (e.g., 251203). Dates must come from document text.\n"
            "2) Do NOT put withholding tax (WHT) into unit price.\n"
            "3) If unsure, leave empty.\n"
            "4) Money fields: '1234.56' only.\n"
            "5) Tax ID must be 13 digits (vendor's).\n"
            "\n"
            f"{platform_prompt}\n"
        )

        user_payload = {
            "source_file": source_filename,
            "platform_detected": platform,
            "platform_hint": platform_hint,
            "vendor_label_guess": vendor_label,
            "vat_guess": {"J_price_type": jp_guess, "O_vat_rate": vr_guess},
            "payment_guess": pay_guess,
            "vendor_tax_id_guess": vendor_tax_guess,
            "partial_row_from_rule_based": partial_row,
            "required_schema": schema,
            "document_text": t,
        }

        out = _openai_chat_json(system=system, user=json.dumps(user_payload, ensure_ascii=False), model=model)
        if not out:
            logger.warning("OpenAI returned empty response")
            return {}

        allowed = set(schema.keys())
        cleaned: Dict[str, Any] = {k: v for k, v in (out or {}).items() if k in allowed}

        # ---------------------------------------------------------------------
        # Base normalization
        # ---------------------------------------------------------------------
        # vendor label (soft)
        if platform in PLATFORM_VENDORS and PLATFORM_VENDORS[platform]:
            cleaned["D_vendor_code"] = PLATFORM_VENDORS[platform]
        else:
            cleaned["D_vendor_code"] = str(cleaned.get("D_vendor_code") or vendor_label or "Other").strip()

        # VAT enforcement
        if platform in PLATFORM_VAT_RULES:
            rules = PLATFORM_VAT_RULES[platform]
            cleaned["J_price_type"] = rules["J_price_type"]
            cleaned["O_vat_rate"] = rules["O_vat_rate"]
        else:
            cleaned["J_price_type"] = _clamp_choice(cleaned.get("J_price_type"), PRICE_TYPES, jp_guess)
            cleaned["O_vat_rate"] = _clamp_choice(cleaned.get("O_vat_rate"), VAT_RATES, vr_guess)

        cleaned["E_tax_id_13"] = _to_tax13(cleaned.get("E_tax_id_13")) or vendor_tax_guess
        cleaned["F_branch_5"] = _to_branch5(cleaned.get("F_branch_5"))

        # Dates: only accept YYYYMMDD
        for dk in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
            v = str(cleaned.get(dk, "") or "").strip()
            cleaned[dk] = v if re.fullmatch(r"\d{8}", v) else ""

        # numeric
        cleaned["M_qty"] = str(cleaned.get("M_qty") or "1").strip() or "1"
        cleaned["N_unit_price"] = _to_money_2(cleaned.get("N_unit_price")) or ""
        cleaned["R_paid_amount"] = _to_money_2(cleaned.get("R_paid_amount")) or ""

        # group
        ug = str(cleaned.get("U_group", "") or "").strip()
        cleaned["U_group"] = ug if ug in GROUPS else PLATFORM_GROUPS.get(platform, "Other Expense")

        # payment
        if not str(cleaned.get("Q_payment_method", "") or "").strip():
            cleaned["Q_payment_method"] = pay_guess

        # WHT basic normalize
        p_wht = cleaned.get("P_wht", "")
        if isinstance(p_wht, (int, float, Decimal)):
            cleaned["P_wht"] = _to_money_2(p_wht) or "0"
        else:
            s = str(p_wht or "").strip()
            cleaned["P_wht"] = _to_money_2(s) or ("0" if not s else "0")

        # PND
        s_pnd = str(cleaned.get("S_pnd", "") or "").strip()
        if s_pnd not in PND_ALLOWED:
            s_pnd = ""
        if s_pnd == "" and cleaned.get("P_wht") not in ("", "0", "0.00"):
            s_pnd = _guess_pnd(full_text, cleaned["P_wht"])
        cleaned["S_pnd"] = s_pnd

        # ---------------------------------------------------------------------
        # HARD LOCKS (your requirements)
        # ---------------------------------------------------------------------
        hard_notes: List[str] = []

        # 1) Guard: dates not from filename
        _guard_dates_not_from_filename(cleaned, source_filename, full_text, hard_notes)

        # 2) Lock doc+ref for C_reference + G_invoice_no
        _lock_doc_ref_fields(cleaned, source_filename)

        # 3) Lock K_account by client tax id (if provided)
        client_tax_id = str(partial_row.get("client_tax_id") or partial_row.get("A_company_tax_id") or "").strip()
        if client_tax_id:
            _lock_k_account(cleaned, client_tax_id)

        # 4) Lock description pattern by platform (Shopee etc.)
        _lock_description_pattern(cleaned, platform, source_filename, full_text, partial_row)

        # 5) Enforce WHT from subtotal (when subtotal is available)
        _enforce_wht_from_subtotal(cleaned, full_text, partial_row, hard_notes)

        # 6) Final safety: ensure N_unit_price is not negative / and not WHT
        if cleaned.get("N_unit_price") and cleaned.get("N_unit_price") in (cleaned.get("P_wht"),):
            # if still equal, clear it (better empty than wrong)
            cleaned["N_unit_price"] = ""
            hard_notes.append("N_unit_price cleared (matched WHT)")

        note = str(cleaned.get("T_note", "") or "").strip()
        lines = [ln.strip() for ln in note.splitlines() if ln.strip()]
        if platform != "UNKNOWN":
            lines.append(f"Platform: {platform}")
        if source_filename:
            lines.append(f"Source: {source_filename}")
        if hard_notes:
            for hn in hard_notes:
                lines.append(f"LOCK: {hn}")
        lines.append("Extraction: AI")

        # dedupe keep order
        deduped: List[str] = []
        seen = set()
        for ln in lines:
            if ln not in seen:
                seen.add(ln)
                deduped.append(ln)
        cleaned["T_note"] = "\n".join(deduped)[:1500]

        # confidence
        try:
            c = float(cleaned.get("_ai_confidence", 0))
            cleaned["_ai_confidence"] = max(0.0, min(1.0, c))
        except Exception:
            cleaned["_ai_confidence"] = 0.0

        cleaned["_ai_notes"] = str(cleaned.get("_ai_notes", "") or "").strip()
        cleaned["_extraction_method"] = "ai_th" if "ก" in full_text else "ai_en"
        cleaned["_platform_detected"] = platform
        cleaned["_model_used"] = model

        # Final: only schema keys
        final: Dict[str, Any] = {}
        for k in schema.keys():
            if k in cleaned:
                final[k] = cleaned[k]

        logger.info("AI extraction complete: %s confidence=%s", platform, final.get("_ai_confidence", 0))
        return final

    except Exception as e:
        logger.error("AI extraction error: %s", e, exc_info=True)
        return {}


__all__ = ["ai_fill_peak_row", "PLATFORM_VENDORS", "PLATFORM_VAT_RULES", "PLATFORM_GROUPS"]
