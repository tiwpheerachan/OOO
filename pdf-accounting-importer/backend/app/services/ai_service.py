from __future__ import annotations

import json
import os
import re
from typing import Dict, Any, Optional

import requests


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _first_json_object(s: str) -> Optional[str]:
    """Best-effort: extract first JSON object from a model output."""
    if not s:
        return None
    # Try strict
    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        return s
    # Try find first {...}
    m = re.search(r"\{[\s\S]*\}", s)
    return m.group(0) if m else None


def _openai_chat_json(system: str, user: str, model: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY")

    url = os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        # Ask the model to return JSON as-is (best effort)
        "response_format": {"type": "json_object"},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"]
    js = _first_json_object(content) or "{}"
    return json.loads(js)


def ai_fill_peak_row(text: str, platform_hint: str, partial_row: Dict[str, Any], source_filename: str) -> Dict[str, Any]:
    """LLM step: fill PEAK columns from OCR/text.

    Enabled when ENABLE_LLM=1 and OPENAI_API_KEY is set.
    Returns a dict containing PEAK fields (B..U) + optional meta:
      - _ai_confidence (0..1)
      - _ai_notes (short)
    """

    if not _env_bool("ENABLE_LLM", default=False):
        return {}

    model = os.getenv("OPENAI_MODEL") or "gpt-4o-mini"

    # Keep payload small-ish but include enough context.
    # If text is huge, truncate head+tail.
    t = (text or "").strip()
    if len(t) > 22000:
        t = t[:14000] + "\n\n...<TRUNCATED>...\n\n" + t[-7000:]

    # Provide required schema to the model
    schema = {
        "B_doc_date": "YYYYMMDD",
        "C_reference": "string <=32",
        "D_vendor_code": "string (optional)",
        "E_tax_id_13": "13 digits or empty",
        "F_branch_5": "5 digits (00000 allowed) or empty",
        "G_invoice_no": "string",
        "H_invoice_date": "YYYYMMDD",
        "I_tax_purchase_date": "YYYYMMDD",
        "J_price_type": "1|2|3",
        "K_account": "string (optional)",
        "L_description": "string",
        "M_qty": "number-as-string",
        "N_unit_price": "number-as-string",
        "O_vat_rate": "7%|NO",
        "P_wht": "0|number|percent (e.g. 3%)",
        "Q_payment_method": "string (optional)",
        "R_paid_amount": "number-as-string",
        "S_pnd": "1|2|3|53|empty",
        "T_note": "string",
        "U_group": "string (category/group)",
        "_ai_confidence": "0..1",
        "_ai_notes": "short Thai explanation",
    }

    # Your initial mapping request
    group_dictionary = (
        "Dictionary ตัวอย่าง (ปรับได้):\n"
        "- Lazada / Shopee / TikTok → Marketplace Expense\n"
        "- Commission → Selling Expense\n"
        "- Advertising → Advertising Expense\n"
        "- Goods → Inventory / COGS\n"
        "Rule: If ไม่มี Tax ID → e-Tax Ready = NO (ใส่ไว้ใน T_note ได้)\n"
    )

    system = (
        "You are a meticulous Thai accounting document extraction engine. "
        "Extract structured fields for PEAK expense import. "
        "Return STRICT JSON ONLY. No markdown. No extra text. "
        "If you are unsure, leave the field empty but try your best. "
        "Normalize dates to YYYYMMDD (Gregorian). "
        "Tax ID must be 13 digits. Branch must be 5 digits (00000 allowed). "
        "price_type: 1=VAT separated, 2=VAT included, 3=no VAT. "
        "vat_rate: 7% or NO. "
        "For paid amount, prefer total including VAT if present. "
        "Write short Thai notes in _ai_notes."
    )

    user = (
        f"SOURCE_FILE: {source_filename}\n"
        f"PLATFORM_HINT: {platform_hint}\n"
        f"PARTIAL_ROW_JSON: {json.dumps(partial_row, ensure_ascii=False)}\n\n"
        f"REQUIRED_SCHEMA_KEYS: {json.dumps(schema, ensure_ascii=False)}\n\n"
        f"{group_dictionary}\n"
        "---\n"
        "DOCUMENT_TEXT (OCR/TEXT):\n"
        f"{t}\n"
    )

    try:
        out = _openai_chat_json(system=system, user=user, model=model)
    except Exception:
        return {}

    # Keep only expected keys
    allowed = set(schema.keys())
    cleaned: Dict[str, Any] = {}
    for k, v in out.items():
        if k in allowed:
            cleaned[k] = v
    return cleaned
