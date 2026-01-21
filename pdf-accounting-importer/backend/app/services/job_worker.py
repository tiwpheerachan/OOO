"""
Job Worker - Locked Version (cfg+filename pass-through + client_tax_id resolver + compute_wht support)

✅ MUST:
- call extract_row_from_text(text, filename=..., cfg=cfg, client_tax_id=...) for EVERY file
- cfg must include: client_tax_ids / client_tags / compute_wht (bool)
- resolve client_tax_id per-file so GL mapping doesn't go blank

Notes:
- Worker still DOES NOT calculate WHT itself (business logic stays in extractor/finalize)
- Worker will only pass cfg["compute_wht"] so extractor can decide:
    ✅ compute_wht=True  -> compute P_wht & set S_pnd (if your extractor does)
    ❌ compute_wht=False -> do not compute WHT (leave P_wht empty, S_pnd empty)
"""

from __future__ import annotations

import io
import os
import re
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

from .extract_service import extract_row_from_text
from .ocr_service import maybe_ocr_to_text
from .ai_service import ai_fill_peak_row
from ..utils.text_utils import normalize_text
from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

from .platform_constants import normalize_platform as _norm_platform

try:
    from ..extractors.wallet_mapping import resolve_wallet_code
except Exception:  # pragma: no cover
    resolve_wallet_code = None  # type: ignore


# ============================================================
# Company / Client config
# ============================================================

CLIENT_TAX_IDS: Dict[str, str] = {
    "RABBIT": "0105561071873",
    "SHD": "0105563022918",
    "TOPONE": "0105565027615",
}
TAXID_TO_COMPANY: Dict[str, str] = {v: k for k, v in CLIENT_TAX_IDS.items()}

# ✅ GL mapping per company (เติมให้ครบตามรูปของคุณ)
ACCOUNT_BY_CLIENT_TAX_ID: Dict[str, str] = {
    "0105563022918": "520317",  # SHD
    "0105561071873": "520315",  # RABBIT (จากรูป: Marketplace Shopee = 520315)
    "0105565027615": "520314",  # TOPONE (จากรูป: Marketplace Shopee = 520314)
}


# ============================================================
# Regex / helpers
# ============================================================

RE_ALL_WS = re.compile(r"\s+")
RE_SELLER_ID_HINTS = [
    re.compile(
        r"\b(?:seller_id|seller\s*id|shop_id|shop\s*id|merchant_id|merchant\s*id)\b\D{0,20}(\d{5,20})",
        re.IGNORECASE,
    ),
    re.compile(r"(?:รหัสร้าน|ไอดีร้าน|รหัสผู้ขาย|ร้านค้า)\D{0,20}(\d{5,20})", re.IGNORECASE),
]
RE_USERNAME_HINTS = [
    re.compile(r"\busername\b\D{0,20}([A-Za-z0-9_.\-]{2,64})", re.IGNORECASE),
    re.compile(r"(?:ชื่อผู้ใช้|ยูสเซอร์|ชื่อร้าน)\D{0,20}([A-Za-z0-9_.\-]{2,64})", re.IGNORECASE),
]
RE_ANY_LONG_DIGITS = re.compile(r"\b(\d{6,20})\b")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _digits_only(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def _clean_money_str(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    s = s.replace("฿", "").replace("THB", "").replace(",", "").strip()
    s = RE_ALL_WS.sub("", s)
    return s


def _compact_ref(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _filename_base(filename: str) -> str:
    try:
        return os.path.basename(filename or "").strip()
    except Exception:
        return filename or ""


def _filename_stem(filename: str) -> str:
    base = _filename_base(filename)
    if not base:
        return ""
    stem, _ext = os.path.splitext(base)
    return stem.strip()


def _doc_ref_from_filename(filename: str) -> str:
    stem = _filename_stem(filename)
    if not stem:
        return ""
    return _compact_ref(stem)


def _detect_platform_hint_from_filename(filename: str) -> str:
    fn = (filename or "").upper()

    if "META" in fn or "FACEBOOK" in fn or "RCMETA" in fn:
        return "META"
    if "GOOGLE" in fn or "GOOG" in fn:
        return "GOOGLE"

    if "SPX" in fn or "RCSPX" in fn or "SHOPEE EXPRESS" in fn or "SHOPEE-EXPRESS" in fn:
        return "SPX"

    if "SHOPEE" in fn:
        return "SHOPEE"
    if "LAZADA" in fn or fn.startswith("LAZ"):
        return "LAZADA"
    if "TIKTOK" in fn or "TTS" in fn or "TTSHOP" in fn:
        return "TIKTOK"

    if "TAX" in fn or "INVOICE" in fn or "ใบกำกับ" in fn:
        return "THAI_TAX"

    return "UNKNOWN"


# ============================================================
# Client Detection / Resolver
# ============================================================

def _detect_client_tax_id(text: str, filename: str = "", cfg: Optional[Dict[str, Any]] = None) -> str:
    t = text or ""
    for tax in CLIENT_TAX_IDS.values():
        if tax and tax in t:
            return tax

    if isinstance(cfg, dict):
        taxs = cfg.get("client_tax_ids")
        if isinstance(taxs, list) and len(taxs) == 1 and str(taxs[0]).strip():
            return str(taxs[0]).strip()

    fn = (filename or "").upper()
    for key, tax in CLIENT_TAX_IDS.items():
        if key in fn:
            return tax

    return ""


def _company_from_tax_id(client_tax_id: str, filename: str = "") -> str:
    if client_tax_id and client_tax_id in TAXID_TO_COMPANY:
        return TAXID_TO_COMPANY[client_tax_id]

    fn = (filename or "").upper()
    for k in ("RABBIT", "SHD", "TOPONE"):
        if k in fn:
            return k
    return ""


def _resolve_client_tax_id_for_file(
    *,
    detected_tax_id: str,
    company_tag: str,
    cfg: Dict[str, Any],
) -> str:
    """
    ✅ Make sure we pass a SINGLE client_tax_id into extract_service/finalize.

    Priority:
    1) detected_tax_id from text
    2) cfg["client_tax_id"] (single) if exists
    3) cfg["client_tax_ids"] if list:
       - if length==1 -> that
       - if many -> use company_tag mapping if possible
       - else fallback first non-empty
    """
    d = (detected_tax_id or "").strip()
    if d:
        return d

    one = str(cfg.get("client_tax_id") or "").strip()
    if one:
        return one

    taxs = cfg.get("client_tax_ids")
    if isinstance(taxs, list):
        cleaned = [str(x).strip() for x in taxs if str(x).strip()]
        if len(cleaned) == 1:
            return cleaned[0]
        if len(cleaned) > 1:
            c = (company_tag or "").upper().strip()
            if c and c in CLIENT_TAX_IDS:
                pick = CLIENT_TAX_IDS[c]
                if pick in cleaned:
                    return pick
            return cleaned[0]  # fallback (never empty)
    return ""


# ============================================================
# Job Config Helpers
# ============================================================

def _get_job_cfg(job_service, job_id: str) -> Dict[str, Any]:
    try:
        job = job_service.get_job(job_id)  # type: ignore[attr-defined]
    except Exception:
        job = None

    if not isinstance(job, dict):
        return {}

    raw = job.get("cfg")
    cfg: Dict[str, Any] = raw if isinstance(raw, dict) else {}

    # fallback legacy shapes
    if not cfg:
        filters = job.get("filters")
        if isinstance(filters, dict):
            for k in ("client_tax_ids", "client_tags", "platforms", "strictMode", "compute_wht"):
                if k in filters:
                    cfg[k] = filters.get(k)

    # normalize list fields
    def _norm_list(v: Any, *, upper: bool = False) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",") if p.strip()]
        elif isinstance(v, (list, tuple, set)):
            parts = [str(i).strip() for i in v if str(i).strip()]
        else:
            parts = []
        parts = [p.upper() if upper else p for p in parts]
        seen = set()
        out: List[str] = []
        for x in parts:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    cfg["client_tags"] = _norm_list(cfg.get("client_tags"), upper=True)
    cfg["client_tax_ids"] = _norm_list(cfg.get("client_tax_ids"), upper=False)
    cfg["platforms"] = _norm_list(cfg.get("platforms"), upper=False)

    # normalize booleans
    cfg["strictMode"] = bool(cfg.get("strictMode", False))

    # ✅ Ensure compute_wht exists as bool for whole pipeline
    # Default = True (คำนวณ) ตามที่คุณขอให้เป็น parameter
    if "compute_wht" not in cfg:
        cfg["compute_wht"] = True
    else:
        cfg["compute_wht"] = bool(cfg.get("compute_wht"))

    return cfg


def _get_job_filters(job_service, job_id: str) -> Tuple[List[str], List[str], bool]:
    cfg = _get_job_cfg(job_service, job_id)
    strict_mode = bool(cfg.get("strictMode", False))

    companies = cfg.get("client_tags", [])
    platforms = cfg.get("platforms", [])

    def _norm_company_list(x: Any) -> List[str]:
        if isinstance(x, list):
            out = [str(i).strip().upper() for i in x if str(i).strip()]
        elif isinstance(x, str):
            out = [p.strip().upper() for p in x.split(",") if p.strip()]
        else:
            out = []
        seen = set()
        uniq: List[str] = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    def _norm_platform_list(x: Any) -> List[str]:
        if isinstance(x, list):
            out0 = [str(i).strip() for i in x if str(i).strip()]
        elif isinstance(x, str):
            out0 = [p.strip() for p in x.split(",") if p.strip()]
        else:
            out0 = []
        out = [_norm_platform(p) or "UNKNOWN" for p in out0]
        seen = set()
        uniq: List[str] = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    return (_norm_company_list(companies), _norm_platform_list(platforms), strict_mode)


def _cfg_mismatch(
    allowed_companies: List[str],
    allowed_platforms: List[str],
    strict_mode: bool,
    *,
    company: str,
    platform_u: str,
) -> Tuple[bool, str]:
    c = (company or "").upper().strip()
    p = (platform_u or "UNKNOWN").upper().strip()

    has_company_filter = bool(allowed_companies)
    has_platform_filter = bool(allowed_platforms)

    if not has_company_filter and not has_platform_filter:
        return (False, "")

    if has_company_filter:
        if c:
            if c not in allowed_companies:
                return (True, f"company={c} not in allowed ({','.join(allowed_companies)})")
        else:
            if strict_mode:
                return (True, f"company=unknown (strictMode, allowed: {','.join(allowed_companies)})")

    if has_platform_filter:
        if p and p != "UNKNOWN":
            if p not in allowed_platforms:
                return (True, f"platform={p} not in allowed ({','.join(allowed_platforms)})")
        else:
            if strict_mode:
                return (True, f"platform=unknown (strictMode, allowed: {','.join(allowed_platforms)})")

    return (False, "")


# ============================================================
# Validation
# ============================================================

def _revalidate(row: Dict[str, Any]) -> List[str]:
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

    if row.get("J_price_type") and not validate_price_type(row.get("J_price_type", "")):
        errors.append("ประเภทราคาไม่ถูกต้อง")

    if row.get("O_vat_rate") and not validate_vat_rate(row.get("O_vat_rate", "")):
        errors.append("อัตราภาษีไม่ถูกต้อง")

    return errors


# ============================================================
# Row policies
# ============================================================

def _apply_locked_fields(row: Dict[str, Any], *, filename: str, platform_u: str, text: str, client_tax_id: str) -> None:
    doc_ref = _doc_ref_from_filename(filename)
    if doc_ref:
        row["C_reference"] = doc_ref
        row["G_invoice_no"] = doc_ref
    else:
        c = _compact_ref(row.get("C_reference"))
        g = _compact_ref(row.get("G_invoice_no"))
        pick = c or g
        if pick:
            row["C_reference"] = pick
            row["G_invoice_no"] = pick

    # ✅ K_account by client tax id (now includes RB/TOPONE too)
    if client_tax_id and client_tax_id in ACCOUNT_BY_CLIENT_TAX_ID:
        row["K_account"] = ACCOUNT_BY_CLIENT_TAX_ID[client_tax_id]

    base = _filename_base(filename)
    seller_id = _safe_str(row.get("_seller_id")) or _detect_seller_id(text, filename)
    username = _detect_username(text)

    seller_id = seller_id or "unknown"
    username = username or "unknown"

    cur_desc = _safe_str(row.get("L_description"))
    should_override = (not cur_desc) or (cur_desc.lower() in {"-", "n/a", "na", "unknown"})

    if platform_u == "SHOPEE":
        locked_desc = f"Record Marketplace Expense - Shopee - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "LAZADA":
        locked_desc = f"Record Marketplace Expense - Lazada - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "TIKTOK":
        locked_desc = f"Record Marketplace Expense - Tiktok - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "SPX":
        locked_desc = f"Record Delivery/Logistics Expense - SPX - Seller ID {seller_id} - {username} - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "META":
        locked_desc = f"Record Advertising Expense - Meta - {base}"
        if should_override:
            row["L_description"] = locked_desc
    elif platform_u == "GOOGLE":
        locked_desc = f"Record Advertising Expense - Google - {base}"
        if should_override:
            row["L_description"] = locked_desc
    else:
        if should_override:
            row["L_description"] = f"Record Expense - {platform_u} - {base}"


def _normalize_row_fields(row: Dict[str, Any], seq: int) -> None:
    row["A_seq"] = seq

    for k in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        if row.get(k):
            row[k] = _digits_only(_safe_str(row.get(k)))[:8]
        else:
            row[k] = _safe_str(row.get(k))

    row["E_tax_id_13"] = _digits_only(_safe_str(row.get("E_tax_id_13")))[:13]
    br = _digits_only(_safe_str(row.get("F_branch_5")))
    row["F_branch_5"] = br.zfill(5)[:5] if br else "00000"

    j = _safe_str(row.get("J_price_type"))
    row["J_price_type"] = j if j in {"1", "2", "3"} else (j or "1")

    o = _safe_str(row.get("O_vat_rate")).upper()
    row["O_vat_rate"] = "NO" if o in {"NO", "0", "NONE"} else ("7%" if (o == "" or "7" in o) else o)

    row["M_qty"] = _safe_str(row.get("M_qty") or "1") or "1"

    n = _clean_money_str(row.get("N_unit_price"))
    row["N_unit_price"] = n if n else "0"

    r = _clean_money_str(row.get("R_paid_amount"))
    row["R_paid_amount"] = r if r else "0"

    # ✅ keep P_wht (do not erase)
    p = _clean_money_str(row.get("P_wht"))
    row["P_wht"] = p

    row["C_reference"] = _compact_ref(row.get("C_reference"))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no"))

    for k in (
        "A_company_name",
        "D_vendor_code",
        "K_account",
        "L_description",
        "Q_payment_method",
        "S_pnd",
        "T_note",
        "U_group",
        "_source_file",
        "_platform",
        "_client_tax_id",
        "_seller_id",
        "_status",
        "_status_reason",
    ):
        if k in row:
            row[k] = _safe_str(row.get(k))


# ============================================================
# PDF/OCR helpers
# ============================================================

def _extract_embedded_pdf_text(data: bytes, max_pages: int = 15) -> str:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            parts: List[str] = []
            for p in pdf.pages[:max_pages]:
                parts.append(p.extract_text() or "")
            return "\n".join(parts).strip()
    except Exception:
        return ""


def _write_temp_file(filename: str, data: bytes) -> str:
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}:
        if data[:5] == b"%PDF-":
            ext = ".pdf"
        else:
            ext = ext or ".bin"

    fd, path = tempfile.mkstemp(prefix="peak_import_", suffix=ext)
    os.close(fd)
    with open(path, "wb") as f:
        f.write(data)
    return path


def _should_call_ai(errors: List[str], row: Dict[str, Any]) -> bool:
    critical_missing = (
        not _safe_str(row.get("B_doc_date"))
        or not _safe_str(row.get("L_description"))
        or _safe_str(row.get("R_paid_amount")) in {"", "0", "0.0", "0.00"}
    )
    return bool(errors) or critical_missing


def _append_and_update_file(
    job_service,
    job_id: str,
    idx: int,
    *,
    rows: List[Dict[str, Any]],
    state: str,
    platform: str,
    company: str,
    message: str,
) -> None:
    if rows:
        job_service.append_rows(job_id, rows)
    job_service.update_file(
        job_id,
        idx,
        {
            "state": state,
            "platform": platform,
            "company": company,
            "message": message,
            "rows_count": len(rows),
        },
    )


def _merge_unique_errors(*lists: List[str]) -> List[str]:
    out: List[str] = []
    for xs in lists:
        for e in xs or []:
            if e and e not in out:
                out.append(e)
    return out


def _add_note(row: Dict[str, Any], note: str) -> None:
    note = _safe_str(note)
    if not note:
        return
    cur = _safe_str(row.get("T_note"))
    if not cur:
        row["T_note"] = note
    else:
        if note not in cur:
            row["T_note"] = f"{cur} | {note}"


def _detect_seller_id(text: str, filename: str = "") -> str:
    t = text or ""
    for rx in RE_SELLER_ID_HINTS:
        m = rx.search(t)
        if m:
            return _safe_str(m.group(1))

    candidates = RE_ANY_LONG_DIGITS.findall(t)
    if candidates:
        return candidates[0]

    fn_digits = re.findall(r"\d{6,20}", filename or "")
    if fn_digits:
        return fn_digits[0]

    return ""


def _detect_username(text: str) -> str:
    t = text or ""
    for rx in RE_USERNAME_HINTS:
        m = rx.search(t)
        if m:
            return _safe_str(m.group(1))
    return ""


# ============================================================
# Main worker
# ============================================================

def process_job_files(job_service, job_id: str) -> None:
    payloads: List[Tuple[str, str, bytes]] = job_service.get_payloads(job_id)

    allowed_companies, allowed_platforms, strict_mode = _get_job_filters(job_service, job_id)
    cfg = _get_job_cfg(job_service, job_id)

    # ✅ Running sequence across whole job
    seq = 1

    ok_files = 0
    review_files = 0
    error_files = 0
    processed = 0

    ai_only_fill_empty = _env_bool("AI_ONLY_FILL_EMPTY", default=False)

    for idx, (filename, content_type, data) in enumerate(payloads):
        filename = filename or "unknown"
        content_type = content_type or ""

        job_service.update_file(job_id, idx, {"state": "processing"})

        platform_u = "UNKNOWN"
        company = ""
        rows_out: List[Dict[str, Any]] = []
        tmp_path: Optional[str] = None

        try:
            # ---------- Extract text ----------
            text = ""
            is_pdf = filename.lower().endswith(".pdf") or (content_type == "application/pdf")

            if is_pdf:
                text = _extract_embedded_pdf_text(data, max_pages=15)

            if not text:
                tmp_path = _write_temp_file(filename, data)
                text = maybe_ocr_to_text(tmp_path)

            text = normalize_text(text)

            # detect client from text / cfg
            detected_tax = _detect_client_tax_id(text, filename, cfg=cfg)
            company = _company_from_tax_id(detected_tax, filename)

            # ✅ resolve to SINGLE client_tax_id per file
            client_tax_id = _resolve_client_tax_id_for_file(
                detected_tax_id=detected_tax,
                company_tag=company,
                cfg=cfg,
            )

            # keep meta in cfg too (helps extract_service if it looks for client_tax_id)
            if client_tax_id:
                cfg_for_file = dict(cfg)
                cfg_for_file["client_tax_id"] = client_tax_id
            else:
                cfg_for_file = dict(cfg)

            # ---------- If still no text ----------
            if not text:
                platform_u = _norm_platform(_detect_platform_hint_from_filename(filename)) or "UNKNOWN"

                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )

                row_min: Dict[str, Any] = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_client_tax_id": client_tax_id,
                    "_status": "NEEDS_REVIEW",
                    "_status_reason": "no_text",
                    "_errors": ["ไม่พบข้อความจากเอกสาร"],
                }

                _normalize_row_fields(row_min, seq=seq)
                _apply_locked_fields(row_min, filename=filename, platform_u=platform_u, text="", client_tax_id=client_tax_id)

                if is_mismatch:
                    row_min["_errors"] = list(row_min.get("_errors") or []) + [f"ไม่ตรง filter: {mismatch_reason}"]
                    row_min["_status_reason"] = "filter_mismatch"
                    _add_note(row_min, f"Filtered: {mismatch_reason}")

                rows_out.append(row_min)
                seq += 1

                file_state = "needs_review"
                message = "ไม่พบข้อความจากเอกสาร"
                if is_mismatch:
                    message += f" | {mismatch_reason}"
                review_files += 1

                _append_and_update_file(
                    job_service,
                    job_id,
                    idx,
                    rows=rows_out,
                    state=file_state,
                    platform=platform_u,
                    company=company,
                    message=message,
                )

            else:
                # ---------- Extract structured row ----------
                # ✅ MUST: pass filename + cfg every file
                platform, base_row, errors = extract_row_from_text(
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg_for_file,
                )
                platform_u = _norm_platform(platform) or "UNKNOWN"

                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )

                seller_id = _detect_seller_id(text, filename)
                shop_name_hint = _filename_stem(filename)

                wallet_code = ""
                if resolve_wallet_code is not None:
                    try:
                        wallet_code = (
                            resolve_wallet_code(
                                client_tax_id,
                                seller_id=seller_id,
                                shop_name=shop_name_hint,
                                text=text,
                            )
                            or ""
                        )
                    except Exception:
                        wallet_code = ""

                # Company name fallback (if you want company name column always filled)
                if not company and client_tax_id:
                    company = _company_from_tax_id(client_tax_id, filename)

                row: Dict[str, Any] = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_client_tax_id": client_tax_id,
                    "_seller_id": seller_id,
                    "_errors": list(errors) if errors else [],
                }
                if isinstance(base_row, dict):
                    row.update(base_row)

                if wallet_code:
                    row["Q_payment_method"] = wallet_code

                _normalize_row_fields(row, seq=seq)

                # ✅ LOCK BEFORE AI
                _apply_locked_fields(row, filename=filename, platform_u=platform_u, text=text, client_tax_id=client_tax_id)

                # ---------- Optional AI patch ----------
                if _should_call_ai(list(row.get("_errors") or []), row):
                    partial_keys = [
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

                    ai_patch = ai_fill_peak_row(
                        text=text,
                        platform_hint=platform_u,
                        partial_row={k: row.get(k, "") for k in partial_keys},
                        source_filename=filename,
                    )

                    if ai_patch and isinstance(ai_patch, dict):
                        for k, v in ai_patch.items():
                            if not k:
                                continue
                            if k.startswith("_"):
                                row[k] = v
                                continue

                            # ✅ HARD LOCK: AI ห้ามใส่ P_wht
                            if k == "P_wht":
                                continue

                            v_str = _safe_str(v)
                            if not v_str:
                                continue

                            if ai_only_fill_empty:
                                if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                    row[k] = v_str
                            else:
                                if row.get("_errors"):
                                    row[k] = v_str
                                else:
                                    if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                        row[k] = v_str

                if wallet_code:
                    row["Q_payment_method"] = wallet_code

                _normalize_row_fields(row, seq=seq)

                # ✅ re-lock again after AI
                _apply_locked_fields(row, filename=filename, platform_u=platform_u, text=text, client_tax_id=client_tax_id)

                errors2 = _revalidate(row)
                row["_errors"] = _merge_unique_errors(list(row.get("_errors") or []), errors2)

                if is_mismatch:
                    row["_status"] = "NEEDS_REVIEW"
                    row["_status_reason"] = "filter_mismatch"
                    row["_errors"] = _merge_unique_errors(list(row.get("_errors") or []), [f"ไม่ตรง filter: {mismatch_reason}"])
                    _add_note(row, f"Filtered: {mismatch_reason}")
                    file_state = "needs_review"
                    message = mismatch_reason
                    review_files += 1
                else:
                    if row.get("_errors"):
                        row["_status"] = "NEEDS_REVIEW"
                        row["_status_reason"] = "validation_or_missing"
                        file_state = "needs_review"
                        message = "มีช่องที่ต้องตรวจสอบ"
                        review_files += 1
                    else:
                        row["_status"] = "OK"
                        row["_status_reason"] = ""
                        file_state = "done"
                        message = ""
                        ok_files += 1

                rows_out.append(row)
                seq += 1

                _append_and_update_file(
                    job_service,
                    job_id,
                    idx,
                    rows=rows_out,
                    state=file_state,
                    platform=platform_u,
                    company=company,
                    message=message,
                )

        except Exception as e:
            error_files += 1

            err_row: Dict[str, Any] = {
                "A_seq": seq,
                "A_company_name": company or "",
                "_source_file": filename,
                "_platform": platform_u or "UNKNOWN",
                "_status": "ERROR",
                "_status_reason": "exception",
                "_errors": [f"{type(e).__name__}: {e}"],
            }
            _normalize_row_fields(err_row, seq=seq)
            _apply_locked_fields(err_row, filename=filename, platform_u=platform_u or "UNKNOWN", text="", client_tax_id="")
            seq += 1

            try:
                job_service.append_rows(job_id, [err_row])
            except Exception:
                pass

            job_service.update_file(
                job_id,
                idx,
                {
                    "state": "error",
                    "platform": platform_u or "UNKNOWN",
                    "company": company or "",
                    "message": f"Error: {type(e).__name__}: {e}",
                    "rows_count": 1,
                },
            )

        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        processed += 1
        job_service.update_job(
            job_id,
            {
                "processed_files": processed,
                "ok_files": ok_files,
                "review_files": review_files,
                "error_files": error_files,
            },
        )

    final_state = "done" if error_files == 0 else "error"
    job_service.update_job(job_id, {"state": final_state})


__all__ = ["process_job_files"]
