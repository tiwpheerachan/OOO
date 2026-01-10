from __future__ import annotations

import io
import os
import re
import tempfile
from typing import List, Dict, Any, Tuple, Optional

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

# ✅ wallet mapping
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


# ============================================================
# Regex / helpers
# ============================================================

RE_TAX13_STRICT = re.compile(r"\b(\d{13})\b")

RE_SELLER_ID_HINTS = [
    re.compile(r"\b(?:seller_id|seller\s*id|shop_id|shop\s*id|merchant_id|merchant\s*id)\b\D{0,20}(\d{5,20})", re.IGNORECASE),
    re.compile(r"(?:รหัสร้าน|ไอดีร้าน|รหัสผู้ขาย|ร้านค้า)\D{0,20}(\d{5,20})", re.IGNORECASE),
]
RE_ANY_LONG_DIGITS = re.compile(r"\b(\d{6,20})\b")

RE_ALL_WS = re.compile(r"\s+")


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
    return s.replace("฿", "").replace("THB", "").replace(",", "").strip()


def _compact_ref(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _detect_client_tax_id(text: str, filename: str = "", cfg: Optional[Dict[str, Any]] = None) -> str:
    """
    ✅ FIX: Better priority logic
    
    Priority:
      1) Text contains known client tax IDs (most reliable)
      2) cfg has exactly one client_tax_ids (user selected)
      3) Filename/path hints (RABBIT/SHD/TOPONE)
      
    Note: Text is checked FIRST because it's more reliable than user selection
    """
    # ✅ Step 1: Check text first (most reliable)
    t = text or ""
    for _, tax in CLIENT_TAX_IDS.items():
        if tax in t:
            return tax

    # ✅ Step 2: Check cfg (user selection)
    if isinstance(cfg, dict):
        taxs = cfg.get("client_tax_ids")
        if isinstance(taxs, list) and len(taxs) == 1 and str(taxs[0]).strip():
            return str(taxs[0]).strip()

    # ✅ Step 3: Check filename as fallback
    fn = (filename or "").upper()
    for key, tax in CLIENT_TAX_IDS.items():
        if key in fn:
            return tax

    return ""


def _company_from_tax_id(client_tax_id: str, filename: str = "") -> str:
    """
    ✅ FIX: Return company name from tax ID or filename
    Returns empty string if cannot determine (NOT "UNKNOWN")
    """
    if client_tax_id and client_tax_id in TAXID_TO_COMPANY:
        return TAXID_TO_COMPANY[client_tax_id]

    fn = (filename or "").upper()
    for k in ("RABBIT", "SHD", "TOPONE"):
        if k in fn:
            return k
    
    return ""  # ✅ return "" not "UNKNOWN"


def _detect_platform_hint_from_filename(filename: str) -> str:
    """
    Detect platform from filename
    Returns empty string if cannot determine (NOT "UNKNOWN")
    """
    fn = (filename or "").upper()
    
    # ✅ Priority order matters: SPX before SHOPEE
    if "SPX" in fn or "RCSPX" in fn or "SHOPEE EXPRESS" in fn or "SHOPEE-EXPRESS" in fn:
        return "SPX"
    if "SHOPEE" in fn:
        return "SHOPEE"
    if "LAZADA" in fn or "LAZ" in fn:
        return "LAZADA"
    if "TIKTOK" in fn or "TTS" in fn or "TTSHOP" in fn:
        return "TIKTOK"
    if "FACEBOOK" in fn or "META" in fn or "GOOGLE" in fn or "ADS" in fn:
        return "ADS"
    if "HASHTAG" in fn:
        return "HASHTAG"
    
    return ""  # ✅ return "" not "UNKNOWN"


def _platform_upper(platform: str, filename: str = "") -> str:
    """
    ✅ FIX: Normalize platform string
    """
    p = (platform or "").strip().lower()
    
    # Known platforms
    if p in {"shopee", "spx", "lazada", "tiktok", "ads", "facebook", "hashtag"}:
        return p.upper()
    
    # Try filename hint
    fh = _detect_platform_hint_from_filename(filename)
    if fh:
        return fh
    
    # ✅ If still unknown, return "UNKNOWN"
    return (platform or "UNKNOWN").upper()


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


def _get_job_cfg(job_service, job_id: str) -> Dict[str, Any]:
    """
    ✅ FIX: ดึง cfg จาก job ได้หลายรูปแบบ (รองรับของเดิม/ของใหม่)
    main.py ส่ง cfg = {client_tags, client_tax_ids, platforms, strictMode}
    """
    try:
        job = job_service.get_job(job_id)  # type: ignore[attr-defined]
    except Exception:
        job = None

    if not isinstance(job, dict):
        return {}

    cfg = job.get("cfg")
    if isinstance(cfg, dict):
        return cfg

    # fallback shapes
    filters = job.get("filters")
    if isinstance(filters, dict):
        # normalize to cfg-ish keys
        out: Dict[str, Any] = {}
        if "client_tax_ids" in filters:
            out["client_tax_ids"] = filters.get("client_tax_ids")
        if "platforms" in filters:
            out["platforms"] = filters.get("platforms")
        if "client_tags" in filters:
            out["client_tags"] = filters.get("client_tags")
        if "strictMode" in filters:
            out["strictMode"] = filters.get("strictMode")
        if out:
            return out

    return {}


def _get_job_filters(job_service, job_id: str) -> Tuple[List[str], List[str], bool]:
    """
    ✅ FIX: Return (allowed_companies, allowed_platforms, strictMode)
    
    Returns:
        - allowed_companies: empty list means "allow all"
        - allowed_platforms: empty list means "allow all"  
        - strictMode: if True, reject unknown files; if False, allow unknown files
    """
    try:
        job = job_service.get_job(job_id)  # type: ignore[attr-defined]
    except Exception:
        job = None

    if not isinstance(job, dict):
        return ([], [], False)

    # ✅ Extract strictMode
    cfg = job.get("cfg") or {}
    strict_mode = bool(cfg.get("strictMode", False)) if isinstance(cfg, dict) else False

    companies = (
        job.get("company_filters")
        or job.get("companies")
        or job.get("company")
        or job.get("selected_companies")
        or (job.get("filters") or {}).get("companies")
        or (job.get("filters") or {}).get("company")
        or (cfg.get("client_tags") if isinstance(cfg, dict) else None)
        or []
    )

    platforms = (
        job.get("platform_filters")
        or job.get("platforms")
        or job.get("platform")
        or job.get("selected_platforms")
        or (job.get("filters") or {}).get("platforms")
        or (job.get("filters") or {}).get("platform")
        or (cfg.get("platforms") if isinstance(cfg, dict) else None)
        or []
    )

    def _norm_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, str):
            parts = [p.strip() for p in x.split(",") if p.strip()]
            return [p.upper() for p in parts]
        if isinstance(x, (list, tuple, set)):
            return [str(i).strip().upper() for i in x if str(i).strip()]
        return []

    return (_norm_list(companies), _norm_list(platforms), strict_mode)


def _cfg_mismatch(
    allowed_companies: List[str],
    allowed_platforms: List[str],
    strict_mode: bool,
    *,
    company: str,
    platform_u: str,
) -> Tuple[bool, str]:
    """
    ✅ FIX: Improved filter matching logic with strictMode
    
    Args:
        allowed_companies: List of allowed companies (empty = allow all)
        allowed_platforms: List of allowed platforms (empty = allow all)
        strict_mode: If True, reject unknown; if False, allow unknown
        company: Detected company (may be empty)
        platform_u: Detected platform (may be "UNKNOWN")
    
    Returns:
        (is_mismatch, reason)
    
    Rules:
        1. If no filters → allow all (False, "")
        2. If company/platform is known → must match filter
        3. If company/platform is unknown:
           - strictMode=False → allow (False, "")
           - strictMode=True → reject (True, "unknown in strict mode")
    """
    c = (company or "").upper().strip()
    p = (platform_u or "").upper().strip()

    # ✅ No filters → allow all
    has_company_filter = bool(allowed_companies)
    has_platform_filter = bool(allowed_platforms)

    if not has_company_filter and not has_platform_filter:
        return (False, "")

    # ============================================================
    # ✅ COMPANY FILTER
    # ============================================================
    if has_company_filter:
        if c:  # known company
            if c not in allowed_companies:
                return (True, f"company={c} not in allowed companies ({','.join(allowed_companies)})")
        else:  # unknown company
            if strict_mode:
                return (True, f"company=unknown (strict mode, allowed: {','.join(allowed_companies)})")
            # else: allow unknown in non-strict mode

    # ============================================================
    # ✅ PLATFORM FILTER
    # ============================================================
    if has_platform_filter:
        if p and p != "UNKNOWN":  # known platform
            if p not in allowed_platforms:
                return (True, f"platform={p} not in allowed platforms ({','.join(allowed_platforms)})")
        else:  # unknown platform
            if strict_mode:
                return (True, f"platform=unknown (strict mode, allowed: {','.join(allowed_platforms)})")
            # else: allow unknown in non-strict mode

    return (False, "")


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

    row["N_unit_price"] = _clean_money_str(row.get("N_unit_price") or row.get("R_paid_amount") or "0") or "0"
    row["R_paid_amount"] = _clean_money_str(row.get("R_paid_amount") or row.get("N_unit_price") or "0") or "0"

    row["P_wht"] = ""

    row["C_reference"] = _compact_ref(row.get("C_reference"))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no"))

    if not _safe_str(row.get("C_reference")) and _safe_str(row.get("G_invoice_no")):
        row["C_reference"] = _safe_str(row.get("G_invoice_no"))
    if not _safe_str(row.get("G_invoice_no")) and _safe_str(row.get("C_reference")):
        row["G_invoice_no"] = _safe_str(row.get("C_reference"))

    for k in (
        "A_company_name",
        "D_vendor_code",
        "K_account",
        "L_description",
        "Q_payment_method",
        "S_pnd",
        "T_note",
        "U_group",
    ):
        row[k] = _safe_str(row.get(k))


def _extract_embedded_pdf_text(data: bytes, max_pages: int = 15) -> str:
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages: List[str] = []
            for p in pdf.pages[:max_pages]:
                pages.append(p.extract_text() or "")
            return "\n".join(pages).strip()
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


# ============================================================
# Main worker
# ============================================================

def process_job_files(job_service, job_id: str) -> None:
    """
    ✅ FIX summary:
    - Improved filter logic with strictMode support
    - Better company/platform detection (text > cfg > filename)
    - Clear error messages with reasons
    - Always append rows (prevents rows=0)
    """
    payloads: List[Tuple[str, str, bytes]] = job_service.get_payloads(job_id)

    # ✅ Get filters + strictMode
    allowed_companies, allowed_platforms, strict_mode = _get_job_filters(job_service, job_id)
    cfg = _get_job_cfg(job_service, job_id)

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
        file_state = "done"
        message = ""
        rows_out: List[Dict[str, Any]] = []
        tmp_path: Optional[str] = None

        try:
            text = ""
            is_pdf = filename.lower().endswith(".pdf") or (content_type == "application/pdf")

            if is_pdf:
                text = _extract_embedded_pdf_text(data, max_pages=15)

            if not text:
                tmp_path = _write_temp_file(filename, data)
                text = maybe_ocr_to_text(tmp_path)

            text = normalize_text(text)

            # ✅ Determine client/company early (priority: text > cfg > filename)
            client_tax_id = _detect_client_tax_id(text, filename, cfg=cfg)
            company = _company_from_tax_id(client_tax_id, filename)

            if not text:
                # ✅ No text → minimal row
                platform_u = _detect_platform_hint_from_filename(filename) or "UNKNOWN"
                
                # ✅ Check filter mismatch even for no-text case
                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )
                
                row_min = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_status": "NEEDS_REVIEW",
                    "_errors": ["ไม่พบข้อความจากเอกสาร"],
                }
                
                if is_mismatch:
                    row_min["_errors"].append(f"ไม่ตรง filter: {mismatch_reason}")
                
                _normalize_row_fields(row_min, seq=seq)
                rows_out.append(row_min)
                seq += 1

                file_state = "needs_review"
                message = "ไม่พบข้อความ"
                if is_mismatch:
                    message += f" + {mismatch_reason}"
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
                # ✅ Has text → full extraction
                platform, base_row, errors = extract_row_from_text(
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                )

                platform_u = _platform_upper(platform, filename)

                # ✅ Check filter mismatch with improved logic
                is_mismatch, mismatch_reason = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    strict_mode,
                    company=company,
                    platform_u=platform_u,
                )

                seller_id = _detect_seller_id(text, filename)
                shop_name_hint = os.path.splitext(os.path.basename(filename))[0]

                wallet_code = ""
                if resolve_wallet_code is not None:
                    try:
                        wallet_code = resolve_wallet_code(
                            client_tax_id,
                            seller_id=seller_id,
                            shop_name=shop_name_hint,
                            text=text,
                        ) or ""
                    except Exception:
                        wallet_code = ""

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
                else:
                    if platform_u == "SHOPEE":
                        row["_errors"] = list(row.get("_errors") or []) + ["ไม่พบ wallet code (Q_payment_method)"]

                _normalize_row_fields(row, seq=seq)

                # ✅ Call AI if needed
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

                row["P_wht"] = ""
                _normalize_row_fields(row, seq=seq)

                # ✅ Revalidate
                errors2 = _revalidate(row)
                prev_errs = list(row.get("_errors") or [])
                merged = []
                for e in prev_errs + errors2:
                    if e and e not in merged:
                        merged.append(e)
                row["_errors"] = merged

                # ✅ Determine final status with clear messaging
                if is_mismatch:
                    row["_status"] = "NEEDS_REVIEW"
                    row["_errors"].append(f"ไม่ตรง filter: {mismatch_reason}")
                    file_state = "needs_review"
                    message = mismatch_reason
                    review_files += 1
                else:
                    if row["_errors"]:
                        row["_status"] = "NEEDS_REVIEW"
                        file_state = "needs_review"
                        message = "มีช่องที่ต้องตรวจสอบ"
                        review_files += 1
                    else:
                        row["_status"] = "OK"
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

            # ✅ IMPORTANT: always append an ERROR row (prevents rows=0)
            err_row = {
                "A_seq": seq,
                "A_company_name": company or "",
                "_source_file": filename,
                "_platform": platform_u or "UNKNOWN",
                "_status": "ERROR",
                "_errors": [f"{type(e).__name__}: {e}"],
            }
            _normalize_row_fields(err_row, seq=seq)
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