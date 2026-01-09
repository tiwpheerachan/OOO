# backend/app/services/job_worker.py
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

# ✅ NEW: wallet mapping (recommended file you added)
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

# seller/shop id hints in OCR text
RE_SELLER_ID_HINTS = [
    re.compile(r"\b(?:seller_id|seller\s*id|shop_id|shop\s*id|merchant_id|merchant\s*id)\b\D{0,20}(\d{5,20})", re.IGNORECASE),
    re.compile(r"(?:รหัสร้าน|ไอดีร้าน|รหัสผู้ขาย|ร้านค้า)\D{0,20}(\d{5,20})", re.IGNORECASE),
]
RE_ANY_LONG_DIGITS = re.compile(r"\b(\d{6,20})\b")

RE_THAI_DATE_HINT = re.compile(
    r"(?:วันที่|Date)\s*[:#：]?\s*([0-9]{1,2}[\/\-.][0-9]{1,2}[\/\-.][0-9]{2,4})",
    re.IGNORECASE,
)

# Join multi-line invoice/reference tokens: remove ALL whitespace
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
    """
    Keep as string for export. Tries to normalize commas/฿/THB.
    """
    s = _safe_str(v)
    if not s:
        return ""
    return s.replace("฿", "").replace("THB", "").replace(",", "").strip()


def _compact_ref(v: Any) -> str:
    """
    Remove ALL whitespace (spaces, tabs, newlines)
    e.g. "RCSPXSPB00-00000-25 1218-0001593" -> "RCSPXSPB00-00000-251218-0001593"
    """
    s = _safe_str(v)
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _detect_client_tax_id(text: str, filename: str = "") -> str:
    """
    Detect which client company this document belongs to.
    Priority:
      1) Text contains known client tax IDs
      2) Filename/path hints (RABBIT/SHD/TOPONE)
    """
    t = text or ""
    for _, tax in CLIENT_TAX_IDS.items():
        if tax in t:
            return tax

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


def _detect_platform_hint_from_filename(filename: str) -> str:
    fn = (filename or "").upper()
    if "SHOPEE" in fn:
        return "SHOPEE"
    if "LAZADA" in fn:
        return "LAZADA"
    if "TIKTOK" in fn or "TTS" in fn:
        return "TIKTOK"
    if "SPX" in fn:
        return "SPX"
    if "FACEBOOK" in fn or "META" in fn:
        return "FACEBOOK"
    if "HASHTAG" in fn:
        return "HASHTAG"
    return ""


def _platform_upper(platform: str, filename: str = "") -> str:
    p = (platform or "").strip().lower()
    if p in {"shopee", "spx", "lazada", "tiktok"}:
        return p.upper()
    # fallback from filename if classifier says "generic"
    fh = _detect_platform_hint_from_filename(filename)
    return fh or (platform or "UNKNOWN").upper()


def _detect_seller_id(text: str, filename: str = "") -> str:
    """
    Best-effort seller_id/shop_id detection.
    - Try explicit hints in text
    - Else try long digit tokens (6..20)
    - Else try digits in filename
    """
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


def _get_job_filters(job_service, job_id: str) -> Tuple[List[str], List[str]]:
    """
    Pull selected filters from job (best-effort).
    Supports multiple possible shapes because your backend/job_service may differ.

    Returns: (allowed_companies_upper, allowed_platforms_upper)
      - empty list means "allow all"
    """
    try:
        job = job_service.get_job(job_id)  # type: ignore[attr-defined]
    except Exception:
        job = None

    if not isinstance(job, dict):
        return ([], [])

    # try common keys (your frontend may send these)
    # companies
    companies = (
        job.get("company_filters")
        or job.get("companies")
        or job.get("company")
        or job.get("selected_companies")
        or (job.get("filters") or {}).get("companies")
        or (job.get("filters") or {}).get("company")
        or []
    )

    # platforms
    platforms = (
        job.get("platform_filters")
        or job.get("platforms")
        or job.get("platform")
        or job.get("selected_platforms")
        or (job.get("filters") or {}).get("platforms")
        or (job.get("filters") or {}).get("platform")
        or []
    )

    def _norm_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, str):
            # allow "A,B,C"
            parts = [p.strip() for p in x.split(",") if p.strip()]
            return [p.upper() for p in parts]
        if isinstance(x, (list, tuple, set)):
            return [str(i).strip().upper() for i in x if str(i).strip()]
        return []

    return (_norm_list(companies), _norm_list(platforms))


def _cfg_mismatch(
    allowed_companies: List[str],
    allowed_platforms: List[str],
    *,
    company: str,
    platform_u: str,
) -> bool:
    """
    Backend enforcement:
    - If user selected companies/platforms, and file does not match -> mismatch True
    - If selection empty -> allow all
    """
    c = (company or "").upper().strip()
    p = (platform_u or "").upper().strip()

    if allowed_companies and (c not in allowed_companies):
        return True
    if allowed_platforms and (p not in allowed_platforms):
        return True
    return False


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
    """
    Normalize core PEAK fields after extractor + AI merge.
    Keeps everything as strings.
    Implements:
      - A_seq sequential
      - P_wht blank always
      - C_reference/G_invoice_no compact (remove whitespace/newlines)
    """
    # ✅ A_seq must be sequential 1,2,3,...
    row["A_seq"] = seq

    # Dates: must be YYYYMMDD or empty
    for k in ("B_doc_date", "H_invoice_date", "I_tax_purchase_date"):
        if row.get(k):
            row[k] = _digits_only(_safe_str(row.get(k)))[:8]
        else:
            row[k] = _safe_str(row.get(k))

    # Tax / branch
    row["E_tax_id_13"] = _digits_only(_safe_str(row.get("E_tax_id_13")))[:13]
    br = _digits_only(_safe_str(row.get("F_branch_5")))
    row["F_branch_5"] = br.zfill(5)[:5] if br else "00000"

    # price_type
    j = _safe_str(row.get("J_price_type"))
    row["J_price_type"] = j if j in {"1", "2", "3"} else (j or "1")

    # vat_rate
    o = _safe_str(row.get("O_vat_rate")).upper()
    row["O_vat_rate"] = "NO" if o in {"NO", "0", "NONE"} else ("7%" if (o == "" or "7" in o) else o)

    # qty
    row["M_qty"] = _safe_str(row.get("M_qty") or "1") or "1"

    # money-ish
    row["N_unit_price"] = _clean_money_str(row.get("N_unit_price") or row.get("R_paid_amount") or "0") or "0"
    row["R_paid_amount"] = _clean_money_str(row.get("R_paid_amount") or row.get("N_unit_price") or "0") or "0"

    # ✅ WHT must be blank always (your requirement)
    row["P_wht"] = ""

    # ✅ Compact reference/invoice (no whitespace)
    row["C_reference"] = _compact_ref(row.get("C_reference"))
    row["G_invoice_no"] = _compact_ref(row.get("G_invoice_no"))

    # Optional sync: if one empty, copy from the other (after compact)
    if not _safe_str(row.get("C_reference")) and _safe_str(row.get("G_invoice_no")):
        row["C_reference"] = _safe_str(row.get("G_invoice_no"))
    if not _safe_str(row.get("G_invoice_no")) and _safe_str(row.get("C_reference")):
        row["G_invoice_no"] = _safe_str(row.get("C_reference"))

    # strings
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
    """
    PDF -> embedded text via pdfplumber (fast). If scanned, will usually return empty.
    """
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages: List[str] = []
            for p in pdf.pages[:max_pages]:
                pages.append(p.extract_text() or "")
            return "\n".join(pages).strip()
    except Exception:
        return ""


def _write_temp_file(filename: str, data: bytes) -> str:
    """
    Save uploaded bytes to a temp file (keeps extension if possible),
    so OCR pipeline that expects a file path can work.
    """
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


def _append_and_update_file(job_service, job_id: str, idx: int, *, rows: List[Dict[str, Any]], state: str, platform: str, company: str, message: str) -> None:
    """
    Compatible with your older job_service API:
      - append_rows(job_id, rows)
      - update_file(job_id, idx, {...})
    """
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
    ✅ Implements your requirements (backend side):

    1) Backend filter enforcement:
       - อ่าน filter ที่ frontend ส่งมาไว้ใน job (best-effort)
       - ถ้า company/platform ไม่อยู่ในตัวเลือก -> file_state = needs_review
         และ row._status = NEEDS_REVIEW (+ error reason)

    2) A_seq sequential 1..N (ไม่ใช่ 1,1,1)

    3) P_wht blank ALWAYS

    4) C_reference + G_invoice_no: remove whitespace/newlines so tokens glued

    5) Q_payment_method wallet:
       - ใช้ wallet_mapping.resolve_wallet_code(client_tax_id, seller_id, shop_name, text)
       - ถ้า resolve ไม่ได้ -> เพิ่ม error "ไม่พบ wallet code" (ให้ไป review)
       - กัน AI มาเขียนทับ wallet + กัน AI มาใส่ P_wht
    """
    payloads: List[Tuple[str, str, bytes]] = job_service.get_payloads(job_id)

    allowed_companies, allowed_platforms = _get_job_filters(job_service, job_id)

    seq = 1
    ok_files = 0
    review_files = 0
    error_files = 0
    processed = 0

    ai_only_fill_empty = _env_bool("AI_ONLY_FILL_EMPTY", default=False)

    for idx, (filename, content_type, data) in enumerate(payloads):
        filename = filename or "unknown"
        content_type = content_type or ""

        # Start processing state
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

            # 1) embedded PDF text first
            if is_pdf:
                text = _extract_embedded_pdf_text(data, max_pages=15)

            # 2) OCR/text extraction fallback (expects file_path)
            if not text:
                tmp_path = _write_temp_file(filename, data)
                text = maybe_ocr_to_text(tmp_path)

            text = normalize_text(text)

            if not text:
                platform_u = _detect_platform_hint_from_filename(filename) or "UNKNOWN"
                company = _company_from_tax_id("", filename)

                row_min = {
                    "A_seq": seq,
                    "A_company_name": company,
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_status": "NEEDS_REVIEW",
                    "_errors": ["ไม่พบข้อความจากเอกสาร"],
                }
                _normalize_row_fields(row_min, seq=seq)
                rows_out.append(row_min)
                seq += 1

                file_state = "needs_review"
                message = "ยังไม่มีข้อความจากเอกสาร (PDF สแกน/รูปภาพ) — ต้องเปิด OCR หรือรีวิวเอง"
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
                # 3) Extract with rule-based extractor (pass filename so extractor can use it)
                platform, base_row, errors = extract_row_from_text(text, filename=filename, client_tax_id="")

                # detect client/company
                client_tax_id = _detect_client_tax_id(text, filename)
                company = _company_from_tax_id(client_tax_id, filename)

                # normalize platform label (upper)
                platform_u = _platform_upper(platform, filename)

                # backend filter mismatch?
                mismatch = _cfg_mismatch(
                    allowed_companies,
                    allowed_platforms,
                    company=company,
                    platform_u=platform_u,
                )

                # detect seller_id for wallet mapping
                seller_id = _detect_seller_id(text, filename)

                # shop_name hint: filename stem without extension (best-effort)
                shop_name_hint = os.path.splitext(os.path.basename(filename))[0]

                # resolve wallet code (only if mapping module available)
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

                # Build row + merge base extractor row
                row: Dict[str, Any] = {
                    "A_seq": seq,
                    "A_company_name": company,  # ✅ show in table/export
                    "_source_file": filename,
                    "_platform": platform_u,
                    "_client_tax_id": client_tax_id,
                    "_seller_id": seller_id,
                    "_errors": list(errors) if errors else [],
                }
                if isinstance(base_row, dict):
                    row.update(base_row)

                # apply wallet (do not let extractor/AI override later)
                if wallet_code:
                    row["Q_payment_method"] = wallet_code
                else:
                    # wallet is required in your rule-set (at least for Shopee; you can widen later)
                    # we'll enforce for Shopee documents
                    if platform_u == "SHOPEE":
                        row["_errors"] = list(row.get("_errors") or []) + ["ไม่พบ wallet code (Q_payment_method)"]

                # Normalize BEFORE AI
                _normalize_row_fields(row, seq=seq)

                # 4) Optional AI completion step
                # IMPORTANT:
                # - exclude P_wht from AI (always blank)
                # - after AI, re-apply wallet_code and set P_wht blank again
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
                        # "P_wht",  # ❌ DO NOT ask AI (must be blank)
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
                            # keep internal meta keys
                            if k.startswith("_"):
                                row[k] = v
                                continue

                            # Hard bans (your rules)
                            if k == "P_wht":
                                continue

                            v_str = _safe_str(v)
                            if not v_str:
                                continue

                            if ai_only_fill_empty:
                                if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                    row[k] = v_str
                            else:
                                # if base extractor had errors -> allow override
                                if row.get("_errors"):
                                    row[k] = v_str
                                else:
                                    if _safe_str(row.get(k)) in {"", "0", "0.0", "0.00"}:
                                        row[k] = v_str

                # Re-apply wallet AFTER AI (AI must not overwrite)
                if wallet_code:
                    row["Q_payment_method"] = wallet_code
                else:
                    if platform_u == "SHOPEE":
                        # keep the error (already appended above) — no action
                        pass

                # Force rules again AFTER AI
                row["P_wht"] = ""  # ✅ always blank

                # Normalize AFTER AI
                _normalize_row_fields(row, seq=seq)

                # Re-validate
                errors2 = _revalidate(row)
                # Keep wallet error if present
                prev_errs = list(row.get("_errors") or [])
                # Merge unique
                merged = []
                for e in prev_errs + errors2:
                    if e and e not in merged:
                        merged.append(e)
                row["_errors"] = merged

                # 5) Decide status
                if mismatch:
                    row["_status"] = "NEEDS_REVIEW"
                    file_state = "needs_review"
                    message = "ไม่ตรงตัวกรองที่เลือก (backend) — ส่งไป review"
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
                    "rows_count": 0,
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
