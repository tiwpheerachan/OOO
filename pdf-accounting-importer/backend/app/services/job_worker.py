from __future__ import annotations

from typing import List, Dict, Any
import io
import pdfplumber

from .extract_service import extract_row_from_text
from .ocr_service import maybe_ocr_to_text
from .ai_service import ai_fill_peak_row
from ..utils.text_utils import normalize_text
from ..utils.validators import (
    validate_yyyymmdd, validate_branch5, validate_tax13, validate_price_type, validate_vat_rate
)


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

def process_job_files(job_service, job_id: str) -> None:
    payloads = job_service.get_payloads(job_id)
    seq = 1

    ok_files = review_files = error_files = 0
    processed = 0

    for idx, (filename, content_type, data) in enumerate(payloads):
        job_service.update_file(job_id, idx, {"state":"processing"})
        platform = "unknown"
        file_state = "done"
        message = ""
        rows_out: List[Dict[str, Any]] = []

        try:
            text = ""
            is_pdf = filename.lower().endswith(".pdf") or content_type == "application/pdf"
            if is_pdf:
                # try extract embedded text
                with pdfplumber.open(io.BytesIO(data)) as pdf:
                    pages = []
                    for p in pdf.pages[:15]:  # safety cap
                        pages.append(p.extract_text() or "")
                    text = "\n".join(pages).strip()
            else:
                # non-pdf -> OCR hook
                text = ""

            if not text:
                # OCR hook (currently returns "", marks for review)
                text = maybe_ocr_to_text(filename, content_type, data)

            text = normalize_text(text)

            if not text:
                platform = "unknown"
                file_state = "needs_review"
                message = "ยังไม่มี OCR/ข้อความจากเอกสาร (ต้องรีวิว)"
                review_files += 1
            else:
                platform, base_row, errors = extract_row_from_text(text)

                row = {
                    "A_seq": seq,
                    "_source_file": filename,
                    "_platform": platform,
                    "_errors": errors,
                }
                row.update(base_row)

                # --- AI completion step (optional) ---
                # Trigger when there are validation errors OR missing critical fields.
                critical_missing = (
                    not row.get("B_doc_date")
                    or not row.get("L_description")
                    or row.get("R_paid_amount") in {"", None}
                )
                if errors or critical_missing:
                    ai_patch = ai_fill_peak_row(
                        text=text,
                        platform_hint=platform,
                        partial_row={k: row.get(k, "") for k in [
                            "B_doc_date","C_reference","D_vendor_code","E_tax_id_13","F_branch_5",
                            "G_invoice_no","H_invoice_date","I_tax_purchase_date","J_price_type",
                            "K_account","L_description","M_qty","N_unit_price","O_vat_rate","P_wht",
                            "Q_payment_method","R_paid_amount","S_pnd","T_note","U_group"
                        ]},
                        source_filename=filename,
                    )
                    if ai_patch:
                        # Merge AI results into row (do not override A_seq/meta)
                        for k, v in ai_patch.items():
                            if k.startswith("_"):
                                row[k] = v
                            else:
                                # Prefer AI when field is empty or we had errors
                                if (row.get(k, "") in {"", None}) or errors:
                                    row[k] = v

                        # Add helpful note when tax id missing
                        if not row.get("E_tax_id_13"):
                            note = (row.get("T_note") or "").strip()
                            hint = "e-Tax Ready = NO (ไม่พบเลขผู้เสียภาษี 13 หลัก)"
                            if hint not in note:
                                row["T_note"] = (note + (" | " if note else "") + hint).strip()

                # Re-validate after AI
                errors2 = _revalidate(row)
                row["_errors"] = errors2

                # status
                if errors2:
                    row["_status"] = "NEEDS_REVIEW"
                    file_state = "needs_review"
                    message = "มีช่องที่ต้องตรวจสอบ (ใช้ OCR/AI แล้ว)" if row.get("_ai_confidence") else "มีช่องที่ต้องตรวจสอบ"
                    review_files += 1
                else:
                    row["_status"] = "OK"
                    ok_files += 1

                rows_out.append(row)
                seq += 1

            job_service.append_rows(job_id, rows_out)
            job_service.update_file(job_id, idx, {
                "state": file_state,
                "platform": platform,
                "message": message,
                "rows_count": len(rows_out),
            })

        except Exception as e:
            error_files += 1
            job_service.update_file(job_id, idx, {
                "state": "error",
                "platform": platform,
                "message": f"Error: {type(e).__name__}: {e}",
                "rows_count": 0,
            })

        processed += 1
        job = job_service.get_job(job_id) or {}
        job_service.update_job(job_id, {
            "processed_files": processed,
            "ok_files": ok_files,
            "review_files": review_files,
            "error_files": error_files,
        })

    final_state = "done" if error_files == 0 else "error"
    job_service.update_job(job_id, {"state": final_state})
