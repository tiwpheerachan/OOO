# backend/app/extractors/tiktok.py
from __future__ import annotations

import re
from typing import Dict, Any, Tuple, List

from .common import (
    base_row_dict,
    # TikTok-focused helpers (จาก common.py ของคุณ)
    tiktok_tax_reg_number,
    tiktok_invoice_number,
    tiktok_invoice_date_yyyymmdd,
    tiktok_totals,
    tiktok_fee_summary,
    # generic fallbacks
    find_branch,
    find_best_date,
    find_totals,
)
from ..utils.text_utils import normalize_text, parse_money


# -------------------------
# Extra TikTok fallback patterns (รองรับเคส OCR เพี้ยน/สลับคำ)
# -------------------------
RE_TIKTOK_SELLER_TAX_FALLBACK = re.compile(
    r"(?:Tax\s*Registration\s*Number|VAT\s*Registration\s*Number|เลขประจำตัวผู้เสียภาษี(?:อากร)?ของผู้ขาย|เลขประจำตัวผู้เสียภาษี)\s*[:：]?\s*([0-9]{13})",
    re.IGNORECASE,
)

RE_TIKTOK_INVOICE_NO_FALLBACK = re.compile(
    r"(?:Invoice\s*number|Invoice\s*No\.?|Tax\s*Invoice\s*No\.?|เลขที่(?:ใบกำกับภาษี|เอกสาร))\s*[:：]?\s*([A-Z0-9\-_/.]+)",
    re.IGNORECASE,
)

RE_TIKTOK_PERIOD = re.compile(
    r"(?:Period|งวด)\s*[:：]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})\s*[-–]\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})",
    re.IGNORECASE,
)

RE_WHT_TH = re.compile(
    r"(?:หักภาษี\s*ณ\s*ที่จ่าย).*?(?:อัตรา\s*3%|ร้อยละ\s*3).*?(?:จำนวน|เป็นจำนวน)\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_WHT_EN = re.compile(
    r"(?:withheld\s*tax).*?(?:rate\s*of\s*3%|3%).*?(?:amounting\s*to)\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)

RE_TOTAL_INC_FALLBACK = re.compile(
    r"(?:Total\s*amount).*?(?:including\s*VAT|รวม\s*ภาษี).*?฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)
RE_SUBTOTAL_EX_FALLBACK = re.compile(
    r"(?:Subtotal).*?(?:excluding\s*VAT|ไม่รวม\s*VAT|ก่อน\s*VAT).*?฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)


def _safe_pick_first(*vals: str) -> str:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _pick_seller_tax_id(t: str) -> str:
    # 1) helper ที่แม่น (Tax Registration Number: 13 digits)
    tax = tiktok_tax_reg_number(t) or ""
    if tax:
        return tax

    # 2) fallback regex
    m = RE_TIKTOK_SELLER_TAX_FALLBACK.search(t)
    if m:
        return m.group(1).strip()

    # 3) สุดท้ายค่อยใช้ common totals/tax finder (กันพลาด)
    # NOTE: ไม่ดึง Tax ID ของ "Client/Bill To" มาเป็นผู้ขาย
    return ""


def _pick_invoice_no(t: str) -> str:
    inv = tiktok_invoice_number(t) or ""
    if inv:
        return inv

    m = RE_TIKTOK_INVOICE_NO_FALLBACK.search(t)
    if m:
        return m.group(1).strip()

    # fallback สุดท้าย: TTSTHxxxx pattern
    m2 = re.search(r"\b(TTSTH[0-9]{6,})\b", t, re.IGNORECASE)
    return m2.group(1) if m2 else ""


def _pick_invoice_date(t: str) -> str:
    # 1) helper อ่าน "Dec 9, 2025" → 20251209
    d = tiktok_invoice_date_yyyymmdd(t) or ""
    if d:
        return d

    # 2) fallback: ใช้ best date (แต่ไม่ชัวร์เท่า invoice date)
    return find_best_date(t) or ""


def _pick_period_note(t: str) -> str:
    m = RE_TIKTOK_PERIOD.search(t)
    if not m:
        return ""
    a = (m.group(1) or "").strip()
    b = (m.group(2) or "").strip()
    if a and b:
        return f"Period {a} - {b}"
    return ""


def _pick_wht_amount(t: str) -> str:
    # TikTok ระบุ “withheld tax … amounting to ฿x” (เป็นจำนวนเงิน)
    m = RE_WHT_EN.search(t)
    if m:
        return parse_money(m.group(1)) or ""
    m2 = RE_WHT_TH.search(t)
    if m2:
        return parse_money(m2.group(1)) or ""
    return ""


def _pick_amounts_best_effort(t: str) -> Tuple[str, str, str]:
    """
    Returns: (subtotal_ex, vat7, total_inc)
    - subtotal_ex: ก่อน VAT
    - vat7: VAT 7% amount
    - total_inc: รวม VAT
    """
    # 1) ใช้ helper TikTok ที่ทำมาเฉพาะ
    tt = tiktok_totals(t) or {}
    subtotal_ex = (tt.get("subtotal_ex") or "").strip()
    vat7 = (tt.get("vat_7") or "").strip()
    total_inc = (tt.get("total_inc") or "").strip()

    # 2) fallback regex (กรณี OCR เพี้ยน)
    if not subtotal_ex:
        m = RE_SUBTOTAL_EX_FALLBACK.search(t)
        if m:
            subtotal_ex = parse_money(m.group(1)) or ""
    if not total_inc:
        m = RE_TOTAL_INC_FALLBACK.search(t)
        if m:
            total_inc = parse_money(m.group(1)) or ""

    # 3) fallback generic totals (กรณีคำว่า Total amount โดนตัด)
    if not total_inc or not subtotal_ex:
        g = find_totals(t) or {}
        total_inc = total_inc or (g.get("total_due") or "")
        subtotal_ex = subtotal_ex or (g.get("subtotal") or "")

    return subtotal_ex, vat7, total_inc


def extract_tiktok(text: str) -> Dict[str, Any]:
    """
    TikTok Tax Invoice / Receipt → PEAK A–U row

    ✅ แนวคิด (ให้ผลแม่นกับไฟล์จริงของ TikTok):
    - D_vendor_code = "TikTok" (รหัสคู่ค้า)
    - E_tax_id_13   = Tax Registration Number ของ TikTok (ผู้ขาย/ผู้ออกเอกสาร)
    - F_branch_5    = "00000" ถ้าเป็น Head Office
    - B/H/I date    = Invoice date (ถ้ามี) ไม่งั้น fallback หา date ที่ดีที่สุด
    - G/C ref       = Invoice number
    - N_unit_price  = Subtotal (excluding VAT) (ถ้ามี) ไม่งั้นใช้ Total
    - R_paid_amount = Total (including VAT) (ถ้ามี)
    - P_wht         = จำนวนเงิน WHT (เช่น 4,414.88) ไม่ใช่ "3%"
    - S_pnd         = 53 เมื่อมี WHT
    """
    t = normalize_text(text)
    row = base_row_dict()

    # คู่ค้า (รหัส/ชื่อสั้น) — ถ้าคุณอยากเก็บชื่อเต็มไว้ทำ mapping ค่อยใส่เพิ่มภายหลัง
    row["D_vendor_code"] = "TikTok"
    row["U_group"] = "Marketplace Expense"
    row["Q_payment_method"] = "หักจากยอดขาย"  # ปรับได้

    # Tax ID / Branch (ของ TikTok ผู้ขาย)
    seller_tax = _pick_seller_tax_id(t)
    if seller_tax:
        row["E_tax_id_13"] = seller_tax

    # TikTok ตัวอย่างเป็น Head Office
    row["F_branch_5"] = find_branch(t) or "00000"

    # Invoice no/date
    inv_no = _pick_invoice_no(t)
    inv_date = _pick_invoice_date(t)
    if inv_no:
        row["G_invoice_no"] = inv_no
        row["C_reference"] = inv_no

    if inv_date:
        row["B_doc_date"] = inv_date
        row["H_invoice_date"] = inv_date
        row["I_tax_purchase_date"] = inv_date

    # Amounts (subtotal/vat/total)
    subtotal_ex, vat7, total_inc = _pick_amounts_best_effort(t)

    row["M_qty"] = "1"

    # ✅ ที่ “ควร” ใส่ใน PEAK:
    # - N_unit_price = subtotal ก่อน VAT ถ้ามี
    # - R_paid_amount = total รวม VAT ถ้ามี
    # ถ้าไม่มี subtotal ให้ใช้ total แทน
    if subtotal_ex:
        row["N_unit_price"] = subtotal_ex
    elif total_inc:
        row["N_unit_price"] = total_inc

    if total_inc:
        row["R_paid_amount"] = total_inc
    elif subtotal_ex:
        row["R_paid_amount"] = subtotal_ex

    # VAT: ถ้าเห็น VAT 7% หรือเอกสารแนวนี้ ให้ล็อก 7%
    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # WHT amount (จำนวนเงิน)
    wht_amt = _pick_wht_amount(t)
    if wht_amt:
        row["P_wht"] = wht_amt
        row["S_pnd"] = "53"

    # Description / Notes
    short, fee_note = tiktok_fee_summary(t)
    period_note = _pick_period_note(t)

    desc_parts: List[str] = ["TikTok Platform Fees (Tax Invoice/Receipt)"]
    if short:
        desc_parts.append(short)
    if period_note:
        desc_parts.append(period_note)

    # ใส่สมการยอดแบบอ่านง่าย (ช่วยตรวจด้วยตา)
    if subtotal_ex and vat7 and total_inc:
        desc_parts.append(f"Subtotal {subtotal_ex} + VAT {vat7} = Total {total_inc}")
    elif total_inc:
        desc_parts.append(f"Total {total_inc}")

    if wht_amt:
        desc_parts.append(f"WHT 3% {wht_amt}")

    row["L_description"] = " | ".join([p for p in desc_parts if p])

    # หมายเหตุ (รายละเอียดรายการ fee lines)
    row["T_note"] = fee_note or ""

    return row
