from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any

PlatformLabel = Literal["shopee","lazada","tiktok","other","ads","unknown"]
RowStatus = Literal["OK","NEEDS_REVIEW","ERROR"]

class ExtractedRow(BaseModel):
    # PEAK columns (A–U)
    A_seq: int = Field(..., description="ลำดับที่*")
    B_doc_date: str = Field("", description="วันที่เอกสาร (YYYYMMDD)")
    C_reference: str = Field("", description="อ้างอิงถึง")
    D_vendor_code: str = Field("", description="ผู้รับเงิน/คู่ค้า (รหัสผู้ติดต่อ)")
    E_tax_id_13: str = Field("", description="เลขทะเบียน 13 หลัก")
    F_branch_5: str = Field("", description="เลขสาขา 5 หลัก")
    G_invoice_no: str = Field("", description="เลขที่ใบกำกับฯ")
    H_invoice_date: str = Field("", description="วันที่ใบกำกับฯ (YYYYMMDD)")
    I_tax_purchase_date: str = Field("", description="วันที่บันทึกภาษีซื้อ (YYYYMMDD)")
    J_price_type: str = Field("1", description="ประเภทราคา 1/2/3")
    K_account: str = Field("", description="บัญชี (GL code)")
    L_description: str = Field("", description="คำอธิบาย")
    M_qty: str = Field("1", description="จำนวน")
    N_unit_price: str = Field("0", description="ราคาต่อหน่วย")
    O_vat_rate: str = Field("7%", description="อัตราภาษี")
    P_wht: str = Field("0", description="หัก ณ ที่จ่าย")
    Q_payment_method: str = Field("", description="ชำระโดย")
    R_paid_amount: str = Field("0", description="จำนวนเงินที่ชำระ")
    S_pnd: str = Field("", description="ภ.ง.ด.")
    T_note: str = Field("", description="หมายเหตุ")
    U_group: str = Field("", description="กลุ่มจัดประเภท")

    # meta columns for UI
    _status: RowStatus = "NEEDS_REVIEW"
    _platform: PlatformLabel = "unknown"
    _source_file: str = ""
    _errors: List[str] = []

class FileResult(BaseModel):
    filename: str
    platform: PlatformLabel
    state: Literal["queued","processing","done","needs_review","error"]
    message: str = ""
    rows_count: int = 0

class JobStatus(BaseModel):
    job_id: str
    created_at: str
    state: Literal["queued","processing","done","error"]
    total_files: int
    processed_files: int
    ok_files: int
    review_files: int
    error_files: int
    files: List[FileResult]
