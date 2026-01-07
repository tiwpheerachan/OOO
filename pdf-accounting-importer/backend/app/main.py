from __future__ import annotations

import io
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

# =========================
# ✅ Load .env intelligently
# =========================
def _load_env_safely() -> None:
    """
    โหลด .env แบบฉลาด:
    - รองรับรันจาก backend/ หรือรันจาก root project
    - ไม่พังถ้าไม่มี python-dotenv (ยังรันได้ด้วย ENV ของระบบ)
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    # candidate .env paths
    here = Path(__file__).resolve()
    backend_dir = here.parents[2]  # .../backend
    app_dir = here.parent          # .../backend/app
    project_root_guess = backend_dir.parent

    candidates = [
        backend_dir / ".env",
        app_dir / ".env",
        project_root_guess / ".env",
    ]

    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=False)
            # ใช้ไฟล์แรกที่เจอ
            return

    # ถ้าไม่เจอไฟล์ ก็ลองโหลดแบบ default (จะโหลด .env ใน cwd ถ้ามี)
    load_dotenv(override=False)


_load_env_safely()

# =========================
# App imports (after ENV)
# =========================
from .services.job_service import JobService
from .services.export_service import export_rows_to_csv_bytes, export_rows_to_xlsx_bytes

# =========================
# FastAPI app
# =========================
app = FastAPI(title="PDF Accounting Importer (PEAK A–U)")

# =========================
# CORS (configurable)
# =========================
cors_origins = os.getenv("CORS_ORIGINS", "*")
if cors_origins.strip() == "*":
    allow_origins = ["*"]
else:
    allow_origins = [o.strip() for o in cors_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs = JobService()

# =========================
# Error handler (nice JSON)
# =========================
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    debug = os.getenv("DEBUG", "0") == "1"
    payload: Dict[str, Any] = {
        "ok": False,
        "error": "internal_error",
        "message": str(exc) if debug else "Internal server error",
        "path": str(request.url),
    }
    return JSONResponse(status_code=500, content=payload)


# =========================
# Routes
# =========================
@app.get("/api/health")
def health():
    """
    Health check + config summary (ไม่โชว์ secrets)
    """
    return {
        "ok": True,
        "ai": {
            "enabled": os.getenv("ENABLE_AI_EXTRACT", "0") == "1",
            "provider": os.getenv("AI_PROVIDER", ""),
            "model": os.getenv("OPENAI_MODEL", ""),
            "repair_pass": os.getenv("AI_REPAIR_PASS", "0") == "1",
            "fill_missing": os.getenv("AI_FILL_MISSING", "1") == "1",
            "has_openai_key": bool(os.getenv("OPENAI_API_KEY")),
        },
        "ocr": {
            "enabled": os.getenv("ENABLE_OCR", "1") == "1",
            "provider": os.getenv("OCR_PROVIDER", "paddle"),
        },
        "cors": {"origins": allow_origins},
    }


@app.get("/api/config")
def config_check():
    """
    ตรวจว่า ENV สำคัญมาไหม (ไม่เปิดเผย key จริง)
    """
    return {
        "ok": True,
        "env": {
            "ENABLE_AI_EXTRACT": os.getenv("ENABLE_AI_EXTRACT", ""),
            "AI_PROVIDER": os.getenv("AI_PROVIDER", ""),
            "OPENAI_MODEL": os.getenv("OPENAI_MODEL", ""),
            "AI_REPAIR_PASS": os.getenv("AI_REPAIR_PASS", ""),
            "AI_FILL_MISSING": os.getenv("AI_FILL_MISSING", ""),
            "OCR_PROVIDER": os.getenv("OCR_PROVIDER", ""),
            "ENABLE_OCR": os.getenv("ENABLE_OCR", ""),
            "CORS_ORIGINS": os.getenv("CORS_ORIGINS", ""),
            "OPENAI_API_KEY_present": bool(os.getenv("OPENAI_API_KEY")),
        },
    }


@app.post("/api/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    # soft limit (กัน RAM พัง) ปรับได้ด้วย ENV
    max_files = int(os.getenv("MAX_UPLOAD_FILES", "500"))
    if len(files) > max_files:
        raise HTTPException(status_code=400, detail=f"Too many files (max {max_files})")

    job_id = jobs.create_job()

    for f in files:
        content = await f.read()
        if not content:
            continue

        # จำกัดขนาดไฟล์ ปรับได้ด้วย ENV
        max_mb = float(os.getenv("MAX_FILE_MB", "25"))
        if len(content) > int(max_mb * 1024 * 1024):
            raise HTTPException(
                status_code=400,
                detail=f"File too large: {f.filename} (max {max_mb} MB)",
            )

        jobs.add_file(
            job_id=job_id,
            filename=f.filename or "unknown",
            content_type=f.content_type or "",
            content=content,
        )

    jobs.start_processing(job_id)
    return {"ok": True, "job_id": job_id}


@app.get("/api/job/{job_id}")
def get_job(job_id: str):
    job = jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/job/{job_id}/rows")
def get_rows(job_id: str):
    rows = jobs.get_rows(job_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "rows": rows}


@app.get("/api/export/{job_id}.csv")
def export_csv(job_id: str):
    rows = jobs.get_rows(job_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Job not found")

    data = export_rows_to_csv_bytes(rows)
    filename = f"peak_import_{job_id}.csv"
    return StreamingResponse(
        io.BytesIO(data),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/{job_id}.xlsx")
def export_xlsx(job_id: str):
    rows = jobs.get_rows(job_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Job not found")

    data = export_rows_to_xlsx_bytes(rows)
    filename = f"peak_import_{job_id}.xlsx"
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
