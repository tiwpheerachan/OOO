from __future__ import annotations

import io
import os
import json
import inspect
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
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

    here = Path(__file__).resolve()
    backend_dir = here.parents[2]       # .../backend
    app_dir = here.parent               # .../backend/app
    project_root_guess = backend_dir.parent

    candidates = [
        backend_dir / ".env",
        app_dir / ".env",
        project_root_guess / ".env",
    ]

    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=False)
            return

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
# Helpers: cfg parsing + safe service call
# =========================
def _parse_list_field(raw: Optional[str]) -> List[str]:
    """
    รองรับ:
      - JSON string: '["SHD","RABBIT"]'
      - comma-separated: 'SHD,RABBIT'
      - single: 'SHD'
      - empty/None -> []
    """
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []

    # Try JSON
    if (s.startswith("[") and s.endswith("]")) or (s.startswith('"') and s.endswith('"')):
        try:
            v = json.loads(s)
            if isinstance(v, list):
                out: List[str] = []
                for x in v:
                    xs = str(x).strip()
                    if xs:
                        out.append(xs)
                return out
            if isinstance(v, str):
                return [v.strip()] if v.strip() else []
        except Exception:
            pass

    # Fallback: comma separated
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]

    return [s]


def _normalize_cfg(
    client_tags: Optional[str],
    client_tax_ids: Optional[str],
    platforms: Optional[str],
) -> Dict[str, Any]:
    """
    ทำ cfg ให้สะอาด + normalize ตัวอักษร
    """
    tags = [t.upper().strip() for t in _parse_list_field(client_tags)]
    plats = [p.upper().strip() for p in _parse_list_field(platforms)]
    taxs = [t.strip() for t in _parse_list_field(client_tax_ids)]

    # ตัดค่าซ้ำ โดยยังรักษาลำดับ
    def uniq(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        "client_tags": uniq(tags),
        "client_tax_ids": uniq(taxs),
        "platforms": uniq(plats),
    }


def _call_if_supported(obj: Any, method_name: str, /, *args: Any, **kwargs: Any) -> Any:
    """
    เรียก method แบบ backward-compatible:
    - ถ้า method มีพารามิเตอร์ตาม kwargs -> ส่งให้
    - ถ้าไม่รองรับ -> ตัด kwargs ออกแล้วเรียกแบบเดิม
    """
    fn = getattr(obj, method_name, None)
    if fn is None:
        raise AttributeError(f"{type(obj).__name__}.{method_name} not found")

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        supported = {k: v for k, v in kwargs.items() if k in params}
        return fn(*args, **supported)
    except Exception:
        # fallback: call without kwargs
        return fn(*args)


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
async def upload(
    files: List[UploadFile] = File(...),
    # ✅ NEW: รับ cfg จาก FormData
    client_tags: Optional[str] = Form(None),
    client_tax_ids: Optional[str] = Form(None),
    platforms: Optional[str] = Form(None),
):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    # ✅ parse + normalize cfg
    cfg = _normalize_cfg(client_tags, client_tax_ids, platforms)

    # soft limit (กัน RAM พัง) ปรับได้ด้วย ENV
    max_files = int(os.getenv("MAX_UPLOAD_FILES", "500"))
    if len(files) > max_files:
        raise HTTPException(status_code=400, detail=f"Too many files (max {max_files})")

    # ✅ create job (attach cfg if JobService supports it)
    job_id = _call_if_supported(jobs, "create_job", cfg=cfg)

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

        # ✅ add file (attach cfg if add_file supports it)
        _call_if_supported(
            jobs,
            "add_file",
            job_id=job_id,
            filename=f.filename or "unknown",
            content_type=f.content_type or "",
            content=content,
            cfg=cfg,  # optional (only if supported)
        )

    # ✅ start processing (attach cfg if start_processing supports it)
    _call_if_supported(jobs, "start_processing", job_id, cfg=cfg)

    return {"ok": True, "job_id": job_id, "cfg": cfg}


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
