from __future__ import annotations

import io
import os
import json
import time
import inspect
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

logger = logging.getLogger(__name__)

# ============================================================
# ✅ Logging defaults (safe)
# ============================================================
def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        level = "INFO"
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

_setup_logging()

# ============================================================
# ✅ Load .env intelligently
# ============================================================
def _load_env_safely() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    here = Path(__file__).resolve()
    backend_dir = here.parents[1]
    app_dir = here.parent
    project_root_guess = backend_dir.parent

    candidates = [
        backend_dir / ".env",
        app_dir / ".env",
        project_root_guess / ".env",
    ]

    for p in candidates:
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=False)
            logger.info("Loaded .env: %s", str(p))
            return

    load_dotenv(override=False)

_load_env_safely()

# ============================================================
# App imports (after ENV)
# ============================================================
from .services.job_service import JobService
from .services.export_service import export_rows_to_csv_bytes, export_rows_to_xlsx_bytes

# ============================================================
# FastAPI app
# ============================================================
app = FastAPI(title="PDF Accounting Importer (PEAK A–U)")

# ============================================================
# CORS (configurable)
# ============================================================
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

# ============================================================
# ✅ Helpers
# ============================================================
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _parse_bool_field(raw: Optional[str], default: Optional[bool] = None) -> Optional[bool]:
    """
    Accept:
      - "1"/"0"
      - "true"/"false"
      - "yes"/"no"
      - ""/None
    Return:
      - bool or None (if cannot parse and default is None)
    """
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s == "":
        return default
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default

def _now_ms() -> int:
    return int(time.time() * 1000)

def _safe_filename(name: str) -> str:
    n = (name or "").strip()
    return n if n else "unknown"

# ============================================================
# ✅ Error handler (nice JSON)
# ============================================================
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    debug = _env_bool("DEBUG", default=False)
    payload: Dict[str, Any] = {
        "ok": False,
        "error": "internal_error",
        "message": str(exc) if debug else "Internal server error",
        "path": str(request.url),
        "method": request.method,
    }
    try:
        logger.exception("Unhandled error: %s %s", request.method, request.url)
    except Exception:
        pass
    return JSONResponse(status_code=500, content=payload)

# ============================================================
# ✅ Helpers: cfg parsing + safe service call
# ============================================================
def _parse_list_field(raw: Optional[str]) -> List[str]:
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
    compute_wht: Optional[str],
    strictMode: Optional[str] = None,
) -> Dict[str, Any]:
    tags = [t.upper().strip() for t in _parse_list_field(client_tags)]
    plats = [p.lower().strip() for p in _parse_list_field(platforms)]
    taxs = [t.strip() for t in _parse_list_field(client_tax_ids)]

    def uniq(seq: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in seq:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    cw = _parse_bool_field(compute_wht, default=None)
    sm = _parse_bool_field(strictMode, default=None)

    cfg: Dict[str, Any] = {
        "client_tags": uniq(tags),
        "client_tax_ids": uniq(taxs),
        "platforms": uniq(plats),
    }

    # ✅ important: compute_wht flag
    # - If user didn't send: keep None (worker/extractor can decide default)
    # - If sent: True/False
    if cw is not None:
        cfg["compute_wht"] = bool(cw)

    # optional strict mode (if your UI sends it)
    if sm is not None:
        cfg["strictMode"] = bool(sm)

    return cfg

def _call_if_supported(obj: Any, method_name: str, /, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(obj, method_name, None)
    if fn is None:
        raise AttributeError(f"{type(obj).__name__}.{method_name} not found")

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        supported = {k: v for k, v in kwargs.items() if k in params}
        return fn(*args, **supported)
    except Exception:
        return fn(*args)

# ============================================================
# ✅ Streaming read with max bytes (prevent RAM blow)
# ============================================================
async def _read_uploadfile_safely(f: UploadFile, max_bytes: int) -> bytes:
    buf = io.BytesIO()
    total = 0
    chunk_size = int(os.getenv("UPLOAD_READ_CHUNK_BYTES", "1048576"))  # 1MB default

    while True:
        chunk = await f.read(chunk_size)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=400,
                detail=f"File too large: {f.filename} (max {max_bytes/1024/1024:.1f} MB)",
            )
        buf.write(chunk)

    return buf.getvalue()

# ============================================================
# ✅ Minimal platform whitelist
# ============================================================
ALLOWED_PLATFORMS = {"shopee", "lazada", "tiktok", "spx", "ads", "other", "unknown"}

# ============================================================
# Routes
# ============================================================
@app.get("/api/health")
def health():
    return {
        "ok": True,
        "time_ms": _now_ms(),
        "ai": {
            "enabled": _env_bool("ENABLE_AI_EXTRACT", default=False) or _env_bool("ENABLE_LLM", default=False),
            "provider": os.getenv("AI_PROVIDER", ""),
            "model": os.getenv("OPENAI_MODEL", ""),
            "repair_pass": _env_bool("AI_REPAIR_PASS", default=False),
            "fill_missing": _env_bool("AI_FILL_MISSING", default=True),
            "has_openai_key": bool(os.getenv("OPENAI_API_KEY")),
        },
        "ocr": {
            "enabled": _env_bool("ENABLE_OCR", default=True),
            "provider": os.getenv("OCR_PROVIDER", "paddle"),
        },
        "cors": {"origins": allow_origins},
        "limits": {
            "max_files": int(os.getenv("MAX_UPLOAD_FILES", "500")),
            "max_file_mb": float(os.getenv("MAX_FILE_MB", "25")),
        },
    }

@app.get("/api/config")
def config_check():
    return {
        "ok": True,
        "env": {
            "DEBUG": os.getenv("DEBUG", ""),
            "LOG_LEVEL": os.getenv("LOG_LEVEL", ""),
            "ENABLE_AI_EXTRACT": os.getenv("ENABLE_AI_EXTRACT", ""),
            "ENABLE_LLM": os.getenv("ENABLE_LLM", ""),
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
    client_tags: Optional[str] = Form(None),
    client_tax_ids: Optional[str] = Form(None),
    platforms: Optional[str] = Form(None),
    # ✅ NEW: compute_wht flag from UI
    compute_wht: Optional[str] = Form(None),
    # optional: strict mode if UI sends it
    strictMode: Optional[str] = Form(None),
):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    cfg = _normalize_cfg(client_tags, client_tax_ids, platforms, compute_wht, strictMode=strictMode)

    # keep only allowed platforms
    if cfg.get("platforms"):
        cfg["platforms"] = [p for p in cfg["platforms"] if p in ALLOWED_PLATFORMS]

    # If compute_wht not provided -> choose default here (so whole pipeline consistent)
    # You can flip to False if your default is "ไม่คำนวณ"
    if "compute_wht" not in cfg:
        cfg["compute_wht"] = True

    # limits
    max_files = int(os.getenv("MAX_UPLOAD_FILES", "500"))
    if len(files) > max_files:
        raise HTTPException(status_code=400, detail=f"Too many files (max {max_files})")

    max_mb = float(os.getenv("MAX_FILE_MB", "25"))
    max_bytes = int(max_mb * 1024 * 1024)

    # create job (attach cfg if supported)
    job_id = _call_if_supported(jobs, "create_job", cfg=cfg)

    added = 0
    skipped = 0
    reasons: List[str] = []

    for f in files:
        filename = _safe_filename(f.filename or "")

        ctype = (f.content_type or "").lower()
        if ctype and not (
            ctype.startswith("application/pdf")
            or ctype.startswith("image/")
            or ctype == "application/octet-stream"
        ):
            skipped += 1
            reasons.append(f"skip:{filename}:unsupported_content_type:{ctype}")
            continue

        content = await _read_uploadfile_safely(f, max_bytes=max_bytes)
        if not content:
            skipped += 1
            reasons.append(f"skip:{filename}:empty")
            continue

        _call_if_supported(
            jobs,
            "add_file",
            job_id=job_id,
            filename=filename,
            content_type=f.content_type or "",
            content=content,
            cfg=cfg,  # optional
        )
        added += 1

    if added == 0:
        raise HTTPException(
            status_code=400,
            detail={"message": "All uploaded files were invalid/empty", "reasons": reasons[:50]},
        )

    # start processing (attach cfg if supported)
    _call_if_supported(jobs, "start_processing", job_id, cfg=cfg)

    return {
        "ok": True,
        "job_id": job_id,
        "cfg": cfg,
        "files_added": added,
        "files_skipped": skipped,
        "skip_reasons_sample": reasons[:25],
    }

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
