# backend/app/services/job_service.py
"""
Job Service - Final Version (Fixed Circular Import)

✅ Enhancements:
1. ✅ Support for 8 platforms (META, GOOGLE, SHOPEE, LAZADA, TIKTOK, SPX, THAI_TAX, UNKNOWN)
2. ✅ Platform validation (only valid platforms allowed in cfg)
3. ✅ Platform normalization (lowercase → UPPERCASE)
4. ✅ Enhanced summary (platform breakdown, extraction methods)
5. ✅ Better metadata tracking
6. ✅ Backward compatible with job_worker.py
7. ✅ Fixed circular import (uses platform_constants.py + lazy import)
"""
from __future__ import annotations

import uuid
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# ✅ Import from platform_constants (NO circular import!)
from .platform_constants import (
    VALID_PLATFORMS,
    PLATFORM_GROUPS,
    LEGACY_PLATFORM_MAP,
    normalize_platform as _norm_platform,
)

# ✅ NO top-level import of job_worker (prevents circular import)
# Instead, we use lazy import inside start_processing()

# ============================================================
# Helpers
# ============================================================

def _utc_iso_z(dt: Optional[datetime] = None) -> str:
    """Generate UTC ISO timestamp with Z suffix"""
    dt = dt or datetime.now(timezone.utc)
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_token(s: Any) -> str:
    """Normalize token to uppercase"""
    return str(s or "").strip().upper()


def _norm_list(xs: Any) -> List[str]:
    """Normalize list of tokens to uppercase"""
    if not xs:
        return []
    if isinstance(xs, (list, tuple)):
        out = []
        for x in xs:
            t = _norm_token(x)
            if t:
                out.append(t)
        # unique keep order
        seen = set()
        uniq = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq
    # if string "A,B"
    s = str(xs).strip()
    if not s:
        return []
    if "," in s:
        return _norm_list([p for p in s.split(",") if p.strip()])
    return [_norm_token(s)]


def _norm_platforms(ps: Any) -> List[str]:
    """
    ✅ Normalize list of platforms to valid UPPERCASE platforms
    
    Uses shared normalize_platform from platform_constants
    """
    if not ps:
        return []
    
    # Handle list/tuple
    if isinstance(ps, (list, tuple)):
        out = []
        for p in ps:
            normalized = _norm_platform(p)
            if normalized:
                out.append(normalized)
        # Unique keep order
        seen = set()
        uniq = []
        for p in out:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return uniq
    
    # Handle string "A,B"
    s = str(ps).strip()
    if not s:
        return []
    if "," in s:
        return _norm_platforms([p for p in s.split(",") if p.strip()])
    
    # Single platform
    normalized = _norm_platform(s)
    return [normalized] if normalized else []


def _safe_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    ✅ Normalize cfg to stable shape + validate platforms
    
    Expected keys:
      - client_tags: ["SHD","RABBIT","TOPONE",...]
      - client_tax_ids: ["0105...","..."]
      - platforms: ["META","SHOPEE",...] (✅ validated)
      - strictMode: bool (default False)
    
    Empty list = allow all
    """
    cfg = cfg or {}
    
    # Normalize platforms with validation
    platforms_raw = cfg.get("platforms")
    platforms_normalized = _norm_platforms(platforms_raw)
    
    return {
        "client_tags": _norm_list(cfg.get("client_tags")),
        "client_tax_ids": [str(x).strip() for x in (cfg.get("client_tax_ids") or []) if str(x).strip()],
        "platforms": platforms_normalized,  # ✅ Validated platforms only
        "strictMode": bool(cfg.get("strictMode", False)),
    }


# ============================================================
# Job Service
# ============================================================

class JobService:
    """
    ✅ Enhanced Job Service (Fixed Circular Import)
    
    Features:
    - 8 platforms support with validation
    - Platform normalization (lowercase → UPPERCASE)
    - Enhanced summary (platform breakdown)
    - Better metadata tracking
    - Backward compatible with job_worker.py
    - No circular import (uses platform_constants.py + lazy import)
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._rows: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()
        self._threads: Dict[str, threading.Thread] = {}
        self._ttl_seconds: int = 0

    # -------------------------
    # Core lifecycle
    # -------------------------

    def create_job(self, cfg: Optional[Dict[str, Any]] = None) -> str:
        """Create new job with validated config"""
        job_id = uuid.uuid4().hex
        now = _utc_iso_z()
        cfg_norm = _safe_cfg(cfg)

        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "created_at": now,
                "updated_at": now,
                "state": "queued",
                "total_files": 0,
                "processed_files": 0,
                "ok_files": 0,
                "review_files": 0,
                "error_files": 0,
                "cfg": cfg_norm,
                "files": [],
                "_payloads": [],
                "_cancel": False,
                "_started_at": "",
                "_finished_at": "",
                "_last_error": "",
                "_platform_stats": {},
                "_extraction_methods": {},
            }
            self._rows[job_id] = []

        return job_id

    def add_file(
        self,
        job_id: str,
        filename: str,
        content_type: str,
        content: bytes,
        cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add file to job"""
        filename = (filename or "").strip() or "file"
        content_type = (content_type or "").strip() or "application/octet-stream"

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if job.get("state") in {"processing", "done"}:
                return

            if cfg:
                job["cfg"] = _safe_cfg({**job.get("cfg", {}), **cfg})

            job["total_files"] = int(job.get("total_files") or 0) + 1
            job["updated_at"] = _utc_iso_z()
            job["_payloads"].append((filename, content_type, content))
            
            job["files"].append({
                "filename": filename,
                "platform": "unknown",
                "company": "",
                "state": "queued",
                "message": "",
                "rows_count": 0,
            })

    def start_processing(self, job_id: str, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Start background processing thread
        
        ✅ Uses lazy import to avoid circular dependency
        """
        # ✅ Lazy import worker (only when needed, inside function)
        from .job_worker import process_job_files
        
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if cfg:
                job["cfg"] = _safe_cfg(cfg)

            if job["state"] in {"processing", "done", "cancelled"}:
                return

            job["state"] = "processing"
            job["_started_at"] = _utc_iso_z()
            job["updated_at"] = _utc_iso_z()
            job["_cancel"] = False
            job["_last_error"] = ""

            # ✅ Pass process_job_files as parameter
            t = threading.Thread(
                target=self._run_job, 
                args=(job_id, process_job_files), 
                daemon=True
            )
            self._threads[job_id] = t
            t.start()

    def cancel_job(self, job_id: str) -> bool:
        """Cancel job"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            if job["state"] not in {"queued", "processing"}:
                return False
            job["_cancel"] = True
            job["state"] = "cancelled"
            job["updated_at"] = _utc_iso_z()
        return True

    def should_cancel(self, job_id: str) -> bool:
        """Check if job should be cancelled"""
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.get("_cancel"))

    # -------------------------
    # Worker runner
    # -------------------------

    def _run_job(self, job_id: str, process_job_files) -> None:
        """
        Background job processing
        
        ✅ Receives process_job_files as parameter (no import needed)
        """
        try:
            process_job_files(self, job_id)

            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                if job.get("state") != "cancelled":
                    if job.get("state") == "processing":
                        err = int(job.get("error_files") or 0)
                        job["state"] = "done" if err == 0 else "error"

                job["_finished_at"] = _utc_iso_z()
                job["updated_at"] = _utc_iso_z()

        except Exception as e:
            with self._lock:
                job = self._jobs.get(job_id)
                if job:
                    job["state"] = "error"
                    job["_last_error"] = f"{type(e).__name__}: {e}"
                    job["_finished_at"] = _utc_iso_z()
                    job["updated_at"] = _utc_iso_z()

    # -------------------------
    # Helpers for worker
    # -------------------------

    def get_cfg(self, job_id: str) -> Dict[str, Any]:
        """Get job config (for worker)"""
        with self._lock:
            job = self._jobs.get(job_id) or {}
            return dict(job.get("cfg") or _safe_cfg(None))

    # -------------------------
    # Mutations used by worker
    # -------------------------

    def update_job(self, job_id: str, patch: Dict[str, Any]) -> None:
        """Update job fields"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job.get("state") == "cancelled":
                patch = dict(patch)
                patch.pop("state", None)

            job.update(patch)
            job["updated_at"] = _utc_iso_z()

    def update_file(self, job_id: str, index: int, patch: Dict[str, Any]) -> None:
        """Update file metadata"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            files = job.get("files") or []
            if 0 <= index < len(files):
                if "platform" in patch:
                    platform_raw = patch["platform"]
                    platform_normalized = _norm_platform(platform_raw)
                    if platform_normalized:
                        patch["platform"] = platform_normalized
                    else:
                        patch["platform"] = str(platform_raw or "unknown")
                
                files[index].update(patch)
                job["updated_at"] = _utc_iso_z()

    def append_rows(self, job_id: str, rows: List[Dict[str, Any]]) -> None:
        """Append rows to job results"""
        if not rows:
            return

        with self._lock:
            if job_id not in self._rows:
                return

            job = self._jobs.get(job_id)
            if not job:
                return

            platform_stats = job.get("_platform_stats") or {}
            extraction_methods = job.get("_extraction_methods") or {}

            for r in rows:
                self._rows[job_id].append(dict(r))
                
                platform = r.get("_platform") or r.get("U_group") or "UNKNOWN"
                platform_stats[platform] = platform_stats.get(platform, 0) + 1
                
                method = r.get("_extraction_method") or "unknown"
                extraction_methods[method] = extraction_methods.get(method, 0) + 1

            job["_platform_stats"] = platform_stats
            job["_extraction_methods"] = extraction_methods

    def get_payloads(self, job_id: str) -> List[Tuple[str, str, bytes]]:
        """Get file payloads (for worker)"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            return list(job.get("_payloads") or [])

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job snapshot (for worker)"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None

            out = {}
            for k, v in job.items():
                if k.startswith("_"):
                    continue
                out[k] = v

            out["files"] = [dict(x) for x in (out.get("files") or [])]
            out["cfg"] = dict(out.get("cfg") or _safe_cfg(None))
            out["summary"] = self._get_job_summary(job_id)
            
            return out

    # -------------------------
    # Reads (safe snapshots)
    # -------------------------

    def get_rows(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get job rows"""
        with self._lock:
            rows = self._rows.get(job_id)
            if rows is None:
                return None
            return [dict(r) for r in rows]

    # -------------------------
    # Enhanced summary
    # -------------------------

    def _get_job_summary(self, job_id: str) -> Dict[str, Any]:
        """Get enhanced job summary with platform breakdown"""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return {}
            
            rows = self._rows.get(job_id) or []
            
            summary = {
                "total_files": job.get("total_files", 0),
                "processed_files": job.get("processed_files", 0),
                "ok_files": job.get("ok_files", 0),
                "review_files": job.get("review_files", 0),
                "error_files": job.get("error_files", 0),
                "total_rows": len(rows),
                "platforms": dict(job.get("_platform_stats") or {}),
                "extraction_methods": dict(job.get("_extraction_methods") or {}),
                "state": job.get("state", "unknown"),
            }
            
            platform_groups = {}
            for platform, count in summary["platforms"].items():
                group = PLATFORM_GROUPS.get(platform, "Other Expense")
                platform_groups[group] = platform_groups.get(group, 0) + count
            
            summary["platform_groups"] = platform_groups
            
            return summary

    def get_summary(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job summary (public API)"""
        with self._lock:
            if job_id not in self._jobs:
                return None
            return self._get_job_summary(job_id)

    # -------------------------
    # Optional: cleanup utilities
    # -------------------------

    def set_ttl_seconds(self, ttl_seconds: int) -> None:
        """Set TTL for job cleanup"""
        with self._lock:
            self._ttl_seconds = max(0, int(ttl_seconds))

    def cleanup_expired(self) -> int:
        """Cleanup expired jobs"""
        ttl = int(self._ttl_seconds or 0)
        if ttl <= 0:
            return 0

        now = time.time()
        removed = 0

        with self._lock:
            to_delete: List[str] = []
            for job_id, job in self._jobs.items():
                ts_str = job.get("_finished_at") or job.get("updated_at") or job.get("created_at")
                try:
                    dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                    ts = dt.timestamp()
                except Exception:
                    ts = now

                if (now - ts) > ttl:
                    to_delete.append(job_id)

            for job_id in to_delete:
                self._jobs.pop(job_id, None)
                self._rows.pop(job_id, None)
                self._threads.pop(job_id, None)
                removed += 1

        return removed

    # -------------------------
    # Utility methods
    # -------------------------

    def get_valid_platforms(self) -> List[str]:
        """Get list of valid platforms"""
        return sorted(VALID_PLATFORMS)

    def normalize_platform(self, platform: str) -> str:
        """Normalize platform name"""
        return _norm_platform(platform)

    def validate_platforms(self, platforms: List[str]) -> Tuple[List[str], List[str]]:
        """Validate list of platforms"""
        valid = []
        invalid = []
        
        for p in platforms:
            normalized = _norm_platform(p)
            if normalized:
                valid.append(normalized)
            else:
                invalid.append(str(p))
        
        return (valid, invalid)


__all__ = [
    "JobService",
]