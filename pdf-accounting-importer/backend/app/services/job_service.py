# backend/app/services/job_service.py
from __future__ import annotations

import uuid
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from .job_worker import process_job_files


def _utc_iso_z(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_token(s: Any) -> str:
    return str(s or "").strip().upper()


def _norm_list(xs: Any) -> List[str]:
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


def _safe_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize cfg to stable shape.
    Expect keys:
      - client_tags: ["SHD","RABBIT","TOPONE",...]
      - client_tax_ids: ["0105...","..."]
      - platforms: ["SHOPEE","SPX",...]
    Empty list = allow all
    """
    cfg = cfg or {}
    return {
        "client_tags": _norm_list(cfg.get("client_tags")),
        "client_tax_ids": [str(x).strip() for x in (cfg.get("client_tax_ids") or []) if str(x).strip()],
        "platforms": _norm_list(cfg.get("platforms")),
    }


class JobService:
    """
    In-memory job service (simple + robust) for:
    - upload files (bytes)
    - async processing via job_worker.process_job_files(self, job_id)
    - polling job status + rows (for frontend)

    Added:
    ✅ store cfg filters at job-level
    ✅ helper methods for worker to route file to NEEDS_REVIEW if cfg mismatch
    ✅ append_rows can stamp _status so frontend review works
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
        job_id = uuid.uuid4().hex
        now = _utc_iso_z()

        cfg_norm = _safe_cfg(cfg)

        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "created_at": now,
                "updated_at": now,
                "state": "queued",  # queued|processing|done|error|cancelled

                "total_files": 0,
                "processed_files": 0,
                "ok_files": 0,
                "review_files": 0,
                "error_files": 0,

                # ✅ job-level filter config (frontend-safe)
                "cfg": cfg_norm,

                # frontend-facing
                "files": [],

                # internal: (filename, content_type, bytes, meta)
                # meta may include hints from filename prefilter or later enrichment
                "_payloads": [],

                "_cancel": False,
                "_started_at": "",
                "_finished_at": "",
                "_last_error": "",
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
        """
        Add file payload for a job.
        - cfg optional (if passed, stored into payload meta snapshot)
        """
        filename = (filename or "").strip() or "file"
        content_type = (content_type or "").strip() or "application/octet-stream"

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            if job.get("state") in {"processing", "done"}:
                return

            job["total_files"] = int(job.get("total_files") or 0) + 1
            job["updated_at"] = _utc_iso_z()

            # ✅ store payload meta (can include upload cfg snapshot)
            meta = {
                "cfg": _safe_cfg(cfg) if cfg else job.get("cfg") or _safe_cfg(None),
                # later worker can fill: inferred_company/platform/client_tax_id/seller_id...
                "inferred_company": "",
                "inferred_platform": "",
                "inferred_client_tax_id": "",
            }

            job["_payloads"].append((filename, content_type, content, meta))
            job["files"].append(
                {
                    "filename": filename,
                    "platform": "unknown",
                    "company": "",              # ✅ UI can show company
                    "client_tax_id": "",        # optional
                    "state": "queued",          # queued|processing|done|needs_review|error
                    "message": "",
                    "rows_count": 0,
                }
            )

    def start_processing(self, job_id: str, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Start background processing thread.
        - cfg optional: if passed, overrides job.cfg (useful when main.py passes cfg)
        """
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

            t = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
            self._threads[job_id] = t
            t.start()

    def cancel_job(self, job_id: str) -> bool:
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
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.get("_cancel"))

    # -------------------------
    # Worker runner
    # -------------------------

    def _run_job(self, job_id: str) -> None:
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
    # ✅ Filter helpers (worker should call)
    # -------------------------

    def get_cfg(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id) or {}
            return dict(job.get("cfg") or _safe_cfg(None))

    def should_review_by_cfg(
        self,
        job_id: str,
        *,
        company_tag: str = "",
        client_tax_id: str = "",
        platform: str = "",
    ) -> bool:
        """
        Decide if file should go to NEEDS_REVIEW based on job.cfg.
        Rules:
          - if cfg lists empty => allow all (return False)
          - if cfg has values and file doesn't match => review (return True)
        """
        cfg = self.get_cfg(job_id)

        allowed_tags = _norm_list(cfg.get("client_tags"))
        allowed_plats = _norm_list(cfg.get("platforms"))
        allowed_taxs = [str(x).strip() for x in (cfg.get("client_tax_ids") or []) if str(x).strip()]

        tag = _norm_token(company_tag)
        plat = _norm_token(platform)
        tax = str(client_tax_id or "").strip()

        # If user selected specific tags and file doesn't match -> review
        if allowed_tags and (not tag or tag not in allowed_tags):
            return True

        # If user selected specific platforms and file doesn't match -> review
        if allowed_plats and (not plat or plat not in allowed_plats):
            return True

        # If user selected specific client tax ids and file doesn't match -> review
        if allowed_taxs and (not tax or tax not in allowed_taxs):
            return True

        return False

    def begin_file(
        self,
        job_id: str,
        index: int,
        *,
        platform: str = "",
        company: str = "",
        client_tax_id: str = "",
    ) -> None:
        """
        Worker calls when starting to process a file.
        """
        self.update_file(
            job_id,
            index,
            {
                "state": "processing",
                "platform": platform or "unknown",
                "company": company or "",
                "client_tax_id": client_tax_id or "",
                "message": "",
            },
        )

    def finish_file(
        self,
        job_id: str,
        index: int,
        *,
        state: str,
        message: str = "",
        rows_count: int = 0,
    ) -> None:
        """
        Worker calls when finalizing a file. Also updates counters safely.
        state: done|needs_review|error
        """
        state = (state or "").strip().lower()
        if state not in {"done", "needs_review", "error"}:
            state = "done"

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return

            files = job.get("files") or []
            if not (0 <= index < len(files)):
                return

            prev_state = (files[index].get("state") or "").strip().lower()

            # update file
            files[index].update(
                {
                    "state": state,
                    "message": message or "",
                    "rows_count": int(rows_count or 0),
                }
            )

            # update job counters (adjust if re-writing state)
            def dec_counter(st: str) -> None:
                if st == "done":
                    job["ok_files"] = max(0, int(job.get("ok_files") or 0) - 1)
                elif st == "needs_review":
                    job["review_files"] = max(0, int(job.get("review_files") or 0) - 1)
                elif st == "error":
                    job["error_files"] = max(0, int(job.get("error_files") or 0) - 1)

            def inc_counter(st: str) -> None:
                if st == "done":
                    job["ok_files"] = int(job.get("ok_files") or 0) + 1
                elif st == "needs_review":
                    job["review_files"] = int(job.get("review_files") or 0) + 1
                elif st == "error":
                    job["error_files"] = int(job.get("error_files") or 0) + 1

            if prev_state in {"done", "needs_review", "error"}:
                dec_counter(prev_state)
            inc_counter(state)

            # processed_files: count files finished (done/review/error)
            # recompute safely (not heavy)
            finished = 0
            for f in files:
                st = (f.get("state") or "").strip().lower()
                if st in {"done", "needs_review", "error"}:
                    finished += 1
            job["processed_files"] = finished

            job["updated_at"] = _utc_iso_z()

    # -------------------------
    # Mutations used by worker
    # -------------------------

    def update_job(self, job_id: str, patch: Dict[str, Any]) -> None:
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
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            files = job.get("files") or []
            if 0 <= index < len(files):
                files[index].update(patch)
                job["updated_at"] = _utc_iso_z()

    def append_rows(self, job_id: str, rows: List[Dict[str, Any]], status: Optional[str] = None) -> None:
        """
        Append extracted rows to the job.
        - stamps _status for frontend filter (OK/NEEDS_REVIEW/ERROR)
        """
        if not rows:
            return

        st = (status or "").strip().upper()
        if st not in {"OK", "NEEDS_REVIEW", "ERROR"}:
            st = ""  # do not override if unknown

        with self._lock:
            if job_id not in self._rows:
                return

            for r in rows:
                rr = dict(r)
                if st and not rr.get("_status"):
                    rr["_status"] = st
                self._rows[job_id].append(rr)

    def get_payloads(self, job_id: str) -> List[Tuple[str, str, bytes, Dict[str, Any]]]:
        """
        Internal: worker reads raw payloads.
        Returns a shallow copy list so iteration is safe.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            return list(job.get("_payloads") or [])

    # -------------------------
    # Reads (safe snapshots)
    # -------------------------

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None

            out = {k: v for k, v in job.items() if k != "_payloads"}
            out["files"] = [dict(x) for x in (out.get("files") or [])]
            out["cfg"] = dict(out.get("cfg") or _safe_cfg(None))
            return out

    def get_rows(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        with self._lock:
            rows = self._rows.get(job_id)
            if rows is None:
                return None
            return [dict(r) for r in rows]

    # -------------------------
    # Optional: cleanup utilities
    # -------------------------

    def set_ttl_seconds(self, ttl_seconds: int) -> None:
        with self._lock:
            self._ttl_seconds = max(0, int(ttl_seconds))

    def cleanup_expired(self) -> int:
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
