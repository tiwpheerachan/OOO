from __future__ import annotations

import uuid
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

from .job_worker import process_job_files

class JobService:
    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._rows: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def create_job(self) -> str:
        job_id = uuid.uuid4().hex
        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "state": "queued",
                "total_files": 0,
                "processed_files": 0,
                "ok_files": 0,
                "review_files": 0,
                "error_files": 0,
                "files": [],
                "_payloads": [],  # internal: (filename, content_type, bytes)
            }
            self._rows[job_id] = []
        return job_id

    def add_file(self, job_id: str, filename: str, content_type: str, content: bytes) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["total_files"] += 1
            job["_payloads"].append((filename, content_type, content))
            job["files"].append({
                "filename": filename,
                "platform": "unknown",
                "state": "queued",
                "message": "",
                "rows_count": 0,
            })

    def start_processing(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job["state"] in ("processing","done"):
                return
            job["state"] = "processing"

        t = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
        t.start()

    def _run_job(self, job_id: str) -> None:
        process_job_files(self, job_id)

    def update_job(self, job_id: str, patch: Dict[str, Any]) -> None:
        with self._lock:
            if job_id not in self._jobs:
                return
            self._jobs[job_id].update(patch)

    def update_file(self, job_id: str, index: int, patch: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if 0 <= index < len(job["files"]):
                job["files"][index].update(patch)

    def append_rows(self, job_id: str, rows: List[Dict[str, Any]]) -> None:
        with self._lock:
            if job_id not in self._rows:
                return
            self._rows[job_id].extend(rows)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            # hide internal payloads
            out = {k:v for k,v in job.items() if k != "_payloads"}
            return out

    def get_rows(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        with self._lock:
            return self._rows.get(job_id)

    def get_payloads(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return []
            return list(job.get("_payloads", []))
