# backend/app/services/ocr_service.py
from __future__ import annotations

import os
import io
import logging
from typing import List, Tuple

from PIL import Image

logger = logging.getLogger(__name__)


# -------------------------
# Helpers
# -------------------------
def _is_pdf(path: str) -> bool:
    return str(path).lower().endswith(".pdf")


def _safe_int(env_name: str, default: int) -> int:
    try:
        return int(os.getenv(env_name, str(default)))
    except Exception:
        return default


def _safe_float(env_name: str, default: float) -> float:
    try:
        return float(os.getenv(env_name, str(default)))
    except Exception:
        return default


def _pdf_has_text_fast(pdf_path: str, min_chars: int = 80) -> Tuple[bool, str]:
    """
    ถ้า PDF เป็นตัวอักษรจริง (ไม่ใช่สแกน) จะดึง text ได้เลย (เร็วและฟรี)
    - ใช้ PyMuPDF (fitz)
    """
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(pdf_path)
        texts: List[str] = []
        for i in range(min(doc.page_count, 3)):  # เช็คแค่ 1-3 หน้าแรกพอ
            t = doc.load_page(i).get_text("text") or ""
            texts.append(t)
        joined = "\n".join(texts).strip()
        return (len(joined) >= min_chars, joined)
    except ModuleNotFoundError:
        logger.warning("PyMuPDF (fitz) not installed. Cannot do fast PDF text extraction.")
        return (False, "")
    except Exception as e:
        logger.warning("pdf_has_text_fast failed: %s", e)
        return (False, "")


def _render_pdf_to_images(pdf_path: str, max_pages: int = 20, zoom: float = 2.0) -> List[Image.Image]:
    """
    Render PDF -> PIL images ด้วย PyMuPDF (ไม่ต้องติด poppler)
    """
    try:
        import fitz  # PyMuPDF
    except ModuleNotFoundError as e:
        raise RuntimeError("Missing dependency: PyMuPDF. Install with: pip install pymupdf") from e

    doc = fitz.open(pdf_path)
    imgs: List[Image.Image] = []

    n = min(doc.page_count, max_pages)
    mat = fitz.Matrix(zoom, zoom)

    for i in range(n):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        imgs.append(img)

    return imgs


# -------------------------
# OCR Service
# -------------------------
class OCRService:
    """
    OCR pipeline:
    - PDF with text layer: extract via PyMuPDF (fast)
    - scanned PDF: render to images -> OCR via PaddleOCR
    - image: OCR via PaddleOCR
    """

    def __init__(self):
        self.enable: bool = os.getenv("ENABLE_OCR", "1") == "1"
        self.provider: str = os.getenv("OCR_PROVIDER", "paddle").strip().lower()  # paddle|document_ai|none

        self.max_pages: int = _safe_int("OCR_MAX_PAGES", 20)
        self.zoom: float = _safe_float("OCR_PDF_ZOOM", 2.0)
        self.min_chars: int = _safe_int("OCR_MIN_TEXT_CHARS", 80)

        self._paddle = None
        self._paddle_ready = False

        if self.enable and self.provider == "paddle":
            self._init_paddle()

    def _init_paddle(self) -> None:
        """
        Initialize PaddleOCR safely.
        """
        try:
            from paddleocr import PaddleOCR

            self._paddle = PaddleOCR(
                use_angle_cls=True,
                lang=os.getenv("PADDLE_OCR_LANG", "en"),
                show_log=False,
            )
            self._paddle_ready = True
        except ModuleNotFoundError:
            logger.warning("paddleocr not installed. OCR will be disabled for images/scanned PDFs.")
            self._paddle_ready = False
        except Exception as e:
            logger.warning("Failed to init PaddleOCR: %s", e)
            self._paddle_ready = False

    def ocr_file(self, file_path: str) -> str:
        """
        คืน text OCR ทั้งไฟล์ (PDF/รูป)
        """
        if not self.enable or self.provider in ("none", "off", "0"):
            return ""

        if self.provider == "document_ai":
            return self._ocr_document_ai(file_path)

        # default: paddle
        if _is_pdf(file_path):
            # 1) Try fast text extraction (no OCR) if PDF has text layer
            has_text, text = _pdf_has_text_fast(file_path, min_chars=self.min_chars)
            if has_text:
                return text

            # 2) scanned PDF -> render -> OCR
            if not self._paddle_ready:
                return ""

            try:
                pages = _render_pdf_to_images(file_path, max_pages=self.max_pages, zoom=self.zoom)
            except Exception as e:
                logger.warning("render_pdf_to_images failed: %s", e)
                return ""

            return self._ocr_images_with_paddle(pages)

        # image
        if not self._paddle_ready:
            return ""

        try:
            img = Image.open(file_path).convert("RGB")
        except Exception as e:
            logger.warning("open image failed: %s", e)
            return ""

        return self._ocr_images_with_paddle([img])

    def _ocr_images_with_paddle(self, images: List[Image.Image]) -> str:
        if not self._paddle_ready or not self._paddle:
            return ""

        try:
            import numpy as np
        except ModuleNotFoundError:
            logger.warning("numpy not installed. Cannot run PaddleOCR.")
            return ""

        chunks: List[str] = []

        for img in images:
            arr = np.array(img)
            result = self._paddle.ocr(arr, cls=True) or []

            for line in result:
                for item in line:
                    try:
                        text_score = item[1]
                        if isinstance(text_score, (list, tuple)) and len(text_score) >= 1:
                            txt = text_score[0]
                            if txt:
                                chunks.append(str(txt))
                    except Exception:
                        continue

        return "\n".join(chunks).strip()

    def _ocr_document_ai(self, file_path: str) -> str:
        raise NotImplementedError("document_ai not enabled in this build yet")


# -------------------------
# Compatibility helper
# -------------------------
def maybe_ocr_to_text(file_path: str) -> str:
    """
    ฟังก์ชันนี้มีไว้เพื่อให้ job_worker.py import ได้
    - จะพยายามดึง text จาก PDF ที่มี text layer ก่อน
    - ถ้าเป็นสแกน/รูป จะใช้ OCR (paddle) ถ้าเปิด ENABLE_OCR=1
    """
    try:
        return OCRService().ocr_file(file_path)
    except Exception as e:
        logger.warning("maybe_ocr_to_text failed: %s", e)
        return ""
