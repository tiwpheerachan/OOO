# backend/app/services/ocr_service.py
"""
OCR Service - Enhanced + Safer Version (Platform-Aware + 2-pass OCR)

เป้าหมายของไฟล์นี้:
- “อ่านข้อความ” ให้ดีที่สุด (PDF text layer / scanned PDF / images)
- ลดความมั่วของ META/GOOGLE/Marketplace ด้วย preprocessing ที่ต่างกัน
- ไม่แตะ logic ทางบัญชี: date/WHT/unit_price/ref/description/account -> ไปล็อกใน job_worker/extractors เท่านั้น

✅ What’s new:
1) ✅ Better platform hint:
   - from filename (fast)
   - optional from extracted text (light heuristic) to reduce META/GOOGLE confusion
2) ✅ Adaptive PDF rendering + preprocessing presets by platform
3) ✅ 2-pass OCR (only when needed): try alternative preset if confidence/text too low
4) ✅ More stable Paddle parsing + confidence scoring
5) ✅ Return richer warnings (caller can decide to review / call AI)
6) ✅ Memory-safe guards (max pages, downscale, zoom)
"""

from __future__ import annotations

import io
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any

from PIL import Image, ImageEnhance, ImageFilter

logger = logging.getLogger(__name__)

# ✅ Import platform constants (optional)
try:
    from .platform_constants import VALID_PLATFORMS
except Exception:
    VALID_PLATFORMS = {"META", "GOOGLE", "SHOPEE", "LAZADA", "TIKTOK", "SPX", "THAI_TAX", "UNKNOWN"}


# ============================================================
# File type helpers
# ============================================================
def _is_pdf(path: str) -> bool:
    return str(path).lower().endswith(".pdf")


def _is_image(path: str) -> bool:
    p = str(path).lower()
    return p.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"))


def _safe_int(env_name: str, default: int) -> int:
    try:
        return int(str(os.getenv(env_name, str(default))).strip())
    except Exception:
        return default


def _safe_float(env_name: str, default: float) -> float:
    try:
        return float(str(os.getenv(env_name, str(default))).strip())
    except Exception:
        return default


def _safe_str(env_name: str, default: str) -> str:
    try:
        v = os.getenv(env_name, default)
        return str(v).strip()
    except Exception:
        return default


def _env_bool(env_name: str, default: bool = False) -> bool:
    v = os.getenv(env_name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


# ============================================================
# Platform detection
# ============================================================
def _detect_platform_from_filename(filename: str) -> str:
    """
    Detect platform from filename (for OCR optimization)
    """
    fn = (filename or "").upper()

    # Ads (high priority)
    if "RCMETA" in fn or "META" in fn or "FACEBOOK" in fn:
        return "META"
    if "GOOGLE" in fn or "GOOG" in fn:
        return "GOOGLE"

    # Thai tax
    if "TAX" in fn or "INVOICE" in fn or "ใบกำกับ" in fn:
        return "THAI_TAX"

    # Logistics / marketplace
    if "SPX" in fn or "RCSPX" in fn or "SHOPEE EXPRESS" in fn or "SHOPEE-EXPRESS" in fn:
        return "SPX"
    if "SHOPEE" in fn:
        return "SHOPEE"
    if "LAZADA" in fn or "LAZ" in fn:
        return "LAZADA"
    if "TIKTOK" in fn or "TTS" in fn:
        return "TIKTOK"

    return "UNKNOWN"


# Light heuristic on text (used only when filename is UNKNOWN or ambiguous)
_RX_GOOGLE = re.compile(r"\b(google ads|adwords|gclid|google payments|google\s+ireland|google asia pacific)\b", re.I)
_RX_META = re.compile(r"\b(meta|facebook|fb\.com|facebook ads|meta business)\b", re.I)
_RX_SHOPEE = re.compile(r"\b(shopee|shopee thailand|seller id|merchant id)\b", re.I)
_RX_LAZADA = re.compile(r"\b(lazada|alibaba|lzd)\b", re.I)
_RX_TIKTOK = re.compile(r"\b(tiktok|tts|tiktok shop)\b", re.I)
_RX_TAX = re.compile(r"\b(ใบกำกับภาษี|เลขประจำตัวผู้เสียภาษี|tax id)\b", re.I)


def _refine_platform_from_text(text: str, current_hint: str) -> str:
    """
    Refine platform hint by lightweight keyword checks.
    This does NOT change any accounting fields; only affects OCR preset selection.

    Policy:
    - If current_hint already confident (META/GOOGLE/THAI_TAX) keep it unless strongly contradicted.
    - If UNKNOWN, try infer.
    """
    t = (text or "").strip()
    if not t:
        return current_hint or "UNKNOWN"

    cur = (current_hint or "UNKNOWN").upper()

    score: Dict[str, int] = {"META": 0, "GOOGLE": 0, "SHOPEE": 0, "LAZADA": 0, "TIKTOK": 0, "THAI_TAX": 0}
    if _RX_GOOGLE.search(t): score["GOOGLE"] += 3
    if _RX_META.search(t): score["META"] += 3
    if _RX_SHOPEE.search(t): score["SHOPEE"] += 2
    if _RX_LAZADA.search(t): score["LAZADA"] += 2
    if _RX_TIKTOK.search(t): score["TIKTOK"] += 2
    if _RX_TAX.search(t): score["THAI_TAX"] += 2

    best = max(score.items(), key=lambda kv: kv[1])
    best_platform, best_score = best[0], best[1]

    if best_score <= 0:
        return cur

    # if cur is UNKNOWN, accept best
    if cur == "UNKNOWN":
        return best_platform

    # keep strong hints unless best is clearly stronger
    if cur in {"META", "GOOGLE", "THAI_TAX"}:
        if best_score >= 4 and best_platform != cur:
            return best_platform
        return cur

    # otherwise allow refinement
    if best_platform != cur and best_score >= 3:
        return best_platform

    return cur


# ============================================================
# Image helpers
# ============================================================
def _open_image_safe(file_path: str) -> Optional[Image.Image]:
    try:
        img = Image.open(file_path)
        img.load()
        return img.convert("RGB")
    except Exception as e:
        logger.warning("open image failed: %s", e)
        return None


def _resize_max_side(img: Image.Image, max_side: int) -> Image.Image:
    if max_side <= 0:
        return img
    w, h = img.size
    m = max(w, h)
    if m <= max_side:
        return img
    ratio = max_side / float(m)
    nw = max(1, int(w * ratio))
    nh = max(1, int(h * ratio))
    try:
        return img.resize((nw, nh), resample=Image.LANCZOS)
    except Exception:
        return img.resize((nw, nh), resample=Image.BILINEAR)


def _preprocess_preset(
    img: Image.Image,
    *,
    platform_hint: str,
    preset: str,
    max_side: int,
    grayscale: bool,
) -> Image.Image:
    """
    Presets:
      - "default" : mild
      - "ads_sharp" : sharpen + contrast (META/GOOGLE)
      - "market_denoise" : median + mild contrast (marketplace phone photos)
      - "thai_tax" : sharpen + slight contrast, keep edges
    """
    if img.mode != "RGB":
        img = img.convert("RGB")
    img = _resize_max_side(img, max_side=max_side)

    ph = (platform_hint or "UNKNOWN").upper()

    # Base knobs
    contrast = 1.0
    sharpen = False
    denoise = False

    if preset == "ads_sharp":
        sharpen = True
        contrast = 1.25
    elif preset == "market_denoise":
        denoise = True
        contrast = 1.10
    elif preset == "thai_tax":
        sharpen = True
        contrast = 1.12
    else:
        # default
        if ph in {"META", "GOOGLE"}:
            contrast = 1.15
        elif ph in {"SHOPEE", "LAZADA", "TIKTOK"}:
            contrast = 1.05
        elif ph == "THAI_TAX":
            contrast = 1.08

    # Apply contrast
    if contrast != 1.0:
        try:
            img = ImageEnhance.Contrast(img).enhance(contrast)
        except Exception:
            pass

    # Denoise (median)
    if denoise:
        try:
            img = img.filter(ImageFilter.MedianFilter(size=3))
        except Exception:
            pass

    # Sharpen
    if sharpen:
        try:
            img = img.filter(ImageFilter.SHARPEN)
        except Exception:
            pass

    # Grayscale
    if grayscale:
        try:
            img = img.convert("L").convert("RGB")
        except Exception:
            pass

    return img


# ============================================================
# PDF helpers
# ============================================================
def _pdf_has_text_fast(pdf_path: str, min_chars: int = 120, max_pages: int = 3) -> Tuple[bool, str]:
    """
    Check if PDF has text layer (not scanned)
    Uses PyMuPDF (fitz) for fast text extraction
    """
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(pdf_path)
        texts: List[str] = []
        n = min(doc.page_count, max(1, max_pages))
        for i in range(n):
            try:
                t = doc.load_page(i).get_text("text") or ""
            except Exception:
                t = ""
            t = (t or "").strip()
            if t:
                texts.append(t)

        joined = "\n\n".join(texts).strip()
        return (len(joined) >= min_chars, joined)

    except ModuleNotFoundError:
        logger.info("PyMuPDF not installed; fast PDF text extraction unavailable.")
        return (False, "")

    except Exception as e:
        logger.warning("pdf_has_text_fast failed: %s", e)
        return (False, "")


def _render_pdf_to_images(pdf_path: str, max_pages: int = 20, zoom: float = 2.0) -> List[Image.Image]:
    """
    Render PDF pages to PIL images using PyMuPDF
    """
    try:
        import fitz  # PyMuPDF
    except ModuleNotFoundError as e:
        raise RuntimeError("Missing dependency: PyMuPDF. Install with: pip install pymupdf") from e

    doc = fitz.open(pdf_path)
    imgs: List[Image.Image] = []

    n = min(doc.page_count, max(1, max_pages))
    mat = fitz.Matrix(zoom, zoom)

    for i in range(n):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        imgs.append(img)

    return imgs


# ============================================================
# Enhanced OCR Result
# ============================================================
@dataclass
class OCRResult:
    text: str
    method: str  # "pdf_text" | "ocr_paddle_pdf" | "ocr_paddle_img" | "document_ai" | "none"
    pages: int = 0
    warnings: List[str] = field(default_factory=list)

    platform_hint: str = "UNKNOWN"
    processing_time_ms: float = 0.0
    confidence_avg: float = 0.0
    text_length: int = 0
    used_preset: str = "default"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text or "",
            "method": self.method,
            "pages": int(self.pages or 0),
            "warnings": self.warnings or [],
            "platform_hint": self.platform_hint,
            "processing_time_ms": round(self.processing_time_ms, 2),
            "confidence_avg": round(self.confidence_avg, 3),
            "text_length": int(self.text_length or 0),
            "used_preset": self.used_preset,
        }

    def __post_init__(self):
        self.text_length = len(self.text or "")


# ============================================================
# OCR Service
# ============================================================
class OCRService:
    """
    Pipeline:
      1) PDF text layer -> extract via PyMuPDF (fast)
      2) scanned PDF / image -> OCR if ENABLE_OCR=1 and provider != none
         - default paddle
      3) 2-pass OCR fallback when low confidence / too short text
    """

    def __init__(self):
        self.enable_ocr: bool = _env_bool("ENABLE_OCR", default=False)
        self.provider: str = _safe_str("OCR_PROVIDER", "none").lower()

        self.min_chars: int = _safe_int("OCR_MIN_TEXT_CHARS", 120)
        self.fast_check_pages: int = _safe_int("OCR_FAST_CHECK_PAGES", 3)

        # Defaults, can be overridden by platform presets
        self.max_pages: int = _safe_int("OCR_MAX_PAGES", 20)
        self.zoom: float = _safe_float("OCR_PDF_ZOOM", 2.0)

        # Image preprocessing
        self.max_side: int = _safe_int("OCR_IMG_MAX_SIDE", 2200)
        self.gray: bool = _env_bool("OCR_IMG_GRAYSCALE", default=False)

        # Platform-aware behavior
        self.platform_aware: bool = _env_bool("OCR_PLATFORM_AWARE", default=True)
        self.refine_platform_by_text: bool = _env_bool("OCR_REFINE_PLATFORM_BY_TEXT", default=True)

        # 2-pass OCR
        self.enable_two_pass: bool = _env_bool("OCR_TWO_PASS", default=True)
        self.two_pass_min_conf: float = _safe_float("OCR_TWO_PASS_MIN_CONF", 0.62)
        self.two_pass_min_len: int = _safe_int("OCR_TWO_PASS_MIN_LEN", 180)

        # Paddle settings
        self.paddle_lang: str = _safe_str("PADDLE_OCR_LANG", "en")
        self._paddle = None
        self._paddle_ready = False

        # Stats
        self._stats: Dict[str, Any] = {
            "total_calls": 0,
            "pdf_text_extractions": 0,
            "ocr_calls": 0,
            "two_pass_used": 0,
            "total_time_ms": 0.0,
            "by_method": {},
            "by_platform": {},
        }

        if self.enable_ocr and self.provider == "paddle":
            self._init_paddle()

    def _init_paddle(self) -> None:
        if self._paddle_ready:
            return
        try:
            from paddleocr import PaddleOCR  # type: ignore

            self._paddle = PaddleOCR(
                use_angle_cls=True,
                lang=self.paddle_lang,
                show_log=False,
            )
            self._paddle_ready = True
            logger.info("PaddleOCR initialized (lang=%s)", self.paddle_lang)
        except ModuleNotFoundError:
            logger.warning("paddleocr not installed. OCR disabled.")
            self._paddle_ready = False
        except Exception as e:
            logger.warning("Failed to init PaddleOCR: %s", e)
            self._paddle_ready = False

    def _ensure_provider_ready(self) -> None:
        if not self.enable_ocr:
            return
        if self.provider == "paddle":
            self._init_paddle()

    # -------------------------
    # Public API
    # -------------------------
    def extract_text(self, file_path: str, platform_hint: str = "UNKNOWN") -> str:
        return self.extract_text_with_meta(file_path, platform_hint=platform_hint).text

    def extract_text_with_meta(self, file_path: str, platform_hint: str = "UNKNOWN") -> OCRResult:
        start_time = time.time()
        self._stats["total_calls"] += 1

        if not file_path:
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["empty_path"],
                platform_hint=platform_hint,
            )

        # auto platform detect from filename if unknown
        if platform_hint == "UNKNOWN":
            platform_hint = _detect_platform_from_filename(file_path)

        if platform_hint not in VALID_PLATFORMS:
            platform_hint = "UNKNOWN"

        # 1) PDF text layer
        if _is_pdf(file_path):
            has_text, text = _pdf_has_text_fast(
                file_path,
                min_chars=self.min_chars,
                max_pages=self.fast_check_pages,
            )
            if has_text:
                # optional refine platform based on text keywords
                if self.refine_platform_by_text:
                    platform_hint = _refine_platform_from_text(text, platform_hint)

                elapsed_ms = (time.time() - start_time) * 1000
                self._stats["pdf_text_extractions"] += 1
                self._update_stats("pdf_text", platform_hint, elapsed_ms)

                return OCRResult(
                    text=text,
                    method="pdf_text",
                    pages=min(self.fast_check_pages, 3),
                    warnings=[],
                    platform_hint=platform_hint,
                    processing_time_ms=elapsed_ms,
                    confidence_avg=1.0,
                    used_preset="pdf_text",
                )

            # scanned PDF -> OCR
            result = self._ocr_scanned_pdf(file_path, platform_hint=platform_hint)
            elapsed_ms = (time.time() - start_time) * 1000
            result.processing_time_ms = elapsed_ms
            self._update_stats(result.method, platform_hint, elapsed_ms)
            return result

        # 2) Image -> OCR
        if _is_image(file_path):
            result = self._ocr_image(file_path, platform_hint=platform_hint)
            elapsed_ms = (time.time() - start_time) * 1000
            result.processing_time_ms = elapsed_ms
            self._update_stats(result.method, platform_hint, elapsed_ms)
            return result

        return OCRResult(
            text="",
            method="none",
            pages=0,
            warnings=["unsupported_file_type"],
            platform_hint=platform_hint,
        )

    # -------------------------
    # Platform preset decision
    # -------------------------
    def _pdf_zoom_for_platform(self, platform_hint: str) -> float:
        ph = (platform_hint or "UNKNOWN").upper()
        # Ads often screenshots -> higher zoom helps small text
        if ph in {"META", "GOOGLE"}:
            return max(self.zoom, 2.4)
        # Thai tax often needs crisp text
        if ph == "THAI_TAX":
            return max(self.zoom, 2.2)
        return self.zoom

    def _preset_primary(self, platform_hint: str) -> str:
        ph = (platform_hint or "UNKNOWN").upper()
        if ph in {"META", "GOOGLE"}:
            return "ads_sharp"
        if ph in {"SHOPEE", "LAZADA", "TIKTOK", "SPX"}:
            return "market_denoise"
        if ph == "THAI_TAX":
            return "thai_tax"
        return "default"

    def _preset_secondary(self, platform_hint: str, primary: str) -> str:
        # second attempt tries a different style
        if primary == "market_denoise":
            return "ads_sharp"
        if primary == "ads_sharp":
            return "market_denoise"
        if primary == "thai_tax":
            return "ads_sharp"
        return "ads_sharp"

    # -------------------------
    # OCR implementations
    # -------------------------
    def _ocr_scanned_pdf(self, pdf_path: str, platform_hint: str = "UNKNOWN") -> OCRResult:
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["ocr_disabled"],
                platform_hint=platform_hint,
            )

        if self.provider == "document_ai":
            text = self._ocr_document_ai(pdf_path)
            return OCRResult(
                text=text,
                method="document_ai",
                pages=0,
                warnings=[],
                platform_hint=platform_hint,
                confidence_avg=0.0,
                used_preset="document_ai",
            )

        self._ensure_provider_ready()
        if not self._paddle_ready:
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["paddle_not_ready"],
                platform_hint=platform_hint,
            )

        # render
        zoom = self._pdf_zoom_for_platform(platform_hint) if self.platform_aware else self.zoom
        try:
            pages = _render_pdf_to_images(pdf_path, max_pages=self.max_pages, zoom=zoom)
        except Exception as e:
            logger.warning("render_pdf_to_images failed: %s", e)
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["pdf_render_failed"],
                platform_hint=platform_hint,
            )

        primary = self._preset_primary(platform_hint) if self.platform_aware else "default"
        imgs1 = [
            _preprocess_preset(p, platform_hint=platform_hint, preset=primary, max_side=self.max_side, grayscale=self.gray)
            for p in pages
        ]
        text1, conf1 = self._ocr_images_with_paddle(imgs1)
        self._stats["ocr_calls"] += 1

        # refine platform by OCR text if requested (helps meta/google mismatch)
        refined = platform_hint
        if self.refine_platform_by_text:
            refined = _refine_platform_from_text(text1, platform_hint)
            platform_hint = refined

        warnings: List[str] = []
        if not text1:
            warnings.append("ocr_empty")
        if conf1 > 0 and conf1 < 0.45:
            warnings.append("low_confidence")

        # 2-pass fallback
        if self.enable_two_pass and (len(text1) < self.two_pass_min_len or (0 < conf1 < self.two_pass_min_conf)):
            secondary = self._preset_secondary(platform_hint, primary)
            imgs2 = [
                _preprocess_preset(p, platform_hint=platform_hint, preset=secondary, max_side=self.max_side, grayscale=self.gray)
                for p in pages
            ]
            text2, conf2 = self._ocr_images_with_paddle(imgs2)
            self._stats["ocr_calls"] += 1
            self._stats["two_pass_used"] += 1

            # choose better result
            pick_text, pick_conf, pick_preset = text1, conf1, primary
            if (len(text2) > len(text1) + 40) or (conf2 > conf1 + 0.08):
                pick_text, pick_conf, pick_preset = text2, conf2, secondary

            return OCRResult(
                text=pick_text,
                method="ocr_paddle_pdf",
                pages=len(pages),
                warnings=warnings + ["two_pass_attempted"],
                platform_hint=platform_hint,
                confidence_avg=pick_conf,
                used_preset=pick_preset,
            )

        return OCRResult(
            text=text1,
            method="ocr_paddle_pdf",
            pages=len(pages),
            warnings=warnings,
            platform_hint=platform_hint,
            confidence_avg=conf1,
            used_preset=primary,
        )

    def _ocr_image(self, file_path: str, platform_hint: str = "UNKNOWN") -> OCRResult:
        if (not self.enable_ocr) or (self.provider in ("none", "off", "0")):
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["ocr_disabled"],
                platform_hint=platform_hint,
            )

        if self.provider == "document_ai":
            text = self._ocr_document_ai(file_path)
            return OCRResult(
                text=text,
                method="document_ai",
                pages=0,
                warnings=[],
                platform_hint=platform_hint,
                confidence_avg=0.0,
                used_preset="document_ai",
            )

        self._ensure_provider_ready()
        if not self._paddle_ready:
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["paddle_not_ready"],
                platform_hint=platform_hint,
            )

        img = _open_image_safe(file_path)
        if img is None:
            return OCRResult(
                text="",
                method="none",
                pages=0,
                warnings=["image_open_failed"],
                platform_hint=platform_hint,
            )

        primary = self._preset_primary(platform_hint) if self.platform_aware else "default"
        img1 = _preprocess_preset(img, platform_hint=platform_hint, preset=primary, max_side=self.max_side, grayscale=self.gray)
        text1, conf1 = self._ocr_images_with_paddle([img1])
        self._stats["ocr_calls"] += 1

        if self.refine_platform_by_text:
            platform_hint = _refine_platform_from_text(text1, platform_hint)

        warnings: List[str] = []
        if not text1:
            warnings.append("ocr_empty")
        if conf1 > 0 and conf1 < 0.45:
            warnings.append("low_confidence")

        if self.enable_two_pass and (len(text1) < self.two_pass_min_len or (0 < conf1 < self.two_pass_min_conf)):
            secondary = self._preset_secondary(platform_hint, primary)
            img2 = _preprocess_preset(img, platform_hint=platform_hint, preset=secondary, max_side=self.max_side, grayscale=self.gray)
            text2, conf2 = self._ocr_images_with_paddle([img2])
            self._stats["ocr_calls"] += 1
            self._stats["two_pass_used"] += 1

            pick_text, pick_conf, pick_preset = text1, conf1, primary
            if (len(text2) > len(text1) + 40) or (conf2 > conf1 + 0.08):
                pick_text, pick_conf, pick_preset = text2, conf2, secondary

            return OCRResult(
                text=pick_text,
                method="ocr_paddle_img",
                pages=1,
                warnings=warnings + ["two_pass_attempted"],
                platform_hint=platform_hint,
                confidence_avg=pick_conf,
                used_preset=pick_preset,
            )

        return OCRResult(
            text=text1,
            method="ocr_paddle_img",
            pages=1,
            warnings=warnings,
            platform_hint=platform_hint,
            confidence_avg=conf1,
            used_preset=primary,
        )

    def _ocr_images_with_paddle(self, images: List[Image.Image]) -> Tuple[str, float]:
        """
        Run PaddleOCR and return (text, average_confidence)
        """
        if not self._paddle_ready or not self._paddle:
            return ("", 0.0)

        try:
            import numpy as np  # type: ignore
        except ModuleNotFoundError:
            logger.warning("numpy not installed. Cannot run PaddleOCR.")
            return ("", 0.0)

        pages_out: List[str] = []
        confidences: List[float] = []

        for page_idx, img in enumerate(images, start=1):
            chunks: List[str] = []
            try:
                arr = np.array(img)
                raw = self._paddle.ocr(arr, cls=True) or []
            except Exception as e:
                logger.warning("PaddleOCR failed on page %s: %s", page_idx, e)
                continue

            # Paddle returns nested; be defensive
            try:
                for line in raw:
                    if not isinstance(line, list):
                        continue
                    for item in line:
                        # item: [box, (text, conf)]
                        try:
                            if not item or len(item) < 2:
                                continue
                            text_score = item[1]
                            if isinstance(text_score, (list, tuple)) and len(text_score) >= 2:
                                txt = text_score[0]
                                conf = float(text_score[1])
                                if txt:
                                    chunks.append(str(txt))
                                    if 0.0 <= conf <= 1.0:
                                        confidences.append(conf)
                        except Exception:
                            continue
            except Exception:
                # fallback stringify
                try:
                    chunks.append(str(raw))
                except Exception:
                    pass

            page_text = "\n".join(chunks).strip()
            if page_text:
                pages_out.append(page_text)

        if not pages_out:
            return ("", 0.0)

        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0

        if len(pages_out) == 1:
            return (pages_out[0].strip(), avg_conf)

        joined: List[str] = []
        for i, ptxt in enumerate(pages_out, start=1):
            joined.append(f"\n\n===== PAGE {i} =====\n\n{ptxt}")
        return ("".join(joined).strip(), avg_conf)

    def _ocr_document_ai(self, file_path: str) -> str:
        raise NotImplementedError("OCR_PROVIDER=document_ai not implemented in this build.")

    # -------------------------
    # Stats
    # -------------------------
    def _update_stats(self, method: str, platform: str, time_ms: float) -> None:
        self._stats["total_time_ms"] += time_ms

        if method not in self._stats["by_method"]:
            self._stats["by_method"][method] = {"count": 0, "total_time_ms": 0.0}
        self._stats["by_method"][method]["count"] += 1
        self._stats["by_method"][method]["total_time_ms"] += time_ms

        if platform not in self._stats["by_platform"]:
            self._stats["by_platform"][platform] = {"count": 0, "total_time_ms": 0.0}
        self._stats["by_platform"][platform]["count"] += 1
        self._stats["by_platform"][platform]["total_time_ms"] += time_ms

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self._stats)
        stats["avg_time_ms"] = stats["total_time_ms"] / stats["total_calls"] if stats["total_calls"] else 0.0

        for _m, data in stats.get("by_method", {}).items():
            if data.get("count", 0) > 0:
                data["avg_time_ms"] = data["total_time_ms"] / data["count"]

        for _p, data in stats.get("by_platform", {}).items():
            if data.get("count", 0) > 0:
                data["avg_time_ms"] = data["total_time_ms"] / data["count"]

        return stats

    def reset_stats(self) -> None:
        self._stats = {
            "total_calls": 0,
            "pdf_text_extractions": 0,
            "ocr_calls": 0,
            "two_pass_used": 0,
            "total_time_ms": 0.0,
            "by_method": {},
            "by_platform": {},
        }


# ============================================================
# Compatibility helper
# ============================================================
def maybe_ocr_to_text(file_path: str, platform_hint: str = "UNKNOWN") -> str:
    """
    Backward compatible helper:
    - PDF with text layer: extract always
    - scanned PDF/image: OCR only if ENABLE_OCR=1
    """
    try:
        return OCRService().extract_text(file_path, platform_hint=platform_hint)
    except Exception as e:
        logger.warning("maybe_ocr_to_text failed: %s", e)
        return ""


__all__ = [
    "OCRService",
    "OCRResult",
    "maybe_ocr_to_text",
]
