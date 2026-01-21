# -*- coding: utf-8 -*-
"""
multi_page_analyzer.py (ROBUST VERSION — all-platform, unknown-form friendly)

Split a PDF (or a long text) into "segments" for routing/AI extraction.

Key goals:
✅ รองรับทุก platform (Shopee/Lazada/TikTok/SPX/Meta/Google/Unknown) แบบไม่ผูกกฎเฉพาะ
✅ รองรับหลายหน้า / หลายเอกสารในไฟล์เดียว (multi receipts / statement pack)
✅ รองรับฟอร์มไม่รู้จัก: ใช้ page-level fingerprints + header signature + id changes
✅ ไม่พังเมื่อ PDF เสีย/สแกน/ไม่มี text: degrade gracefully
✅ deterministic (ไม่เรียก AI)

How it works (high level):
1) Extract text per page (pdfplumber)
2) Build PageProfile per page (document_profile.build_page_profile)
3) Walk pages and decide breaks using:
   - doc_kind/platform/tax_id/seller_id/transaction_id/invoice_no/page reset (from PageProfile)
   - plus robust header signature change (from raw page text) for UNKNOWN docs
4) Merge into Segment objects and create SegmentProfile via merge_segment_profile

This module does NOT call OpenAI. It is deterministic.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ✅ Graceful import handling
try:
    import pdfplumber  # type: ignore
    _PDFPLUMBER_OK = True
except Exception:
    pdfplumber = None  # type: ignore
    _PDFPLUMBER_OK = False

from .document_profile import (
    PageProfile,
    SegmentProfile,
    build_page_profile,
    merge_segment_profile,
)

logger = logging.getLogger(__name__)

# ============================================================
# Tunables (safe defaults)
# ============================================================
DEFAULT_MAX_PAGES = 60
DEFAULT_MAX_SEGMENTS = 60          # hard cap to avoid runaway splitting
MIN_TEXT_LEN_FOR_STRONG_RULES = 30 # ignore noisy rules on near-empty pages
MIN_TEXT_LEN_FOR_HEADER_SIG = 80   # if too little text, header sig is unreliable
BLANK_PAGE_LEN = 5                 # consider page "blankish"
MAX_CONSECUTIVE_BLANKS = 2         # keep blank pages with previous segment

# Header signature settings
HEADER_LINES = 5
HEADER_TOKENS_MAX = 60
HEADER_DIGIT_NOISE_RE = re.compile(r"\b\d{3,}\b")
HEADER_WS_RE = re.compile(r"\s+")
NON_WORD_RE = re.compile(r"[^\w\u0E00-\u0E7F]+", re.UNICODE)  # keep Thai/word chars


# ============================================================
# Data classes
# ============================================================
@dataclass
class Segment:
    """A segment of pages that belong together"""
    segment_index: int
    page_indices: List[int]
    merged_text: str
    seg_profile: SegmentProfile

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        return {
            "segment_index": self.segment_index,
            "page_indices": self.page_indices,
            "merged_text_len": len(self.merged_text or ""),
            "platform_hint": getattr(self.seg_profile, "platform_hint", ""),
            "doc_kind": getattr(self.seg_profile, "doc_kind", ""),
            "tax_id_13": getattr(self.seg_profile, "tax_id_13", ""),
            "seller_id": getattr(self.seg_profile, "seller_id", ""),
            "transaction_id": getattr(self.seg_profile, "transaction_id", ""),
            "invoice_no": getattr(self.seg_profile, "invoice_no", ""),
            "reasons": getattr(self.seg_profile, "reasons", []),
        }


@dataclass
class Analysis:
    """Complete analysis result"""
    filename: str
    total_pages: int
    pages: List[PageProfile]
    segments: List[Segment]
    error: Optional[str] = None

    def to_meta(self) -> Dict[str, Any]:
        """Convert to metadata dict"""
        result = {
            "filename": self.filename,
            "total_pages": self.total_pages,
            "pages": [p.to_meta() for p in self.pages],
            "segments": [s.to_meta() for s in self.segments],
        }
        if self.error:
            result["error"] = self.error
        return result


# ============================================================
# Utilities
# ============================================================
def _safe_get(obj: Any, name: str, default: Any = "") -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _is_blank_text(t: str) -> bool:
    if t is None:
        return True
    s = str(t).strip()
    return len(s) <= BLANK_PAGE_LEN


def _normalize_for_sig(t: str) -> str:
    """
    Normalize raw page text for header signature:
    - remove extra whitespace
    - remove long digit tokens (invoice numbers etc) to reduce false splits
    - keep Thai/word characters
    """
    if not t:
        return ""
    s = str(t)
    s = s.replace("\x00", " ")
    s = HEADER_WS_RE.sub(" ", s).strip()
    s = HEADER_DIGIT_NOISE_RE.sub(" ", s)
    s = NON_WORD_RE.sub(" ", s)
    s = HEADER_WS_RE.sub(" ", s).strip().lower()
    return s


def _header_signature(page_text: str) -> str:
    """
    Build a compact signature based on first N non-empty lines.
    Used for UNKNOWN docs / unknown templates.
    """
    if not page_text:
        return ""

    lines = []
    for raw in str(page_text).splitlines():
        line = raw.strip()
        if not line:
            continue
        lines.append(line)
        if len(lines) >= HEADER_LINES:
            break

    if not lines:
        return ""

    joined = " ".join(lines)
    norm = _normalize_for_sig(joined)
    if not norm:
        return ""

    toks = norm.split(" ")
    toks = [x for x in toks if x]
    if not toks:
        return ""

    # keep a bounded number of tokens
    toks = toks[:HEADER_TOKENS_MAX]
    return " ".join(toks)


def _jaccard(a: str, b: str) -> float:
    """
    Token-set Jaccard similarity.
    1.0 = identical, 0.0 = disjoint
    """
    if not a or not b:
        return 0.0
    sa = set(a.split())
    sb = set(b.split())
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def validate_pdf_bytes(pdf_bytes: bytes) -> Tuple[bool, str]:
    """Validate PDF bytes before processing."""
    if not pdf_bytes:
        return False, "Empty PDF bytes"
    if len(pdf_bytes) < 100:
        return False, f"PDF too small: {len(pdf_bytes)} bytes"
    if not pdf_bytes.startswith(b"%PDF"):
        return False, "Not a valid PDF file (missing %PDF header)"
    if not _PDFPLUMBER_OK:
        # still valid PDF, but extractor cannot read text
        return False, "pdfplumber not installed"
    return True, ""


def is_pdfplumber_available() -> bool:
    return _PDFPLUMBER_OK


def get_analysis_summary(analysis: Analysis) -> str:
    if not analysis or not analysis.segments:
        return "No analysis available"
    platforms = sorted({getattr(s.seg_profile, "platform_hint", "UNKNOWN") for s in analysis.segments})
    return f"{analysis.total_pages} pages, {len(analysis.segments)} segments ({', '.join(platforms)})"


# ============================================================
# PDF text extraction
# ============================================================
def _extract_pdf_page_texts(pdf_bytes: bytes, max_pages: int = DEFAULT_MAX_PAGES) -> List[str]:
    """
    Extract per-page texts safely.

    Notes:
    - If page extract fails => keep "" placeholder to preserve page indices
    - If pdfplumber unavailable => raise ImportError (caller may degrade gracefully)
    """
    if not _PDFPLUMBER_OK or pdfplumber is None:
        raise ImportError("pdfplumber is required. Install with: pip install pdfplumber")

    if not pdf_bytes:
        return []

    if len(pdf_bytes) < 100:
        return []

    texts: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not getattr(pdf, "pages", None):
                return []

            n_pages = len(pdf.pages)
            n = min(n_pages, max_pages)
            if n_pages > max_pages:
                logger.info("PDF has %s pages, limiting analyze to %s", n_pages, max_pages)

            for i in range(n):
                try:
                    page = pdf.pages[i]
                    page_text = page.extract_text()  # may return None
                    if page_text is None:
                        texts.append("")
                    else:
                        texts.append(page_text or "")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("Page %s extracted chars=%s", i, len(texts[-1]))
                except Exception as e:
                    logger.warning("Page %s extraction failed: %s", i, e)
                    texts.append("")
    except Exception as e:
        logger.error("PDF parsing failed: %s", e)
        raise RuntimeError(f"Failed to parse PDF: {str(e)[:200]}")

    return texts


# ============================================================
# Segmentation logic
# ============================================================
def _should_break(prev: PageProfile, cur: PageProfile, prev_text: str, cur_text: str) -> Tuple[bool, str]:
    """
    Decide if we should start a new segment at cur page.
    Return (break?, reason)

    This is platform-agnostic + unknown friendly:
    - uses PageProfile strong signals (doc_kind/platform/tax_id/invoice/etc.)
    - plus header signature change for UNKNOWN docs
    """

    prev_text_len = _safe_get(prev, "text_len", 0) or 0
    cur_text_len = _safe_get(cur, "text_len", 0) or 0

    prev_kind = _safe_get(prev, "doc_kind", "GENERIC") or "GENERIC"
    cur_kind = _safe_get(cur, "doc_kind", "GENERIC") or "GENERIC"

    prev_platform = _safe_get(prev, "platform_hint", "UNKNOWN") or "UNKNOWN"
    cur_platform = _safe_get(cur, "platform_hint", "UNKNOWN") or "UNKNOWN"

    prev_tax = _safe_get(prev, "tax_id_13", "") or ""
    cur_tax = _safe_get(cur, "tax_id_13", "") or ""

    prev_seller = _safe_get(prev, "seller_id", "") or ""
    cur_seller = _safe_get(cur, "seller_id", "") or ""

    prev_txn = _safe_get(prev, "transaction_id", "") or ""
    cur_txn = _safe_get(cur, "transaction_id", "") or ""

    prev_inv = _safe_get(prev, "invoice_no", "") or ""
    cur_inv = _safe_get(cur, "invoice_no", "") or ""

    prev_page_x = _safe_get(prev, "page_x", None)
    cur_page_x = _safe_get(cur, "page_x", None)

    # ------------------------------------------------------------
    # Blank-page handling: don't split on blank pages (usually separator)
    # ------------------------------------------------------------
    if _is_blank_text(cur_text) and not _is_blank_text(prev_text):
        return False, "keep_blank_with_prev"
    if _is_blank_text(prev_text) and not _is_blank_text(cur_text):
        # coming out of blank pages: allow split only if strong signals exist below
        pass

    # ------------------------------------------------------------
    # RULE 1: doc_kind changes (strong)
    # ------------------------------------------------------------
    if prev_kind != cur_kind and cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
        if prev_kind != "GENERIC" and cur_kind != "GENERIC":
            return True, f"doc_kind change: {prev_kind} → {cur_kind}"

    # ------------------------------------------------------------
    # RULE 2: platform changes (strong, but ignore UNKNOWN noise)
    # ------------------------------------------------------------
    if prev_platform != cur_platform and cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
        if prev_platform != "UNKNOWN" and cur_platform != "UNKNOWN":
            return True, f"platform change: {prev_platform} → {cur_platform}"

    # ------------------------------------------------------------
    # RULE 3: any transaction id change (strong for receipt packs)
    # ------------------------------------------------------------
    if prev_txn and cur_txn and prev_txn != cur_txn:
        # only split if current has meaningful text
        if cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
            return True, f"transaction_id change: {prev_txn} → {cur_txn}"

    # ------------------------------------------------------------
    # RULE 4: any invoice number change (strong)
    # ------------------------------------------------------------
    if prev_inv and cur_inv and prev_inv != cur_inv:
        if cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
            return True, f"invoice_no change: {prev_inv} → {cur_inv}"

    # ------------------------------------------------------------
    # RULE 5: Tax ID changes (very strong: different vendor/company)
    # ------------------------------------------------------------
    if prev_tax and cur_tax and prev_tax != cur_tax:
        return True, f"tax_id change: {prev_tax} → {cur_tax}"

    # ------------------------------------------------------------
    # RULE 6: Page numbering reset (common in multi-doc PDFs)
    # ------------------------------------------------------------
    if prev_page_x and cur_page_x:
        if cur_page_x == 1 and prev_page_x != 1:
            # avoid splitting if previous was basically blank (scanned/empty)
            if prev_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES and cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
                return True, f"page reset: prev page_x={prev_page_x}, cur page_x=1"

    # ------------------------------------------------------------
    # RULE 7: Seller ID changes (medium)
    # ------------------------------------------------------------
    if prev_seller and cur_seller and prev_seller != cur_seller:
        if cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
            return True, f"seller_id change: {prev_seller} → {cur_seller}"

    # ------------------------------------------------------------
    # RULE 8: Unknown-doc fallback using header signature change
    # - Works best when platform/doc_kind are UNKNOWN/GENERIC
    # - Reduce false splits by requiring enough text
    # ------------------------------------------------------------
    is_unknownish = (
        (prev_platform in ("UNKNOWN", "", None) or prev_kind in ("GENERIC", "", None)) and
        (cur_platform in ("UNKNOWN", "", None) or cur_kind in ("GENERIC", "", None))
    )

    if is_unknownish and prev_text_len >= MIN_TEXT_LEN_FOR_HEADER_SIG and cur_text_len >= MIN_TEXT_LEN_FOR_HEADER_SIG:
        sig_prev = _header_signature(prev_text)
        sig_cur = _header_signature(cur_text)

        if sig_prev and sig_cur:
            sim = _jaccard(sig_prev, sig_cur)

            # If header is quite different -> split
            # (0.0–0.25 usually different document/header)
            if sim <= 0.25:
                return True, f"header signature change (jaccard={sim:.2f})"

    # ------------------------------------------------------------
    # RULE 9: "hard boundary" markers in text (generic)
    # - e.g., "Tax Invoice", "ใบกำกับภาษี", "Receipt", "Statement"
    # ------------------------------------------------------------
    if cur_text_len > MIN_TEXT_LEN_FOR_STRONG_RULES:
        boundary_markers = (
            "tax invoice", "receipt", "statement", "ใบกำกับภาษี", "ใบเสร็จ", "ใบรับ", "ใบแจ้งหนี้"
        )
        cur_lower = (cur_text or "").lower()
        prev_lower = (prev_text or "").lower()
        # Split if current starts a new document title but previous page wasn't a title start
        if any(m in cur_lower[:400] for m in boundary_markers) and not any(m in prev_lower[:400] for m in boundary_markers):
            # avoid splitting if header signature is still highly similar
            sig_prev = _header_signature(prev_text)
            sig_cur = _header_signature(cur_text)
            if not sig_prev or not sig_cur or _jaccard(sig_prev, sig_cur) < 0.60:
                return True, "boundary marker appears"

    return False, ""


# ============================================================
# Main API
# ============================================================
def analyze_pdf_bytes(
    pdf_bytes: bytes,
    filename: str = "",
    max_pages: int = DEFAULT_MAX_PAGES,
    max_segments: int = DEFAULT_MAX_SEGMENTS,
) -> Analysis:
    """
    Analyze PDF and split into segments.

    Degradation strategy:
    - If pdfplumber missing: return single segment with empty text but valid structure
    - If extraction fails: return Analysis with error and best-effort fallback segment
    - If no pages analyzed: fallback to single segment
    """

    if not pdf_bytes:
        return Analysis(filename=filename, total_pages=0, pages=[], segments=[], error="Empty PDF bytes")

    # If pdfplumber not installed: degrade gracefully (still return 1 segment)
    if not _PDFPLUMBER_OK:
        logger.error("pdfplumber not installed; cannot extract PDF text. Degrading to single empty segment.")
        try:
            # build minimal profiles based on empty text (filename hint still works)
            page = build_page_profile(0, "", filename=filename)
            pages = [page]
            seg_profile = merge_segment_profile(0, pages, "")
            seg_profile.reasons.append("degraded: pdfplumber_missing")
            seg = Segment(segment_index=0, page_indices=[0], merged_text="", seg_profile=seg_profile)
            return Analysis(filename=filename, total_pages=1, pages=pages, segments=[seg], error="pdfplumber not installed")
        except Exception:
            return Analysis(filename=filename, total_pages=0, pages=[], segments=[], error="pdfplumber not installed")

    # Extract per-page texts
    try:
        page_texts = _extract_pdf_page_texts(pdf_bytes, max_pages=max_pages)
    except Exception as e:
        logger.error("PDF text extraction failed: %s", e)
        # fallback: single empty segment
        try:
            page = build_page_profile(0, "", filename=filename)
            pages = [page]
            seg_profile = merge_segment_profile(0, pages, "")
            seg_profile.reasons.append(f"degraded: extraction_failed={str(e)[:80]}")
            seg = Segment(segment_index=0, page_indices=[0], merged_text="", seg_profile=seg_profile)
            return Analysis(filename=filename, total_pages=1, pages=pages, segments=[seg], error=f"PDF extraction failed: {str(e)[:120]}")
        except Exception:
            return Analysis(filename=filename, total_pages=0, pages=[], segments=[], error=f"PDF extraction failed: {str(e)[:120]}")

    if not page_texts:
        # PDF exists but no text extracted (scan / empty)
        logger.warning("No page texts extracted; creating single empty segment.")
        try:
            page = build_page_profile(0, "", filename=filename)
            pages = [page]
            seg_profile = merge_segment_profile(0, pages, "")
            seg_profile.reasons.append("degraded: no_text_extracted")
            seg = Segment(segment_index=0, page_indices=[0], merged_text="", seg_profile=seg_profile)
            return Analysis(filename=filename, total_pages=1, pages=pages, segments=[seg], error="No text extracted (scanned/empty PDF)")
        except Exception:
            return Analysis(filename=filename, total_pages=0, pages=[], segments=[], error="No text extracted (scanned/empty PDF)")

    # Build page profiles (preserve indices; if profile build fails, create minimal profile from empty)
    pages: List[PageProfile] = []
    for i, t in enumerate(page_texts):
        try:
            profile = build_page_profile(i, t or "", filename=filename)
            pages.append(profile)
        except Exception as e:
            logger.warning("Page %s profile building failed: %s", i, e)
            try:
                profile = build_page_profile(i, "", filename=filename)
                pages.append(profile)
            except Exception:
                # skip page only if absolutely impossible
                continue

    if not pages:
        logger.warning("No pages could be analyzed; fallback to single segment.")
        try:
            page = build_page_profile(0, "", filename=filename)
            pages = [page]
            seg_profile = merge_segment_profile(0, pages, "")
            seg_profile.reasons.append("degraded: no_pages_profiled")
            seg = Segment(segment_index=0, page_indices=[0], merged_text="", seg_profile=seg_profile)
            return Analysis(filename=filename, total_pages=1, pages=pages, segments=[seg], error="No pages could be analyzed")
        except Exception:
            return Analysis(filename=filename, total_pages=0, pages=[], segments=[], error="No pages could be analyzed")

    # ============================================================
    # Build segments by break rules
    # ============================================================
    segments: List[Segment] = []
    start = 0
    seg_idx = 0
    consecutive_blanks = 0

    def _make_segment(seg_index: int, chunk_pages: List[PageProfile], reason: str = "") -> Optional[Segment]:
        if not chunk_pages:
            return None

        # Merge page texts
        merged_parts: List[str] = []
        page_idxs: List[int] = []
        for p in chunk_pages:
            pi = _safe_get(p, "page_index", None)
            if pi is None:
                continue
            page_idxs.append(int(pi))
            if 0 <= int(pi) < len(page_texts):
                merged_parts.append(page_texts[int(pi)] or "")
        merged = "\n\n".join(merged_parts).strip()

        try:
            seg_profile = merge_segment_profile(seg_index, chunk_pages, merged)
            if reason:
                try:
                    seg_profile.reasons.append(f"split_reason={reason}")
                except Exception:
                    pass
            return Segment(
                segment_index=seg_index,
                page_indices=page_idxs,
                merged_text=merged,
                seg_profile=seg_profile,
            )
        except Exception as e:
            logger.error("Segment %s creation failed: %s", seg_index, e)
            return None

    try:
        for i in range(1, len(pages)):
            prev_p = pages[i - 1]
            cur_p = pages[i]
            prev_i = int(_safe_get(prev_p, "page_index", i - 1) or (i - 1))
            cur_i = int(_safe_get(cur_p, "page_index", i) or i)

            prev_text = page_texts[prev_i] if 0 <= prev_i < len(page_texts) else ""
            cur_text = page_texts[cur_i] if 0 <= cur_i < len(page_texts) else ""

            # track blank pages to avoid over-splitting on separators
            if _is_blank_text(cur_text):
                consecutive_blanks += 1
            else:
                consecutive_blanks = 0

            brk, reason = _should_break(prev_p, cur_p, prev_text, cur_text)

            # If we have too many blank pages, keep them in same segment
            if consecutive_blanks <= MAX_CONSECUTIVE_BLANKS and brk:
                chunk = pages[start:i]
                seg = _make_segment(seg_idx, chunk, reason=reason)
                if seg:
                    segments.append(seg)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("Segment %s created pages=%s reason=%s", seg_idx, seg.page_indices, reason)
                    seg_idx += 1
                    start = i

                # Hard cap
                if seg_idx >= max_segments:
                    logger.warning("Reached max_segments=%s; stop splitting further.", max_segments)
                    break

        # Last segment: remaining pages
        if start < len(pages):
            seg = _make_segment(seg_idx, pages[start:], reason="")
            if seg:
                segments.append(seg)

    except Exception as e:
        logger.error("Segmentation failed: %s", e)
        # fall back below

    # Final validation: ensure at least one segment
    if not segments:
        logger.warning("No segments created; fallback to single segment.")
        seg = _make_segment(0, pages, reason="fallback_single_segment")
        if seg:
            segments = [seg]
        else:
            # ultimate fallback
            try:
                merged = "\n\n".join(page_texts).strip()
            except Exception:
                merged = ""
            try:
                seg_profile = merge_segment_profile(0, pages, merged)
                seg_profile.reasons.append("fallback: merge_failed_then_recovered")
                segments = [Segment(0, [int(_safe_get(p, "page_index", 0) or 0) for p in pages], merged, seg_profile)]
            except Exception:
                return Analysis(filename=filename, total_pages=len(pages), pages=pages, segments=[], error="Segmentation failed completely")

    logger.info("Analysis complete: %s, %s pages, %s segments", filename, len(pages), len(segments))
    return Analysis(filename=filename, total_pages=len(pages), pages=pages, segments=segments, error=None)


def analyze_text_as_single_segment(text: str, filename: str = "") -> Analysis:
    """For non-pdf flows: treat as 1 segment."""
    if not text:
        return Analysis(filename=filename, total_pages=1, pages=[], segments=[], error="Empty text")

    try:
        page = build_page_profile(0, text or "", filename=filename)
        pages = [page]

        seg_profile = merge_segment_profile(0, pages, text or "")
        seg = Segment(segment_index=0, page_indices=[0], merged_text=text or "", seg_profile=seg_profile)

        return Analysis(filename=filename, total_pages=1, pages=pages, segments=[seg], error=None)
    except Exception as e:
        logger.error("Text analysis failed: %s", e)
        return Analysis(filename=filename, total_pages=1, pages=[], segments=[], error=f"Text analysis failed: {str(e)[:120]}")


__all__ = [
    "Segment",
    "Analysis",
    "analyze_pdf_bytes",
    "analyze_text_as_single_segment",
    "is_pdfplumber_available",
    "get_analysis_summary",
    "validate_pdf_bytes",
]
