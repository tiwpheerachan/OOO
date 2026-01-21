# -*- coding: utf-8 -*-
"""
ai_document_router.py (ENHANCED + FIXED)

Hybrid routing:
  1) Segment PDF/text (multi_page_analyzer)
  2) Decide route/platform (classifier + segment profile)
  3) Try rule-based extractors first (fast/free) when available
  4) Fallback to AI (ai_fill_peak_row) when enabled

Key policies (your requirement):
  ✅ For META/GOOGLE Ads receipts: use rule-based extractor first (ads_meta / ads_google)
  ✅ Marketplace docs (shopee/lazada/tiktok/spx/ads...) should NOT be handled here
     - return "marketplace" route so main extract_service routes them normally
  ✅ Graceful degradation: if AI disabled/unavailable → return informative meta rows (no crash)
  ✅ Thai prompts supported (lang="th")
  ✅ This file is under app/extractors → import services via ..services.*
  ✅ Prompt selection uses get_prompt_for_route()

Important fixes vs your draft:
  ✅ Use one env flag: ENABLE_LLM (same guard as your ai_service) + optional ENABLE_AI_EXTRACT
     - if ENABLE_LLM=0 → skip AI
     - if ENABLE_AI_EXTRACT=0 → also skip AI
  ✅ Ensure rule-based extractor outputs PEAK formatted rows (base_row_dict+format_peak_row already in those files)
  ✅ Make classifier outputs map correctly (classifier might return: "meta"/"google"/"ads"/"facebook"/"unknown")
  ✅ Multi-page: route per segment and keep page_indices metadata
  ✅ Never pollute PEAK fields with debug meta; debug fields use "_" prefix only
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .multi_page_analyzer import (
    Analysis,
    Segment,
    analyze_pdf_bytes,
    analyze_text_as_single_segment,
)

# Prompts
from .prompts import get_prompt_for_route

# Rule-based extractors (hybrid)
try:
    from .ads_meta import extract_meta_ads
    from .ads_google import extract_google_ads
    _RULE_BASED_OK = True
except Exception:
    extract_meta_ads = None  # type: ignore
    extract_google_ads = None  # type: ignore
    _RULE_BASED_OK = False

# AI filler (existing)
try:
    from ..services.ai_service import ai_fill_peak_row
    _AI_SERVICE_OK = True
except Exception:
    ai_fill_peak_row = None  # type: ignore
    _AI_SERVICE_OK = False

# Classifier (existing)
try:
    from ..services.classifier import classify_platform
    _CLASSIFIER_OK = True
except Exception:
    classify_platform = None  # type: ignore
    _CLASSIFIER_OK = False


logger = logging.getLogger(__name__)


# ============================================================
# Data models
# ============================================================

@dataclass
class RoutedJob:
    route_name: str               # "meta_ads"|"google_ads"|"generic"|"marketplace"
    prompt_name: str              # prompt label used by get_prompt_for_route
    platform_hint: str            # "META"|"GOOGLE"|"UNKNOWN"|...
    segment_index: int
    page_indices: List[int]
    merged_text: str
    partial_row: Dict[str, Any]
    meta: Dict[str, Any]
    use_rule_based: bool = False  # rule-based extractor allowed?


# ============================================================
# Env helpers
# ============================================================

def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _ai_enabled() -> bool:
    """
    Your ai_service already guards by ENABLE_LLM.
    We keep compatibility:
      - ENABLE_LLM must be truthy
      - optional ENABLE_AI_EXTRACT can additionally gate
    """
    if not _env_bool("ENABLE_LLM", False):
        return False
    if not _env_bool("ENABLE_AI_EXTRACT", True):
        return False
    return True


# ============================================================
# Platform normalization
# ============================================================

def _norm_classifier_label(x: str) -> str:
    """
    Normalize classifier output to our routing labels.
    classifier might return: shopee/lazada/tiktok/spx/ads/meta/google/other/unknown/facebook/ig...
    """
    s = (x or "").strip().lower()
    if not s:
        return "unknown"

    # common aliases
    if s in {"fb", "facebook", "meta", "meta_ads", "ads_meta", "instagram", "ig"}:
        return "ads_meta"
    if s in {"google", "google_ads", "ads_google"}:
        return "ads_google"

    if s in {"shopee", "lazada", "tiktok", "spx"}:
        return s

    # classifier may return "ads" (generic)
    if s in {"ads"}:
        # let segment profile decide META/GOOGLE if possible; else generic ads => generic
        return "ads"

    if s in {"other", "unknown"}:
        return "unknown"

    return s


def _norm_profile_hint(seg: Segment) -> Tuple[str, str]:
    ph = (seg.seg_profile.platform_hint or "UNKNOWN").strip().upper()
    kind = (seg.seg_profile.doc_kind or "GENERIC").strip().upper()
    return ph, kind


# ============================================================
# Route decision
# ============================================================

def _choose_route_from_segment(
    seg: Segment,
    filename: str = "",
    use_classifier: bool = True
) -> Tuple[str, str, str, bool]:
    """
    Decide route_name and prompt_name.

    Returns:
      (route_name, prompt_name, platform_hint, use_rule_based)
    """
    detected = "unknown"

    # 1) Classifier first
    if use_classifier and _CLASSIFIER_OK and classify_platform:
        try:
            detected = _norm_classifier_label(
                classify_platform(seg.merged_text, filename=filename)
            )
        except Exception as e:
            logger.warning(f"Classifier failed: {e}")
            detected = "unknown"

    # 2) Segment profile fallback
    if detected in {"unknown", "ads"}:
        ph, kind = _norm_profile_hint(seg)

        if ph == "META" or kind.startswith("META"):
            detected = "ads_meta"
        elif ph == "GOOGLE" or kind.startswith("GOOGLE"):
            detected = "ads_google"
        elif ph in {"SHOPEE", "LAZADA", "TIKTOK", "SPX"}:
            detected = ph.lower()
        else:
            detected = "unknown"

    # 3) Routing table
    if detected == "ads_meta":
        use_rule = _RULE_BASED_OK and extract_meta_ads is not None
        return ("meta_ads", "meta_ads_user", "META", use_rule)

    if detected == "ads_google":
        use_rule = _RULE_BASED_OK and extract_google_ads is not None
        return ("google_ads", "google_ads_user", "GOOGLE", use_rule)

    # Marketplace docs: handled by main extract_service, not here
    if detected in {"shopee", "lazada", "tiktok", "spx"}:
        return ("marketplace", "generic_doc_user", detected.upper(), False)

    # Unknown
    return ("generic", "generic_doc_user", "UNKNOWN", False)


# ============================================================
# Partial row builder for AI
# ============================================================

def _build_partial_row_for_ai(
    route_name: str,
    platform_hint: str,
    cfg: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Only safe hints. Never force PEAK fields incorrectly.
    """
    cfg = cfg or {}
    partial: Dict[str, Any] = {}

    # optional UI hint
    shop_name = str(cfg.get("shop_name") or "").strip()
    if shop_name:
        partial["_shop_name"] = shop_name

    # helpful group hint for ads docs (AI)
    if platform_hint in {"META", "GOOGLE"}:
        partial["U_group"] = "Advertising Expense"
        partial["L_description"] = "Advertising Expense"

    # keep platform hint for AI prompt context
    partial["_platform_hint"] = platform_hint

    # allow passing client tax id
    client_tax_id = str(cfg.get("client_tax_id") or "").strip()
    if client_tax_id:
        partial["_client_tax_id"] = client_tax_id

    return partial


# ============================================================
# Build routing plans
# ============================================================

def build_routing_plan_from_pdf(
    pdf_bytes: bytes,
    filename: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[RoutedJob]]:
    cfg = cfg or {}

    try:
        max_pages = int(os.getenv("ROUTER_MAX_PAGES", "60") or "60")
        analysis: Analysis = analyze_pdf_bytes(pdf_bytes, filename=filename, max_pages=max_pages)
    except Exception as e:
        logger.error(f"PDF analysis failed: {e}")
        return {"filename": filename, "total_pages": 0, "error": str(e)[:200]}, []

    jobs: List[RoutedJob] = []

    for seg in analysis.segments:
        route_name, prompt_name, platform_hint, use_rule = _choose_route_from_segment(
            seg,
            filename=filename,
            use_classifier=True
        )

        partial = _build_partial_row_for_ai(route_name, platform_hint, cfg=cfg)

        meta = {
            "route_name": route_name,
            "prompt_name": prompt_name,
            "platform_hint": platform_hint,
            "segment_index": seg.segment_index,
            "page_indices": seg.page_indices,
            "segment_profile": seg.seg_profile.to_meta(),
            "use_rule_based": use_rule,
        }

        jobs.append(
            RoutedJob(
                route_name=route_name,
                prompt_name=prompt_name,
                platform_hint=platform_hint,
                segment_index=seg.segment_index,
                page_indices=seg.page_indices,
                merged_text=seg.merged_text,
                partial_row=partial,
                meta=meta,
                use_rule_based=use_rule,
            )
        )

    return analysis.to_meta(), jobs


def build_routing_plan_from_text(
    text: str,
    filename: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[RoutedJob]]:
    cfg = cfg or {}

    try:
        analysis = analyze_text_as_single_segment(text or "", filename=filename)
    except Exception as e:
        logger.error(f"Text analysis failed: {e}")
        return {"filename": filename, "total_pages": 1, "error": str(e)[:200]}, []

    jobs: List[RoutedJob] = []

    for seg in analysis.segments:
        route_name, prompt_name, platform_hint, use_rule = _choose_route_from_segment(
            seg,
            filename=filename,
            use_classifier=True
        )

        partial = _build_partial_row_for_ai(route_name, platform_hint, cfg=cfg)

        meta = {
            "route_name": route_name,
            "prompt_name": prompt_name,
            "platform_hint": platform_hint,
            "segment_index": seg.segment_index,
            "page_indices": seg.page_indices,
            "segment_profile": seg.seg_profile.to_meta(),
            "use_rule_based": use_rule,
        }

        jobs.append(
            RoutedJob(
                route_name=route_name,
                prompt_name=prompt_name,
                platform_hint=platform_hint,
                segment_index=seg.segment_index,
                page_indices=seg.page_indices,
                merged_text=seg.merged_text,
                partial_row=partial,
                meta=meta,
                use_rule_based=use_rule,
            )
        )

    return analysis.to_meta(), jobs


# ============================================================
# Execute jobs
# ============================================================

def run_ai_jobs_to_peak_rows(
    jobs: List[RoutedJob],
    source_filename: str,
    lang: str = "en",
) -> List[Dict[str, Any]]:
    """
    Run hybrid extraction per segment:
      1) Rule-based (meta/google) if enabled
      2) AI fallback if enabled (ENABLE_LLM=1 and ENABLE_AI_EXTRACT=1)
      3) Otherwise return meta row with error

    Returns list of dict rows (already PEAK formatted for rule-based extractors,
    and AI patch dict for AI output).
    """
    out: List[Dict[str, Any]] = []

    for job in jobs:
        row: Optional[Dict[str, Any]] = None
        extraction_method = "none"
        error_msg: Optional[str] = None

        # ------------------------------
        # Skip marketplace segments here
        # ------------------------------
        if job.route_name == "marketplace":
            out.append({
                "_route_name": job.route_name,
                "_prompt_name": job.prompt_name,
                "_segment_index": job.segment_index,
                "_page_indices": ",".join(str(i) for i in job.page_indices),
                "_extraction_method": "skipped_marketplace",
                "_error": "Marketplace doc should be handled by extract_service (not router).",
            })
            continue

        # ------------------------------
        # 1) Rule-based first
        # ------------------------------
        if job.use_rule_based:
            try:
                if job.route_name == "meta_ads" and extract_meta_ads:
                    # rule-based extractor already returns PEAK formatted row
                    row = extract_meta_ads(job.merged_text, filename=source_filename)
                    extraction_method = "rule_based_meta"
                elif job.route_name == "google_ads" and extract_google_ads:
                    row = extract_google_ads(job.merged_text, filename=source_filename)
                    extraction_method = "rule_based_google"
            except Exception as e:
                logger.warning(f"Rule-based extraction failed seg={job.segment_index}: {e}")
                error_msg = f"rule_based_failed: {str(e)[:120]}"
                row = None

        # ------------------------------
        # 2) AI fallback
        # ------------------------------
        if row is None:
            if not _AI_SERVICE_OK or ai_fill_peak_row is None:
                out.append({
                    "_route_name": job.route_name,
                    "_prompt_name": job.prompt_name,
                    "_segment_index": job.segment_index,
                    "_page_indices": ",".join(str(i) for i in job.page_indices),
                    "_extraction_method": "failed_no_ai_service",
                    "_error": error_msg or "AI service import not available.",
                })
                continue

            if not _ai_enabled():
                out.append({
                    "_route_name": job.route_name,
                    "_prompt_name": job.prompt_name,
                    "_segment_index": job.segment_index,
                    "_page_indices": ",".join(str(i) for i in job.page_indices),
                    "_extraction_method": "skipped_ai_disabled",
                    "_error": error_msg or "AI extraction disabled (ENABLE_LLM=0 or ENABLE_AI_EXTRACT=0).",
                })
                continue

            try:
                custom_prompt = get_prompt_for_route(job.route_name, lang=lang)

                ai_row = ai_fill_peak_row(
                    text=job.merged_text,
                    platform_hint=job.platform_hint,
                    partial_row=job.partial_row,
                    source_filename=source_filename,
                    custom_prompt=custom_prompt if custom_prompt else None,
                )

                if isinstance(ai_row, dict):
                    row = ai_row
                    extraction_method = f"ai_{lang}"
                else:
                    row = None
                    error_msg = "ai_fill_peak_row returned non-dict"

            except Exception as e:
                logger.error(f"AI extraction failed seg={job.segment_index}: {e}")
                row = None
                error_msg = f"ai_failed: {str(e)[:120]}"

        # ------------------------------
        # 3) Output row + metadata
        # ------------------------------
        if row and isinstance(row, dict):
            row["_route_name"] = job.route_name
            row["_prompt_name"] = job.prompt_name
            row["_segment_index"] = job.segment_index
            row["_page_indices"] = ",".join(str(i) for i in job.page_indices)
            row["_extraction_method"] = extraction_method
            if error_msg:
                row["_extraction_notes"] = error_msg
            out.append(row)
        else:
            out.append({
                "_route_name": job.route_name,
                "_prompt_name": job.prompt_name,
                "_segment_index": job.segment_index,
                "_page_indices": ",".join(str(i) for i in job.page_indices),
                "_extraction_method": "failed",
                "_error": error_msg or "Extraction returned no data",
            })

    return out


# ============================================================
# Convenience
# ============================================================

def extract_with_router(
    text: str = "",
    pdf_bytes: Optional[bytes] = None,
    filename: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    lang: str = "en",
) -> List[Dict[str, Any]]:
    """
    Analyze + Route + Extract in one call.

    Returns list of rows:
      - rule-based: full PEAK row dict
      - AI: patch dict from ai_fill_peak_row
      - errors: meta-only dict with _error
    """
    cfg = cfg or {}

    if pdf_bytes:
        _analysis_meta, jobs = build_routing_plan_from_pdf(pdf_bytes=pdf_bytes, filename=filename, cfg=cfg)
    else:
        _analysis_meta, jobs = build_routing_plan_from_text(text=text, filename=filename, cfg=cfg)

    return run_ai_jobs_to_peak_rows(jobs=jobs, source_filename=filename, lang=lang)


__all__ = [
    "RoutedJob",
    "build_routing_plan_from_pdf",
    "build_routing_plan_from_text",
    "run_ai_jobs_to_peak_rows",
    "extract_with_router",
]
