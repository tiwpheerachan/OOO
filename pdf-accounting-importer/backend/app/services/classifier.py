from __future__ import annotations
from typing import Literal
from ..utils.text_utils import normalize_text

PlatformLabel = Literal["shopee","lazada","tiktok","other","ads","unknown"]

def classify_platform(text: str) -> PlatformLabel:
    t = normalize_text(text).lower()
    if not t:
        return "unknown"
    if "shopee" in t or "trs" in t and "tiv" in t:
        return "shopee"
    if "lazada" in t:
        return "lazada"
    if "tiktok" in t or "ttsth" in t:
        return "tiktok"
    if "ads" in t or "advertising" in t or "โฆษณา" in t:
        return "ads"
    # tax invoice signals
    if "ใบกำกับภาษี" in t or "tax invoice" in t:
        return "other"
    return "unknown"
