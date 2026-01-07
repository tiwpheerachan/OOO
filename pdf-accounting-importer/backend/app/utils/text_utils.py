from __future__ import annotations
import re
from datetime import datetime

RE_THAI_DIGITS = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.translate(RE_THAI_DIGITS)
    s = s.replace("\u00a0"," ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def fmt_branch_5(s: str) -> str:
    d = only_digits(s)
    if not d:
        return ""
    try:
        n = int(d)
        return f"{n:05d}"
    except:
        return ""

def fmt_tax_13(s: str) -> str:
    d = only_digits(s)
    if len(d) == 13:
        return d
    return ""

def parse_date_to_yyyymmdd(text: str) -> str:
    # supports: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD
    t = normalize_text(text)
    if not t:
        return ""

    # YYYYMMDD already?
    if re.fullmatch(r"\d{8}", t):
        return t

    m = re.search(r"(\d{4})[\-/\.](\d{1,2})[\-/\.](\d{1,2})", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1900 <= y <= 2200 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}{mo:02d}{d:02d}"

    m = re.search(r"(\d{1,2})[\-/\.](\d{1,2})[\-/\.](\d{2,4})", t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        if 1900 <= y <= 2200 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}{mo:02d}{d:02d}"

    return ""

def parse_money(text: str) -> str:
    # returns normalized numeric string without commas
    if not text:
        return ""
    t = normalize_text(text)
    t = t.replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d{1,4})?)", t)
    return m.group(1) if m else ""
