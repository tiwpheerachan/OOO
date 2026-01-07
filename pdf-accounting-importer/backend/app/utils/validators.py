from __future__ import annotations
import re
from datetime import datetime

def validate_yyyymmdd(v: str) -> bool:
    if not v:
        return True
    if not re.fullmatch(r"\d{8}", v):
        return False
    try:
        datetime.strptime(v, "%Y%m%d")
        return True
    except:
        return False

def validate_branch5(v: str) -> bool:
    if not v:
        return True
    return bool(re.fullmatch(r"\d{5}", v))

def validate_tax13(v: str) -> bool:
    if not v:
        return True
    return bool(re.fullmatch(r"\d{13}", v))

def validate_price_type(v: str) -> bool:
    return v in ("1","2","3","")

def validate_vat_rate(v: str) -> bool:
    if not v:
        return True
    if v.upper() == "NO":
        return True
    return bool(re.fullmatch(r"\d{1,2}%?", v))
