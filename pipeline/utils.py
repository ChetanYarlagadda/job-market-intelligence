# pipeline/utils.py
import hashlib
import re
import logging
import io
import os
import sys
from datetime import datetime, date
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import LOG_DIR


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """Configure logger. Forces UTF-8 on Windows to prevent emoji UnicodeEncodeError."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        # Console - force UTF-8 on Windows (fixes emoji crash)
        try:
            utf8_stream = io.TextIOWrapper(
                sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
            )
            ch = logging.StreamHandler(utf8_stream)
        except AttributeError:
            ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        # File - always UTF-8
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def make_job_id(title: str, company: str, location: str, source: str) -> str:
    raw = f"{title.lower().strip()}|{company.lower().strip()}|{location.lower().strip()}|{source}"
    return hashlib.md5(raw.encode()).hexdigest()


_SALARY_RE = re.compile(
    r'\$?\s*([\d,]+(?:\.\d+)?)\s*(?:[-\u2013to]+\s*\$?\s*([\d,]+(?:\.\d+)?))?',
    re.IGNORECASE
)
_PERIOD_RE = re.compile(r'\b(hour|hr|year|yr|annual|month|week)\b', re.IGNORECASE)

def normalize_salary(raw):
    result = {"salary_min": None, "salary_max": None, "salary_period": None}
    if not raw:
        return result
    raw_clean = raw.replace(",", "")
    match = _SALARY_RE.search(raw_clean)
    if not match:
        return result
    lo = float(match.group(1)) if match.group(1) else None
    hi = float(match.group(2)) if match.group(2) else lo
    period_match = _PERIOD_RE.search(raw)
    period = "annual"
    if period_match:
        p = period_match.group(1).lower()
        if p in ("hour", "hr"):
            period = "hourly"
            lo = round(lo * 2080, 2) if lo else None
            hi = round(hi * 2080, 2) if hi else None
        elif p == "month":
            period = "monthly"
            lo = round(lo * 12, 2) if lo else None
            hi = round(hi * 12, 2) if hi else None
        elif p == "week":
            period = "weekly"
            lo = round(lo * 52, 2) if lo else None
            hi = round(hi * 52, 2) if hi else None
    result.update({"salary_min": lo, "salary_max": hi, "salary_period": period})
    return result


_SENIORITY_MAP = [
    (r'\b(principal|staff|distinguished)\b', "principal"),
    (r'\b(lead|tech lead|architect)\b',       "lead"),
    (r'\b(senior|sr\.?|sr )\b',              "senior"),
    (r'\b(mid.?level|mid\b)',                 "mid"),
    (r'\b(junior|jr\.?|jr |entry.?level|associate)\b', "junior"),
    (r'\b(intern|internship|co.?op)\b',       "intern"),
]

def detect_seniority(title: str) -> str:
    t = title.lower()
    for pattern, label in _SENIORITY_MAP:
        if re.search(pattern, t):
            return label
    return "mid"


_REMOTE_RE = re.compile(r'\b(remote|work from home|wfh|distributed|anywhere)\b', re.IGNORECASE)

def detect_remote(location: str, title: str = "", description: str = "") -> bool:
    return bool(_REMOTE_RE.search(f"{location} {title} {description}"))


def parse_posted_date(raw):
    from datetime import timedelta
    if not raw:
        return None
    r = raw.lower().strip()
    if "just" in r or "today" in r or "hour" in r:
        return date.today()
    m = re.search(r'(\d+)\+?\s*day', r)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))
    m = re.search(r'(\d+)\+?\s*month', r)
    if m:
        return date.today() - timedelta(days=int(m.group(1)) * 30)
    return date.today()


def clean_text(text):
    if not text:
        return None
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text if text else None