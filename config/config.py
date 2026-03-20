# config/config.py
# ─────────────────────────────────────────────
# Central config for the Job Market Intelligence Engine
# ─────────────────────────────────────────────

import os

# ── Search Parameters ──────────────────────────────────────────
SEARCH_QUERIES = [
    "Data Engineer",
    "Data Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer",
]

SEARCH_LOCATIONS = [
    "United States",
    "Remote",
    "New York, NY",
    "San Francisco, CA",
    "Seattle, WA",
    "Austin, TX",
    "Atlanta, GA",
]

MAX_PAGES_PER_QUERY = 5          # 15 results/page → ~75 jobs per query
REQUEST_DELAY_MIN   = 2.5        # seconds between requests (polite scraping)
REQUEST_DELAY_MAX   = 5.0

# ── Database ───────────────────────────────────────────────────
# Railway provides DATABASE_URL as a full connection string.
# Fall back to individual env vars (or local defaults) for local dev.
def _build_db_config():
    url = os.getenv("DATABASE_URL", "")
    if url:
        # Parse postgres://user:password@host:port/dbname
        import re as _re
        m = _re.match(
            r"postgres(?:ql)?://([^:]+):([^@]+)@([^:/]+):(\d+)/(.+)",
            url
        )
        if m:
            return {
                "user":     m.group(1),
                "password": m.group(2),
                "host":     m.group(3),
                "port":     m.group(4),
                "database": m.group(5),
            }
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     os.getenv("DB_PORT",     "5432"),
        "database": os.getenv("DB_NAME",     "job_market"),
        "user":     os.getenv("DB_USER",     "postgres"),
        "password": os.getenv("DB_PASSWORD", "Kafka@2104"),
    }

DB_CONFIG = _build_db_config()

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR   = os.path.join(BASE_DIR, "data", "raw")
PROC_DATA_DIR  = os.path.join(BASE_DIR, "data", "processed")
LOG_DIR        = os.path.join(BASE_DIR, "logs")

# ── Scheduler ──────────────────────────────────────────────────
SCRAPE_HOUR   = 6    # Run daily at 6:00 AM
SCRAPE_MINUTE = 0

# ── User Agents (rotate to avoid blocks) ──────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]
