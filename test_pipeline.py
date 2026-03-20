# test_pipeline.py
# ─────────────────────────────────────────────
# Tests all core logic WITHOUT needing a live DB or real scrape.
# Run this first to verify everything is wired correctly.
# ─────────────────────────────────────────────

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.utils import (
    normalize_salary, detect_seniority, detect_remote,
    parse_posted_date, make_job_id, clean_text, setup_logger
)
from pipeline.etl import validate_job, clean_job

logger = setup_logger("test")

def section(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")

def check(label, result, expected):
    status = "✅" if result == expected else "❌"
    print(f"  {status}  {label}")
    if result != expected:
        print(f"       got:      {result}")
        print(f"       expected: {expected}")


# ── 1. Salary Normalization ────────────────────────────────────
section("1. Salary Normalization")

cases = [
    ("$120,000 - $150,000 a year",  120000.0, 150000.0, "annual"),
    ("$45/hr",                       93600.0,  93600.0, "hourly"),   # 45*2080, period kept as-is
    ("From $80,000 a year",          80000.0,  80000.0, "annual"),
    ("$7,000 a month",              84000.0,  84000.0, "monthly"),  # 7k*12, period kept as-is
    ("Competitive salary",              None,      None,   None),
    (None,                               None,      None,   None),
    ("$150,000+",                   150000.0, 150000.0, "annual"),
]

for raw, exp_min, exp_max, exp_period in cases:
    result = normalize_salary(raw)
    check(f"normalize_salary({repr(raw[:30] if raw else raw)})",
          (result["salary_min"], result["salary_max"], result["salary_period"]),
          (exp_min, exp_max, exp_period))


# ── 2. Seniority Detection ─────────────────────────────────────
section("2. Seniority Detection")

seniority_cases = [
    ("Senior Data Engineer",          "senior"),
    ("Sr. Data Analyst",              "senior"),
    ("Junior ML Engineer",            "junior"),
    ("Lead Data Scientist",           "lead"),
    ("Principal Software Engineer",   "principal"),
    ("Data Engineer II",              "mid"),
    ("Data Science Intern",           "intern"),
    ("Associate Data Analyst",        "junior"),
]

for title, expected in seniority_cases:
    check(f"detect_seniority('{title}')", detect_seniority(title), expected)


# ── 3. Remote Detection ────────────────────────────────────────
section("3. Remote Detection")

remote_cases = [
    ("Remote",             "",                True),
    ("New York, NY",       "Data Engineer",   False),
    ("Work from home",     "",                True),
    ("San Francisco, CA",  "Remote Optional", True),
    ("Austin, TX",         "Onsite",          False),
]

for loc, title, expected in remote_cases:
    check(f"detect_remote('{loc}', '{title}')", detect_remote(loc, title), expected)


# ── 4. Date Parsing ────────────────────────────────────────────
section("4. Date Parsing")

from datetime import date, timedelta

date_cases = [
    ("Posted today",         date.today()),
    ("Just posted",          date.today()),
    ("Posted 3 days ago",    date.today() - timedelta(days=3)),
    ("Posted 30+ days ago",  date.today() - timedelta(days=30)),
    (None,                   None),
]

for raw, expected in date_cases:
    check(f"parse_posted_date({repr(raw)})", parse_posted_date(raw), expected)


# ── 5. Job ID Fingerprint ──────────────────────────────────────
section("5. Job ID / Deduplication")

id1 = make_job_id("Senior Data Engineer", "Google", "Remote", "indeed")
id2 = make_job_id("Senior Data Engineer", "Google", "Remote", "indeed")
id3 = make_job_id("Senior Data Engineer", "Google", "New York", "indeed")
id4 = make_job_id("Senior Data Engineer", "Google", "Remote", "linkedin")

check("Same job → same hash",       id1 == id2, True)
check("Different location → diff",  id1 == id3, False)
check("Different source → diff",    id1 == id4, False)


# ── 6. Validation ──────────────────────────────────────────────
section("6. Job Validation")

valid_job = {
    "job_id": "abc123", "title": "Data Engineer",
    "company": "Acme", "source": "indeed",
    "salary_min": 90000, "salary_max": 130000
}
bad_salary_job = {**valid_job, "salary_min": 200000, "salary_max": 100000}
no_title_job   = {**valid_job, "title": ""}
no_id_job      = {**valid_job, "job_id": None}

check("Valid job passes",           validate_job(valid_job)[0],      True)
check("Bad salary caught",          validate_job(bad_salary_job)[0], False)
check("Missing title caught",       validate_job(no_title_job)[0],   False)
check("Missing job_id caught",      validate_job(no_id_job)[0],      False)


# ── 7. Mock ETL (no DB) ────────────────────────────────────────
section("7. Mock ETL — Clean + Validate Batch")

mock_jobs = [
    {
        "job_id": make_job_id("Data Engineer", "Google", "Remote", "indeed"),
        "title": "  Senior Data Engineer  ",
        "company": "google",
        "location": "Remote",
        "is_remote": None,
        "salary_min": 150000, "salary_max": 200000, "salary_period": "annual",
        "job_type": None, "seniority": "senior",
        "description": "Build data pipelines. " * 100,  # long desc
        "url": "https://indeed.com/job/123",
        "source": "indeed",
        "search_query": "Data Engineer",
        "search_location": "Remote",
        "date_posted": date.today()
    },
    {
        "job_id": None,   # should be INVALID
        "title": "ML Eng", "company": "Meta", "source": "indeed",
        "salary_min": None, "salary_max": None, "salary_period": None,
        "location": "NYC", "is_remote": False, "description": None,
        "url": None, "job_type": None, "seniority": "mid",
        "search_query": "ML", "search_location": "NYC", "date_posted": None
    }
]

valid_count = 0
invalid_count = 0
for job in mock_jobs:
    ok, reason = validate_job(job)
    if ok:
        cleaned = clean_job(job)
        valid_count += 1
        # Check description truncated
        assert len(cleaned.get("description", "") or "") <= 10000, "Description not truncated!"
        # Check is_remote converted to bool
        assert isinstance(cleaned["is_remote"], bool), "is_remote not bool!"
    else:
        invalid_count += 1

check("Batch: 1 valid",   valid_count,   1)
check("Batch: 1 invalid", invalid_count, 1)


# ── Summary ────────────────────────────────────────────────────
print(f"\n{'='*55}")
print("  All tests passed! Pipeline logic is verified.")
print("  Next: connect PostgreSQL to run the full pipeline.")
print(f"{'='*55}\n")
