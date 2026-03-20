# pipeline/etl.py
# ─────────────────────────────────────────────
# ETL Pipeline
# Takes raw scraped job dicts → validates → loads to PostgreSQL
# Also handles: salary normalization, dedup, run logging
# ─────────────────────────────────────────────

import json
import os
import sys
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.utils import setup_logger, normalize_salary, clean_text
from pipeline.database import DatabaseManager
from config.config import PROC_DATA_DIR

logger = setup_logger("etl")


# ── Validation Rules ───────────────────────────────────────────
REQUIRED_FIELDS = ["job_id", "title", "source"]
MAX_DESC_LEN    = 10_000   # characters


def validate_job(job: dict) -> tuple[bool, str]:
    """Return (is_valid, reason). Drop invalid jobs before DB insert."""
    for field in REQUIRED_FIELDS:
        if not job.get(field):
            return False, f"Missing required field: {field}"

    if len(job.get("title", "")) < 3:
        return False, "Title too short"

    if job.get("salary_min") and job.get("salary_max"):
        if job["salary_min"] > job["salary_max"]:
            return False, "salary_min > salary_max"
        if job["salary_max"] > 1_000_000:
            return False, f"Unrealistic salary: {job['salary_max']}"
        if job["salary_min"] < 10_000:
            # Could be hourly not yet annualized — warn but keep
            logger.debug(f"  Low salary_min={job['salary_min']} for {job['title']}")

    return True, "ok"


def clean_job(job: dict) -> dict:
    """Apply cleaning rules to a job dict in-place."""
    # Truncate long descriptions
    if job.get("description") and len(job["description"]) > MAX_DESC_LEN:
        job["description"] = job["description"][:MAX_DESC_LEN]

    # Clean text fields
    for field in ["title", "company", "location", "description"]:
        if job.get(field):
            job[field] = clean_text(job[field])

    # Title case company name
    if job.get("company"):
        job["company"] = job["company"].strip()

    # Ensure booleans
    job["is_remote"] = bool(job.get("is_remote", False))

    return job


class ETLPipeline:
    """
    Orchestrates: validate → clean → deduplicate → load → log.

    Usage:
        etl = ETLPipeline()
        stats = etl.run(jobs)
    """

    def __init__(self):
        self.db = DatabaseManager()
        self.db.setup_schema()

    def run(self, jobs: list[dict], source: str = "indeed",
            query: str = "", location: str = "") -> dict:
        """
        Full ETL run for a batch of scraped jobs.
        Returns stats dict with counts.
        """
        stats = {
            "total_raw":   len(jobs),
            "valid":       0,
            "invalid":     0,
            "inserted":    0,
            "duplicate":   0,
            "errors":      [],
        }

        if not jobs:
            logger.warning("ETL received empty job list")
            return stats

        logger.info(f"\n{'─'*50}")
        logger.info(f"🔄 ETL Pipeline | {len(jobs)} raw jobs | source={source}")
        logger.info(f"{'─'*50}")

        # ── Step 1: Validate & Clean ───────────────────────────
        clean_jobs = []
        for job in jobs:
            valid, reason = validate_job(job)
            if not valid:
                stats["invalid"] += 1
                logger.debug(f"  INVALID [{reason}]: {job.get('title', '?')} @ {job.get('company', '?')}")
                continue

            clean_jobs.append(clean_job(job))
            stats["valid"] += 1

        logger.info(f"  Validation: {stats['valid']} valid, {stats['invalid']} dropped")

        # ── Step 2: Save processed batch to JSON (audit trail) ─
        self._save_processed(clean_jobs, source)

        # ── Step 3: Insert to DB ───────────────────────────────
        attempted, inserted = self.db.insert_jobs(clean_jobs)
        stats["inserted"]  = inserted
        stats["duplicate"] = attempted - inserted

        logger.info(f"  DB insert: {inserted} new, {stats['duplicate']} duplicates skipped")

        # ── Step 4: Log run ────────────────────────────────────
        self.db.log_run(
            source=source, query=query, location=location,
            jobs_found=stats["total_raw"], jobs_new=stats["inserted"],
            status="success"
        )

        # ── Step 5: Summary ────────────────────────────────────
        total_in_db = self.db.get_job_count()
        logger.info(f"\n✅ ETL complete | Total jobs in DB: {total_in_db}")
        stats["total_in_db"] = total_in_db

        return stats

    def _save_processed(self, jobs: list[dict], source: str):
        """Save cleaned jobs as JSON for audit / reprocessing."""
        os.makedirs(PROC_DATA_DIR, exist_ok=True)
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"{source}_processed_{ts}.json"
        fpath = os.path.join(PROC_DATA_DIR, fname)

        # Convert date objects to strings for JSON serialization
        serializable = []
        for job in jobs:
            j = job.copy()
            if hasattr(j.get("date_posted"), "isoformat"):
                j["date_posted"] = j["date_posted"].isoformat()
            serializable.append(j)

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, ensure_ascii=False)

        logger.debug(f"  Processed data saved → {fname}")

    def close(self):
        self.db.close()
