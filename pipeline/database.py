# pipeline/database.py
# ─────────────────────────────────────────────
# PostgreSQL connection manager + schema setup
# ─────────────────────────────────────────────

import psycopg2
import psycopg2.extras
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DB_CONFIG

logger = logging.getLogger(__name__)


# ── Schema DDL ─────────────────────────────────────────────────
SCHEMA_SQL = """
-- Jobs master table
CREATE TABLE IF NOT EXISTS jobs (
    id              SERIAL PRIMARY KEY,
    job_id          VARCHAR(64)  UNIQUE NOT NULL,   -- hash fingerprint
    title           TEXT         NOT NULL,
    company         TEXT,
    location        TEXT,
    is_remote       BOOLEAN      DEFAULT FALSE,
    salary_min      NUMERIC(12,2),
    salary_max      NUMERIC(12,2),
    salary_currency VARCHAR(8)   DEFAULT 'USD',
    salary_period   VARCHAR(16),                    -- hourly | annual
    job_type        VARCHAR(32),                    -- full-time | contract | etc.
    seniority       VARCHAR(32),                    -- junior | mid | senior | lead
    description     TEXT,
    url             TEXT,
    source          VARCHAR(32)  NOT NULL,          -- indeed | linkedin | glassdoor
    search_query    TEXT,
    search_location TEXT,
    date_posted     DATE,
    date_scraped    TIMESTAMP    DEFAULT NOW(),
    is_active       BOOLEAN      DEFAULT TRUE
);

-- Skills extracted from descriptions (populated in Phase 3)
CREATE TABLE IF NOT EXISTS job_skills (
    id       SERIAL PRIMARY KEY,
    job_id   VARCHAR(64) REFERENCES jobs(job_id) ON DELETE CASCADE,
    skill    VARCHAR(128) NOT NULL,
    UNIQUE(job_id, skill)
);

-- Daily scrape run log
CREATE TABLE IF NOT EXISTS scrape_runs (
    id            SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT NOW(),
    source        VARCHAR(32),
    query         TEXT,
    location      TEXT,
    jobs_found    INTEGER DEFAULT 0,
    jobs_new      INTEGER DEFAULT 0,
    status        VARCHAR(16) DEFAULT 'running',   -- running | success | failed
    error_msg     TEXT
);

-- Salary normalization staging
CREATE TABLE IF NOT EXISTS salary_staging (
    job_id         VARCHAR(64) PRIMARY KEY,
    raw_salary     TEXT,
    normalized_min NUMERIC(12,2),
    normalized_max NUMERIC(12,2),
    period         VARCHAR(16),
    processed_at   TIMESTAMP DEFAULT NOW()
);

-- Indexes for fast analytical queries
CREATE INDEX IF NOT EXISTS idx_jobs_source       ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_query        ON jobs(search_query);
CREATE INDEX IF NOT EXISTS idx_jobs_date_scraped ON jobs(date_scraped);
CREATE INDEX IF NOT EXISTS idx_jobs_date_posted  ON jobs(date_posted);
CREATE INDEX IF NOT EXISTS idx_jobs_location     ON jobs(location);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill  ON job_skills(skill);
"""


class DatabaseManager:
    """Handles all DB connections, schema setup, and insert operations."""

    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False
            logger.info("✅ Database connected successfully")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def setup_schema(self):
        """Create all tables and indexes if they don't exist."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
            self.conn.commit()
            logger.info("✅ Schema initialized")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Schema setup failed: {e}")
            raise

    def insert_jobs(self, jobs: list[dict]) -> tuple[int, int]:
        """
        Bulk upsert jobs. Returns (total_attempted, new_inserts).
        Uses ON CONFLICT DO NOTHING to skip duplicates by job_id hash.
        """
        if not jobs:
            return 0, 0

        sql = """
            INSERT INTO jobs (
                job_id, title, company, location, is_remote,
                salary_min, salary_max, salary_period, job_type,
                seniority, description, url, source,
                search_query, search_location, date_posted
            ) VALUES (
                %(job_id)s, %(title)s, %(company)s, %(location)s, %(is_remote)s,
                %(salary_min)s, %(salary_max)s, %(salary_period)s, %(job_type)s,
                %(seniority)s, %(description)s, %(url)s, %(source)s,
                %(search_query)s, %(search_location)s, %(date_posted)s
            )
            ON CONFLICT (job_id) DO NOTHING;
        """
        inserted = 0
        try:
            with self.conn.cursor() as cur:
                for job in jobs:
                    cur.execute(sql, job)
                    inserted += cur.rowcount
            self.conn.commit()
            logger.info(f"  ↳ Inserted {inserted}/{len(jobs)} new jobs")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Insert failed: {e}")
            raise

        return len(jobs), inserted

    def log_run(self, source, query, location, jobs_found=0,
                jobs_new=0, status="success", error_msg=None) -> int:
        """Insert a scrape run log entry, return run_id."""
        sql = """
            INSERT INTO scrape_runs (source, query, location, jobs_found, jobs_new, status, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (source, query, location, jobs_found, jobs_new, status, error_msg))
                run_id = cur.fetchone()[0]
            self.conn.commit()
            return run_id
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Failed to log run: {e}")
            return -1

    def get_job_count(self, source=None) -> int:
        sql = "SELECT COUNT(*) FROM jobs" + (" WHERE source=%s" if source else "")
        with self.conn.cursor() as cur:
            cur.execute(sql, (source,) if source else ())
            return cur.fetchone()[0]

    def close(self):
        if self.conn:
            self.conn.close()
