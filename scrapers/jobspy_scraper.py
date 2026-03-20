# scrapers/jobspy_scraper.py
# ─────────────────────────────────────────────
# Multi-source Job Scraper using python-jobspy
# Handles Indeed + LinkedIn + Glassdoor with built-in
# bot bypass (TLS fingerprint spoofing, rotating headers)
# No manual anti-bot logic needed!
#
# Install: pip install python-jobspy
# ─────────────────────────────────────────────

import os
import sys
import time
import random
import json
import warnings
from datetime import datetime, date
from typing import Optional
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    REQUEST_DELAY_MIN, REQUEST_DELAY_MAX,
    MAX_PAGES_PER_QUERY, RAW_DATA_DIR
)
from pipeline.utils import (
    setup_logger, make_job_id, normalize_salary,
    detect_seniority, detect_remote, clean_text
)

logger = setup_logger("jobspy_scraper")

# Results per search call (jobspy handles pagination internally)
RESULTS_PER_QUERY = 50
HOURS_OLD         = 72    # Only jobs posted in last 3 days per run


class JobSpyScraper:
    """
    Wraps python-jobspy to scrape Indeed, LinkedIn, Glassdoor.
    jobspy handles all bot detection internally using TLS spoofing.

    Usage:
        scraper = JobSpyScraper(sources=["indeed", "linkedin"])
        jobs = scraper.scrape_all(queries, locations)
    """

    def __init__(self, sources: list = None):
        self.sources = sources or ["indeed", "linkedin"]
        try:
            from jobspy import scrape_jobs
            self._scrape_jobs = scrape_jobs
            logger.info(f"JobSpy loaded | sources: {self.sources}")
        except ImportError:
            raise ImportError("Run: pip install python-jobspy")

    def _sleep(self):
        t = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        logger.debug(f"  Sleeping {t:.1f}s...")
        time.sleep(t)

    def _scrape_one(self, query: str, location: str) -> pd.DataFrame:
        """Run jobspy for a single (query, location) pair."""
        logger.info(f"  Scraping '{query}' in '{location}'...")
        try:
            df = self._scrape_jobs(
                site_name        = self.sources,
                search_term      = query,
                location         = location,
                results_wanted   = RESULTS_PER_QUERY,
                hours_old        = HOURS_OLD,
                country_indeed   = "USA",
                verbose          = 0,
            )
            logger.info(f"    Got {len(df)} results")
            return df
        except Exception as e:
            logger.error(f"    jobspy error: {e}")
            return pd.DataFrame()

    def _normalize_row(self, row: pd.Series, query: str, location: str) -> Optional[dict]:
        """Convert a jobspy DataFrame row into our standard job dict."""
        try:
            title   = clean_text(str(row.get("title", "") or ""))
            company = clean_text(str(row.get("company", "") or "Unknown"))
            loc     = clean_text(str(row.get("location", "") or location))
            source  = str(row.get("site", "indeed")).lower()

            if not title or title == "nan":
                return None

            # Salary — jobspy already parses min/max amounts
            sal_min = row.get("min_amount")
            sal_max = row.get("max_amount")
            interval = str(row.get("interval", "") or "").lower()

            # Annualize if needed
            multipliers = {"hourly": 2080, "monthly": 12, "weekly": 52}
            mult = multipliers.get(interval, 1)
            if mult > 1:
                sal_min = round(float(sal_min) * mult, 2) if pd.notna(sal_min) else None
                sal_max = round(float(sal_max) * mult, 2) if pd.notna(sal_max) else None
            else:
                sal_min = float(sal_min) if pd.notna(sal_min) else None
                sal_max = float(sal_max) if pd.notna(sal_max) else None

            # Date posted
            dp = row.get("date_posted")
            if pd.notna(dp) and dp is not None:
                if isinstance(dp, (datetime, date)):
                    date_posted = dp if isinstance(dp, date) else dp.date()
                else:
                    try:
                        date_posted = pd.to_datetime(dp).date()
                    except:
                        date_posted = date.today()
            else:
                date_posted = date.today()

            # Remote
            is_remote = bool(row.get("is_remote", False))
            if not is_remote:
                is_remote = detect_remote(loc, title)

            # Description
            desc = clean_text(str(row.get("description", "") or ""))
            if desc == "nan":
                desc = None
            if desc and len(desc) > 10000:
                desc = desc[:10000]

            # Job level / seniority
            job_level = str(row.get("job_level", "") or "")
            seniority = job_level.lower() if job_level and job_level != "nan" else detect_seniority(title)

            # Job type
            job_type = str(row.get("job_type", "") or "")
            if job_type == "nan":
                job_type = None

            return {
                "job_id":          make_job_id(title, company, loc, source),
                "title":           title,
                "company":         company,
                "location":        loc,
                "is_remote":       is_remote,
                "salary_min":      sal_min,
                "salary_max":      sal_max,
                "salary_period":   interval if interval else "annual",
                "job_type":        job_type,
                "seniority":       seniority,
                "description":     desc,
                "url":             str(row.get("job_url", "") or ""),
                "source":          source,
                "search_query":    query,
                "search_location": location,
                "date_posted":     date_posted,
            }
        except Exception as e:
            logger.debug(f"  Row normalize error: {e}")
            return None

    def _save_raw(self, df: pd.DataFrame, query: str, location: str):
        """Save raw DataFrame as JSON for audit trail."""
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe  = lambda s: s.replace(" ", "_").replace(",", "").replace("/", "-")
        fname = f"raw_{safe(query)}_{safe(location)}_{ts}.json"
        try:
            df.to_json(
                os.path.join(RAW_DATA_DIR, fname),
                orient="records", date_format="iso", indent=2
            )
        except Exception as e:
            logger.debug(f"  Raw save error: {e}")

    def scrape_all(self, queries: list, locations: list) -> list[dict]:
        """
        Scrape all (query x location) combinations.
        Returns list of normalized, deduplicated job dicts.
        """
        all_jobs = []
        seen_ids = set()
        total_combos = len(queries) * len(locations)
        combo_num    = 0

        for query in queries:
            for location in locations:
                combo_num += 1
                logger.info(f"[{combo_num}/{total_combos}] {query} | {location}")

                df = self._scrape_one(query, location)

                if df.empty:
                    logger.warning("  No results returned")
                    self._sleep()
                    continue

                self._save_raw(df, query, location)

                # Normalize each row
                batch_new = 0
                for _, row in df.iterrows():
                    job = self._normalize_row(row, query, location)
                    if job and job["job_id"] not in seen_ids:
                        seen_ids.add(job["job_id"])
                        all_jobs.append(job)
                        batch_new += 1

                logger.info(f"  +{batch_new} new unique jobs (total so far: {len(all_jobs)})")
                self._sleep()

        logger.info(f"Scrape complete: {len(all_jobs)} unique jobs from {total_combos} searches")
        return all_jobs