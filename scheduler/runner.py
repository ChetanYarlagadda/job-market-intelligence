# scheduler/runner.py
# ─────────────────────────────────────────────
# Daily Pipeline Runner
# Scrapes Indeed + LinkedIn via python-jobspy,
# runs ETL, and loads into PostgreSQL.
# ─────────────────────────────────────────────

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.jobspy_scraper import JobSpyScraper
from pipeline.etl import ETLPipeline
from pipeline.utils import setup_logger
from config.config import (
    SEARCH_QUERIES, SEARCH_LOCATIONS,
    SCRAPE_HOUR, SCRAPE_MINUTE
)

logger = setup_logger("scheduler")


def run_full_pipeline():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Pipeline run started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    etl   = ETLPipeline()
    stats = {"total_raw": 0, "valid": 0, "inserted": 0, "duplicate": 0}

    try:
        logger.info("Source: Indeed + LinkedIn (via python-jobspy)")
        scraper = JobSpyScraper(sources=["indeed", "linkedin"])
        jobs    = scraper.scrape_all(SEARCH_QUERIES, SEARCH_LOCATIONS)

        run_stats = etl.run(
            jobs,
            source   = "multi",
            query    = "multi-query",
            location = "multi-location"
        )
        for k in stats:
            stats[k] += run_stats.get(k, 0)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)

    elapsed = (datetime.now() - start_time).seconds
    logger.info("=" * 60)
    logger.info(f"Pipeline complete in {elapsed}s")
    logger.info(f"  Raw scraped : {stats['total_raw']}")
    logger.info(f"  Valid jobs  : {stats['valid']}")
    logger.info(f"  New inserts : {stats['inserted']}")
    logger.info(f"  Duplicates  : {stats['duplicate']}")
    logger.info("=" * 60)

    etl.close()
    return stats


def start_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_full_pipeline,
        trigger = "cron",
        hour    = SCRAPE_HOUR,
        minute  = SCRAPE_MINUTE,
        id      = "daily_scrape",
    )
    logger.info(f"Scheduler started: daily at {SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d}")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["run", "schedule"], default="run")
    args = parser.parse_args()

    if args.mode == "schedule":
        start_scheduler()
    else:
        run_full_pipeline()