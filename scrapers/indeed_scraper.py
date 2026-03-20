# scrapers/indeed_scraper.py
# ─────────────────────────────────────────────
# Indeed Job Scraper
# Uses curl_cffi to mimic real browser TLS fingerprint (bypasses 403).
# Falls back to requests if curl_cffi unavailable.
# ─────────────────────────────────────────────

import time
import random
import json
import os
import sys
from datetime import datetime
from typing import Optional
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    USER_AGENTS, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX,
    MAX_PAGES_PER_QUERY, RAW_DATA_DIR
)
from pipeline.utils import (
    setup_logger, make_job_id, normalize_salary,
    detect_seniority, detect_remote, parse_posted_date, clean_text
)

logger = setup_logger("indeed_scraper")

INDEED_BASE   = "https://www.indeed.com"
INDEED_SEARCH = "https://www.indeed.com/jobs"

# Try curl_cffi first (best anti-bot bypass), fall back to requests
try:
    from curl_cffi import requests as curl_requests
    USE_CURL = True
    logger.info("Using curl_cffi (browser TLS fingerprint mode)")
except ImportError:
    import requests as std_requests
    USE_CURL = False
    logger.info("curl_cffi not found, using standard requests")


class IndeedScraper:

    def __init__(self):
        if USE_CURL:
            self.session = curl_requests.Session(impersonate="chrome120")
        else:
            import requests
            self.session = requests.Session()
        self._set_headers()

    def _set_headers(self):
        self.session.headers.update({
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT":             "1",
            "Connection":      "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer":         "https://www.google.com/",
        })

    def _sleep(self, extra=False):
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        if extra:
            delay += random.uniform(3, 8)
        logger.debug(f"  Sleeping {delay:.1f}s...")
        time.sleep(delay)

    def _fetch_page(self, url: str, params: dict = None) -> Optional[BeautifulSoup]:
        try:
            self._set_headers()
            if USE_CURL:
                resp = self.session.get(url, params=params, timeout=20, impersonate="chrome120")
            else:
                resp = self.session.get(url, params=params, timeout=20)

            if resp.status_code == 403:
                logger.warning("  403 Forbidden - bot detection. Sleeping 60s then retrying once...")
                time.sleep(60)
                # One retry with longer delay
                if USE_CURL:
                    resp = self.session.get(url, params=params, timeout=20, impersonate="chrome110")
                else:
                    resp = self.session.get(url, params=params, timeout=20)
                if resp.status_code != 200:
                    logger.error(f"  Retry failed with status {resp.status_code}. Skipping.")
                    return None

            if resp.status_code != 200:
                logger.warning(f"  HTTP {resp.status_code} for {url}")
                return None

            return BeautifulSoup(resp.text, "lxml")

        except Exception as e:
            logger.error(f"  Fetch error: {e}")
            return None

    def _parse_job_card(self, card, query: str, location: str) -> Optional[dict]:
        try:
            # Title
            title_el = (
                card.find("h2", class_=lambda c: c and "jobTitle" in c) or
                card.find("a", {"data-testid": "job-title"}) or
                card.find("span", {"title": True})
            )
            title = clean_text(title_el.get_text()) if title_el else None
            if not title:
                return None

            # Company
            company_el = (
                card.find("span", {"data-testid": "company-name"}) or
                card.find("span", class_=lambda c: c and "company" in str(c).lower())
            )
            company = clean_text(company_el.get_text()) if company_el else "Unknown"

            # Location
            loc_el = (
                card.find("div", {"data-testid": "text-location"}) or
                card.find("div", class_=lambda c: c and "location" in str(c).lower())
            )
            job_location = clean_text(loc_el.get_text()) if loc_el else location

            # Salary
            salary_el = (
                card.find("div", {"data-testid": "attribute_snippet_testid"}) or
                card.find("div", class_=lambda c: c and "salary" in str(c).lower()) or
                card.find("span", class_=lambda c: c and "salary" in str(c).lower())
            )
            raw_salary  = clean_text(salary_el.get_text()) if salary_el else None
            salary_data = normalize_salary(raw_salary)

            # Date
            date_el  = card.find("span", class_=lambda c: c and "date" in str(c).lower())
            raw_date = clean_text(date_el.get_text()) if date_el else None

            # URL
            link_el = card.find("a", href=True)
            url = (INDEED_BASE + link_el["href"]
                   if link_el and link_el["href"].startswith("/")
                   else (link_el["href"] if link_el else None))

            # Description snippet
            snippet_el = (
                card.find("div", class_=lambda c: c and "snippet" in str(c).lower()) or
                card.find("ul",  class_=lambda c: c and "snippet" in str(c).lower())
            )
            description = clean_text(snippet_el.get_text()) if snippet_el else None

            return {
                "job_id":          make_job_id(title, company, job_location, "indeed"),
                "title":           title,
                "company":         company,
                "location":        job_location,
                "is_remote":       detect_remote(job_location, title),
                "salary_min":      salary_data["salary_min"],
                "salary_max":      salary_data["salary_max"],
                "salary_period":   salary_data["salary_period"],
                "job_type":        None,
                "seniority":       detect_seniority(title),
                "description":     description,
                "url":             url,
                "source":          "indeed",
                "search_query":    query,
                "search_location": location,
                "date_posted":     parse_posted_date(raw_date),
            }
        except Exception as e:
            logger.debug(f"  Card parse error: {e}")
            return None

    def _save_raw(self, html: str, query: str, location: str, page: int):
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe  = lambda s: s.replace(' ', '_').replace(',', '').replace('/', '-')
        fname = f"indeed_{safe(query)}_{safe(location)}_p{page}_{ts}.html"
        with open(os.path.join(RAW_DATA_DIR, fname), "w", encoding="utf-8") as f:
            f.write(html)

    def scrape(self, query: str, location: str) -> list[dict]:
        logger.info(f"Scraping Indeed | '{query}' in '{location}'")
        all_jobs = []
        seen_ids = set()

        for page in range(MAX_PAGES_PER_QUERY):
            start  = page * 15
            params = {
                "q": query, "l": location,
                "start": start, "sort": "date", "fromage": "14",
            }

            logger.info(f"  Page {page + 1}/{MAX_PAGES_PER_QUERY} (start={start})")
            soup = self._fetch_page(INDEED_SEARCH, params=params)

            if soup is None:
                logger.warning("  Skipping page - fetch failed")
                break

            self._save_raw(str(soup), query, location, page)

            # Try multiple card selectors (Indeed changes HTML frequently)
            cards = (
                soup.find_all("div", class_=lambda c: c and "job_seen_beacon" in str(c)) or
                soup.find_all("div", class_=lambda c: c and "tapItem" in str(c)) or
                soup.find_all("li",  class_=lambda c: c and "jobCard" in str(c)) or
                soup.find_all("div", attrs={"data-testid": "slider_item"}) or
                soup.find_all("div", class_=lambda c: c and "result" in str(c).lower() and "job" in str(c).lower())
            )

            if not cards:
                page_text = str(soup).lower()
                if "captcha" in page_text or "robot" in page_text or "unusual traffic" in page_text:
                    logger.error("  Bot detection / CAPTCHA triggered. Stopping.")
                    break
                logger.warning("  No job cards found - Indeed HTML structure may have changed")
                break

            page_jobs = 0
            for card in cards:
                job = self._parse_job_card(card, query, location)
                if job and job["job_id"] not in seen_ids:
                    seen_ids.add(job["job_id"])
                    all_jobs.append(job)
                    page_jobs += 1

            logger.info(f"  Found {page_jobs} jobs on page {page + 1}")

            if len(cards) < 10:
                break

            self._sleep()

        logger.info(f"  Total: {len(all_jobs)} jobs for '{query}' / '{location}'")
        return all_jobs

    def scrape_multiple(self, queries: list, locations: list) -> list[dict]:
        all_jobs = []
        seen_ids = set()

        for query in queries:
            for location in locations:
                jobs = self.scrape(query, location)
                for job in jobs:
                    if job["job_id"] not in seen_ids:
                        seen_ids.add(job["job_id"])
                        all_jobs.append(job)
                self._sleep(extra=True)

        logger.info(f"Indeed scrape complete - {len(all_jobs)} unique jobs total")
        return all_jobs