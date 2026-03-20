# 🧠 Job Market Intelligence Engine
### Phase 1 — Automated Scraping Pipeline

---

## Project Structure

```
job_market/
├── config/
│   └── config.py           ← Search queries, locations, DB config, delays
├── scrapers/
│   └── indeed_scraper.py   ← Indeed scraper (requests + BeautifulSoup)
├── pipeline/
│   ├── database.py         ← PostgreSQL schema + insert/log manager
│   ├── etl.py              ← Validate → clean → deduplicate → load
│   └── utils.py            ← Salary norm, seniority, remote, date parsing
├── scheduler/
│   └── runner.py           ← Manual run or daily APScheduler trigger
├── data/
│   ├── raw/                ← Raw HTML per scrape page (for debugging)
│   └── processed/          ← Cleaned JSON batches (audit trail)
├── logs/                   ← Daily log files
└── test_pipeline.py        ← 33 unit tests (no DB needed)
```

---

## Setup

### 1. Install dependencies
```bash
pip install selenium beautifulsoup4 requests pandas psycopg2-binary \
            apscheduler lxml fake-useragent
```

### 2. Set up PostgreSQL
```bash
createdb job_market
```
Or configure via environment variables:
```bash
export DB_HOST=localhost
export DB_NAME=job_market
export DB_USER=postgres
export DB_PASSWORD=yourpassword
```
Schema is auto-created on first run.

### 3. Run tests first
```bash
python test_pipeline.py
```

### 4. Run the pipeline once
```bash
python scheduler/runner.py --mode run
```

### 5. Start daily scheduler (6AM every day)
```bash
python scheduler/runner.py --mode schedule
```

---

## What Gets Scraped (per run)

| Field         | Source         | Notes                              |
|---------------|----------------|------------------------------------|
| Job title     | Card heading   | Cleaned, whitespace-normalized     |
| Company       | Card metadata  | Title-cased                        |
| Location      | Card metadata  | Includes "Remote" detection        |
| Salary        | Card snippet   | Normalized to annual USD           |
| Date posted   | Card footer    | Converted from relative → absolute |
| Description   | Card snippet   | Truncated at 10,000 chars          |
| Seniority     | Derived        | From title via regex rules         |
| Is remote     | Derived        | From location + title + desc       |
| Job ID        | Derived        | MD5 hash for deduplication         |

---

## Configuration (config/config.py)

```python
SEARCH_QUERIES   = ["Data Engineer", "Data Analyst", ...]
SEARCH_LOCATIONS = ["Remote", "New York, NY", ...]
MAX_PAGES_PER_QUERY = 5      # ~75 jobs per query/location combo
REQUEST_DELAY_MIN   = 2.5    # polite scraping delays (seconds)
SCRAPE_HOUR         = 6      # daily run at 6:00 AM
```

---

## Database Schema

- **`jobs`** — master table, 17 columns, unique on `job_id` hash
- **`job_skills`** — extracted skills (populated in Phase 3)
- **`scrape_runs`** — audit log per scrape run
- **`salary_staging`** — normalization staging table

---

## Next Steps (Phases 2–5)

- [ ] Phase 2 — Add LinkedIn + Glassdoor scrapers
- [ ] Phase 3 — spaCy NER skill extractor on `description`
- [ ] Phase 4 — Salary prediction model (XGBoost)
- [ ] Phase 5 — Streamlit dashboard + FastAPI endpoints
