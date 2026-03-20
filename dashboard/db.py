# dashboard/db.py — All DB queries for the dashboard
import sys, os, pandas as pd
import streamlit as st
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psycopg2
    from config.config import DB_CONFIG
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def query(sql, params=None):
    try:
        conn = get_connection()
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_summary_stats():
    return query("""
        SELECT COUNT(*) AS total_jobs,
               COUNT(DISTINCT company) AS total_companies,
               ROUND(AVG((salary_min+salary_max)/2)) AS avg_salary,
               ROUND(100.0*SUM(is_remote::int)/COUNT(*),1) AS pct_remote,
               MAX(date_scraped)::date AS last_scraped,
               COUNT(CASE WHEN date_scraped >= NOW()-INTERVAL '24 hours' THEN 1 END) AS scraped_today
        FROM jobs WHERE is_active=TRUE
    """)

@st.cache_data(ttl=300)
def get_jobs_over_time():
    return query("""
        SELECT date_scraped::date AS day, COUNT(*) AS count
        FROM jobs WHERE date_scraped >= NOW()-INTERVAL '30 days'
        GROUP BY day ORDER BY day
    """)

@st.cache_data(ttl=300)
def get_top_roles(n=10):
    return query(f"""
        SELECT search_query AS role, COUNT(*) AS count
        FROM jobs WHERE is_active=TRUE
        GROUP BY search_query ORDER BY count DESC LIMIT {n}
    """)

@st.cache_data(ttl=300)
def get_top_companies(n=10):
    return query(f"""
        SELECT company, COUNT(*) AS job_count,
               ROUND(AVG((salary_min+salary_max)/2)) AS avg_salary
        FROM jobs WHERE is_active=TRUE AND company!='Unknown'
        GROUP BY company ORDER BY job_count DESC LIMIT {n}
    """)

@st.cache_data(ttl=300)
def get_jobs_by_state():
    return query("""
        SELECT TRIM(SPLIT_PART(location,',',2)) AS state_raw,
               COUNT(*) AS job_count,
               ROUND(AVG((salary_min+salary_max)/2)) AS avg_salary
        FROM jobs
        WHERE is_active=TRUE AND location IS NOT NULL
          AND location NOT ILIKE '%remote%' AND location LIKE '%,%'
        GROUP BY state_raw HAVING COUNT(*)>0
    """)

@st.cache_data(ttl=300)
def get_salary_by_role():
    return query("""
        SELECT search_query AS role, salary_min, salary_max,
               (salary_min+salary_max)/2 AS salary_mid,
               seniority, location, company
        FROM jobs
        WHERE is_active=TRUE AND salary_min IS NOT NULL
          AND salary_max IS NOT NULL AND salary_max < 500000
    """)

@st.cache_data(ttl=300)
def get_salary_by_seniority():
    return query("""
        SELECT seniority, ROUND(AVG((salary_min+salary_max)/2)) AS avg_salary, COUNT(*) AS count
        FROM jobs WHERE is_active=TRUE AND salary_min IS NOT NULL AND seniority IS NOT NULL
        GROUP BY seniority ORDER BY avg_salary DESC
    """)

@st.cache_data(ttl=300)
def get_top_skills(n=25):
    return query(f"""
        SELECT skill, COUNT(*) AS count FROM job_skills
        GROUP BY skill ORDER BY count DESC LIMIT {n}
    """)

@st.cache_data(ttl=300)
def get_skills_over_time(top_n=8):
    return query(f"""
        WITH top_skills AS (SELECT skill FROM job_skills GROUP BY skill ORDER BY COUNT(*) DESC LIMIT {top_n})
        SELECT DATE_TRUNC('week',j.date_scraped) AS week, js.skill, COUNT(*) AS count
        FROM job_skills js JOIN jobs j ON j.job_id=js.job_id
        WHERE js.skill IN (SELECT skill FROM top_skills)
        GROUP BY week,js.skill ORDER BY week,js.skill
    """)

@st.cache_data(ttl=60)
def get_all_jobs(role=None, location=None, source=None,
                 remote_only=False, min_sal=None, max_sal=None, days_back=30, limit=500):
    conds = ["is_active=TRUE", f"date_scraped >= NOW()-INTERVAL '{days_back} days'"]
    params = []
    if role:    conds.append("search_query=%s"); params.append(role)
    if location: conds.append("location ILIKE %s"); params.append(f"%{location}%")
    if source:  conds.append("source=%s"); params.append(source)
    if remote_only: conds.append("is_remote=TRUE")
    if min_sal: conds.append("salary_min>=%s"); params.append(min_sal)
    if max_sal: conds.append("salary_max<=%s"); params.append(max_sal)
    sql = f"""
        SELECT title,company,location,is_remote,salary_min,salary_max,
               seniority,source,date_posted,url,search_query,date_scraped
        FROM jobs WHERE {" AND ".join(conds)}
        ORDER BY date_scraped DESC LIMIT {limit}
    """
    return query(sql, params or None)

@st.cache_data(ttl=60)
def get_scrape_history(n=20):
    return query(f"""
        SELECT run_timestamp::date AS date, run_timestamp::time(0) AS time,
               source,query,location,jobs_found,jobs_new,status,error_msg
        FROM scrape_runs ORDER BY run_timestamp DESC LIMIT {n}
    """)

@st.cache_data(ttl=300)
def get_distinct_roles():
    df = query("SELECT DISTINCT search_query FROM jobs WHERE is_active=TRUE ORDER BY search_query")
    return df["search_query"].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_distinct_sources():
    df = query("SELECT DISTINCT source FROM jobs WHERE is_active=TRUE ORDER BY source")
    return df["source"].tolist() if not df.empty else []

def get_db_stats():
    return query("""
        SELECT (SELECT COUNT(*) FROM jobs) AS total_jobs,
               (SELECT COUNT(*) FROM job_skills) AS total_skills,
               (SELECT COUNT(*) FROM scrape_runs) AS total_runs,
               (SELECT COUNT(*) FROM scrape_runs WHERE status='failed') AS failed_runs
    """)

def clear_cache():
    st.cache_data.clear()