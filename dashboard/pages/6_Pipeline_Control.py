# dashboard/pages/6_Pipeline_Control.py
import streamlit as st
import pandas as pd
import json, os, sys, threading, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import get_scrape_history, get_db_stats, clear_cache
from config.config import SEARCH_QUERIES, SEARCH_LOCATIONS

st.set_page_config(page_title="Pipeline Control", page_icon="⚙️", layout="wide")

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config", "scrape_config.json"
)

# ── Load / save scrape config ──────────────────────────────────
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {"queries": SEARCH_QUERIES, "locations": SEARCH_LOCATIONS,
            "schedule_hours": 24, "sources": ["indeed","linkedin"]}

def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

# ── Run pipeline in background thread ─────────────────────────
def run_pipeline_thread(queries, locations, sources):
    try:
        from scrapers.jobspy_scraper import JobSpyScraper
        from pipeline.etl import ETLPipeline
        scraper = JobSpyScraper(sources=sources)
        jobs    = scraper.scrape_all(queries, locations)
        etl     = ETLPipeline()
        stats   = etl.run(jobs, source="multi", query="dashboard-triggered", location="multi")
        etl.close()
        st.session_state["last_run_stats"]  = stats
        st.session_state["last_run_time"]   = datetime.now().strftime("%H:%M:%S")
        st.session_state["pipeline_running"] = False
        clear_cache()
    except Exception as e:
        st.session_state["pipeline_error"]   = str(e)
        st.session_state["pipeline_running"] = False

# ── Page ───────────────────────────────────────────────────────
st.title("Pipeline Control Centre")
st.caption("Manage scraping, scheduling, and configuration — no terminal needed")

cfg = load_config()

# ── Status banner ──────────────────────────────────────────────
if st.session_state.get("pipeline_running"):
    st.info("Scrape in progress... This page will update when done.", icon="⏳")
elif st.session_state.get("last_run_stats"):
    s = st.session_state["last_run_stats"]
    t = st.session_state.get("last_run_time","")
    st.success(
        f"Last run at {t} — "
        f"{s.get('inserted',0)} new jobs inserted, "
        f"{s.get('duplicate',0)} duplicates skipped",
        icon="✅"
    )
    if st.button("Dismiss"):
        del st.session_state["last_run_stats"]
        st.rerun()
elif st.session_state.get("pipeline_error"):
    st.error(f"Last run failed: {st.session_state['pipeline_error']}", icon="❌")
    if st.button("Dismiss error"):
        del st.session_state["pipeline_error"]
        st.rerun()

st.divider()

# ── 3 column layout ────────────────────────────────────────────
left, mid, right = st.columns([1.2, 1.2, 1])

# ── LEFT: Run Controls ─────────────────────────────────────────
with left:
    st.subheader("Run scraper")

    src_opts = st.multiselect(
        "Sources to scrape",
        ["indeed","linkedin","glassdoor"],
        default=cfg.get("sources",["indeed","linkedin"])
    )

    run_disabled = bool(st.session_state.get("pipeline_running"))
    if st.button("Run now", type="primary", disabled=run_disabled,
                 use_container_width=True):
        st.session_state["pipeline_running"] = True
        t = threading.Thread(
            target=run_pipeline_thread,
            args=(cfg["queries"], cfg["locations"], src_opts),
            daemon=True
        )
        t.start()
        st.rerun()

    st.divider()
    st.subheader("DB health")
    db_stats = get_db_stats()
    if not db_stats.empty:
        s = db_stats.iloc[0]
        st.metric("Total jobs",    f"{int(s.get('total_jobs',0)):,}")
        st.metric("Skills indexed",f"{int(s.get('total_skills',0)):,}")
        st.metric("Scrape runs",   f"{int(s.get('total_runs',0)):,}")
        fails = int(s.get("failed_runs",0))
        st.metric("Failed runs",   fails, delta=f"-{fails}" if fails else None,
                  delta_color="inverse")

# ── MID: Config Editor ─────────────────────────────────────────
with mid:
    st.subheader("Job roles to scrape")
    queries_text = st.text_area(
        "One role per line",
        value="\n".join(cfg["queries"]),
        height=200,
        help="These are sent as search queries to Indeed and LinkedIn"
    )

    st.subheader("Locations to scrape")
    locations_text = st.text_area(
        "One location per line",
        value="\n".join(cfg["locations"]),
        height=150,
        help="Use city names, state names, or 'Remote'"
    )

    if st.button("Save configuration", use_container_width=True):
        new_queries   = [q.strip() for q in queries_text.split("\n") if q.strip()]
        new_locations = [l.strip() for l in locations_text.split("\n") if l.strip()]
        if not new_queries:
            st.error("At least one role is required")
        elif not new_locations:
            st.error("At least one location is required")
        else:
            cfg["queries"]   = new_queries
            cfg["locations"] = new_locations
            cfg["sources"]   = src_opts
            save_config(cfg)
            st.success(f"Saved: {len(new_queries)} roles, {len(new_locations)} locations")

# ── RIGHT: Schedule ────────────────────────────────────────────
with right:
    st.subheader("Auto-schedule")

    sched_on = st.toggle(
        "Enable auto-scrape",
        value=st.session_state.get("scheduler_on", False)
    )

    interval = st.select_slider(
        "Run every",
        options=["6 hours","12 hours","24 hours","48 hours"],
        value="24 hours"
    )

    if sched_on and not st.session_state.get("scheduler_on"):
        hours = int(interval.split()[0])
        st.session_state["scheduler_on"]  = True
        st.session_state["schedule_hours"] = hours

        def scheduler_loop():
            while st.session_state.get("scheduler_on"):
                run_pipeline_thread(cfg["queries"], cfg["locations"],
                                    cfg.get("sources",["indeed","linkedin"]))
                time.sleep(hours * 3600)

        t = threading.Thread(target=scheduler_loop, daemon=True)
        t.start()
        st.success(f"Scheduler started — runs every {interval}", icon="⏰")

    elif not sched_on and st.session_state.get("scheduler_on"):
        st.session_state["scheduler_on"] = False
        st.info("Scheduler stopped")

    if st.session_state.get("scheduler_on"):
        h = st.session_state.get("schedule_hours", 24)
        st.success(f"Running every {h}h", icon="✅")

    st.divider()
    st.subheader("Quick add role")
    new_role = st.text_input("New job role", placeholder="e.g. Analytics Engineer")
    if st.button("Add role", use_container_width=True):
        if new_role.strip():
            if new_role.strip() not in cfg["queries"]:
                cfg["queries"].append(new_role.strip())
                save_config(cfg)
                st.success(f"Added: {new_role}")
                st.rerun()
            else:
                st.warning("Role already exists")

    new_loc = st.text_input("New location", placeholder="e.g. Chicago, IL")
    if st.button("Add location", use_container_width=True):
        if new_loc.strip():
            if new_loc.strip() not in cfg["locations"]:
                cfg["locations"].append(new_loc.strip())
                save_config(cfg)
                st.success(f"Added: {new_loc}")
                st.rerun()
            else:
                st.warning("Location already exists")

# ── Scrape History Log ─────────────────────────────────────────
st.divider()
st.subheader("Scrape history")

df_hist = get_scrape_history(30)
if df_hist.empty:
    st.info("No scrape runs logged yet.")
else:
    def color_status(val):
        if val == "success": return "background-color: #d4edda; color: #155724"
        if val == "failed":  return "background-color: #f8d7da; color: #721c24"
        return ""

    styled = df_hist.style.map(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, hide_index=True, height=300)