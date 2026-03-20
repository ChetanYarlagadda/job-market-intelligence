# dashboard/pages/5_Job_Explorer.py
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import get_all_jobs, get_distinct_roles, get_distinct_sources

st.set_page_config(page_title="Job Explorer", page_icon="🔍", layout="wide")
st.title("Job Explorer")
st.caption("Search and filter all scraped job listings")

# ── Filters Sidebar ────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    roles   = ["All"] + get_distinct_roles()
    sources = ["All"] + get_distinct_sources()

    role_f    = st.selectbox("Job role", roles)
    source_f  = st.selectbox("Source", sources)
    location_f = st.text_input("Location contains", placeholder="e.g. New York")
    remote_f  = st.checkbox("Remote only")
    days_f    = st.slider("Posted in last N days", 1, 90, 30)

    st.subheader("Salary range ($)")
    sal_min = st.number_input("Min salary", value=0, step=10000)
    sal_max = st.number_input("Max salary", value=300000, step=10000)

    if st.button("Clear filters", use_container_width=True):
        st.rerun()

# ── Load Data ──────────────────────────────────────────────────
df = get_all_jobs(
    role       = None if role_f == "All" else role_f,
    location   = location_f or None,
    source     = None if source_f == "All" else source_f,
    remote_only= remote_f,
    min_sal    = sal_min if sal_min > 0 else None,
    max_sal    = sal_max if sal_max < 300000 else None,
    days_back  = days_f,
    limit      = 1000
)

if df.empty:
    st.info("No jobs match your filters. Try widening the search.")
    st.stop()

# ── Search bar ─────────────────────────────────────────────────
search = st.text_input("Search by title or company", placeholder="e.g. Senior Data Engineer")
if search:
    mask = (
        df["title"].str.contains(search, case=False, na=False) |
        df["company"].str.contains(search, case=False, na=False)
    )
    df = df[mask]

st.caption(f"Showing {len(df):,} jobs")

# ── Format for display ─────────────────────────────────────────
display = df.copy()
display["salary"] = display.apply(
    lambda r: f"${int(r.salary_min):,} – ${int(r.salary_max):,}"
              if pd.notna(r.salary_min) and pd.notna(r.salary_max) else "—",
    axis=1
)
display["remote"] = display["is_remote"].map({True: "Remote", False: "Onsite"})
display["date"]   = pd.to_datetime(display["date_posted"]).dt.strftime("%b %d, %Y")

show_cols = ["title","company","location","remote","salary","seniority","source","date"]
display   = display[show_cols].rename(columns={
    "title":"Title","company":"Company","location":"Location",
    "remote":"Type","salary":"Salary","seniority":"Level",
    "source":"Source","date":"Posted"
})

# ── Table ──────────────────────────────────────────────────────
st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    height=520,
    column_config={
        "Title":    st.column_config.TextColumn(width="large"),
        "Company":  st.column_config.TextColumn(width="medium"),
        "Salary":   st.column_config.TextColumn(width="medium"),
        "Location": st.column_config.TextColumn(width="medium"),
    }
)

# ── Export ─────────────────────────────────────────────────────
csv = display.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download as CSV", csv, "jobs_export.csv", "text/csv",
    use_container_width=False
)