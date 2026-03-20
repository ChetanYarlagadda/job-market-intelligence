# dashboard/pages/1_Home.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import (get_summary_stats, get_jobs_over_time,
                           get_top_roles, get_top_companies, clear_cache)

st.set_page_config(page_title="Home", page_icon="🏠", layout="wide")

st.markdown("""
<style>
.metric-card {
    background: #f8f9fa; border-radius: 12px; padding: 20px 24px;
    border: 1px solid #e9ecef;
}
.metric-val { font-size: 32px; font-weight: 700; color: #1a1a2e; margin: 0; }
.metric-lbl { font-size: 13px; color: #6c757d; margin: 4px 0 0; }
.metric-delta { font-size: 12px; color: #28a745; margin: 4px 0 0; }
.section-head { font-size: 18px; font-weight: 600; color: #1a1a2e; margin: 2rem 0 1rem; }
</style>
""", unsafe_allow_html=True)

# ── Header ──────────────────────────────────────────────────────
col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.title("Job Market Intelligence")
    st.caption("Real-time data scraped from Indeed & LinkedIn — updated daily")
with col_refresh:
    st.markdown("<div style='margin-top:20px'>", unsafe_allow_html=True)
    if st.button("Refresh data", use_container_width=True):
        clear_cache()
        st.rerun()

st.divider()

# ── KPI Cards ──────────────────────────────────────────────────
stats = get_summary_stats()

if stats.empty:
    st.info("No data yet — run the scraper first from the Pipeline Control page.")
    st.stop()

s = stats.iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)

def metric_card(col, value, label, delta=None):
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <p class="metric-val">{value}</p>
            <p class="metric-lbl">{label}</p>
            {f'<p class="metric-delta">+{delta} today</p>' if delta else ''}
        </div>
        """, unsafe_allow_html=True)

total   = f"{int(s.get('total_jobs', 0)):,}"
cos     = f"{int(s.get('total_companies', 0)):,}"
sal     = f"${int(s.get('avg_salary', 0) or 0):,}" if s.get('avg_salary') else "N/A"
remote  = f"{s.get('pct_remote', 0) or 0:.1f}%"
scraped = str(s.get('last_scraped', 'Never'))
today   = int(s.get('scraped_today', 0))

metric_card(c1, total,   "Total jobs",       today)
metric_card(c2, cos,     "Companies hiring")
metric_card(c3, sal,     "Avg salary (annual)")
metric_card(c4, remote,  "Remote roles")
metric_card(c5, scraped, "Last scraped")

st.markdown("<br>", unsafe_allow_html=True)

# ── Charts Row ─────────────────────────────────────────────────
left, right = st.columns([3, 2])

with left:
    st.markdown('<p class="section-head">Jobs scraped — last 30 days</p>', unsafe_allow_html=True)
    df_time = get_jobs_over_time()
    if not df_time.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_time["day"], y=df_time["count"],
            mode="lines+markers",
            line=dict(color="#4361ee", width=2.5),
            marker=dict(size=6, color="#4361ee"),
            fill="tozeroy", fillcolor="rgba(67,97,238,0.08)",
            hovertemplate="<b>%{x}</b><br>%{y} jobs<extra></extra>"
        ))
        fig.update_layout(
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, showline=True, linecolor="#dee2e6"),
            yaxis=dict(showgrid=True, gridcolor="#f1f3f5", zeroline=False),
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data yet.")

with right:
    st.markdown('<p class="section-head">Jobs by role</p>', unsafe_allow_html=True)
    df_roles = get_top_roles(8)
    if not df_roles.empty:
        fig = px.bar(
            df_roles, x="count", y="role", orientation="h",
            color="count", color_continuous_scale=["#c8d8ff", "#4361ee"],
        )
        fig.update_layout(
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white", paper_bgcolor="white",
            showlegend=False, coloraxis_showscale=False,
            xaxis_title="", yaxis_title=""
        )
        fig.update_traces(hovertemplate="<b>%{y}</b>: %{x} jobs<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

# ── Top Companies ──────────────────────────────────────────────
st.markdown('<p class="section-head">Top hiring companies</p>', unsafe_allow_html=True)
df_cos = get_top_companies(10)
if not df_cos.empty:
    df_cos["avg_salary"] = df_cos["avg_salary"].apply(
        lambda x: f"${int(x):,}" if pd.notna(x) and x else "—"
    )
    df_cos.columns = ["Company", "Open Roles", "Avg Salary"]
    st.dataframe(df_cos, use_container_width=True, hide_index=True)