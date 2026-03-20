# dashboard/pages/2_USA_Map.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import get_jobs_by_state, get_distinct_roles, get_distinct_sources

st.set_page_config(page_title="USA Map", page_icon="🗺️", layout="wide")

# US state abbreviation lookup
STATE_ABBREV = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
    'Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA',
    'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA',
    'Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD',
    'Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS',
    'Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV','New Hampshire':'NH',
    'New Jersey':'NJ','New Mexico':'NM','New York':'NY','North Carolina':'NC',
    'North Dakota':'ND','Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Pennsylvania':'PA',
    'Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD','Tennessee':'TN',
    'Texas':'TX','Utah':'UT','Vermont':'VT','Virginia':'VA','Washington':'WA',
    'West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY','District of Columbia':'DC',
    # Abbreviations that come through directly
    'AL':'AL','AK':'AK','AZ':'AZ','AR':'AR','CA':'CA','CO':'CO','CT':'CT',
    'DE':'DE','FL':'FL','GA':'GA','HI':'HI','ID':'ID','IL':'IL','IN':'IN',
    'IA':'IA','KS':'KS','KY':'KY','LA':'LA','ME':'ME','MD':'MD','MA':'MA',
    'MI':'MI','MN':'MN','MS':'MS','MO':'MO','MT':'MT','NE':'NE','NV':'NV',
    'NH':'NH','NJ':'NJ','NM':'NM','NY':'NY','NC':'NC','ND':'ND','OH':'OH',
    'OK':'OK','OR':'OR','PA':'PA','RI':'RI','SC':'SC','SD':'SD','TN':'TN',
    'TX':'TX','UT':'UT','VT':'VT','VA':'VA','WA':'WA','WV':'WV','WI':'WI',
    'WY':'WY','DC':'DC'
}

st.title("USA Jobs Map")
st.caption("Job concentration by state — hover any state for details")

# ── Filters ────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    roles   = ["All roles"] + get_distinct_roles()
    role_f  = st.selectbox("Filter by role", roles)
with c2:
    sources = ["All sources"] + get_distinct_sources()
    src_f   = st.selectbox("Filter by source", sources)

# ── Data ───────────────────────────────────────────────────────
df = get_jobs_by_state()

if df.empty:
    st.warning("No location data yet — run the scraper first.")
    st.stop()

# Normalize state names to abbreviations
df["state"] = df["state_raw"].str.strip().map(STATE_ABBREV)
df = df.dropna(subset=["state"])
df = df.groupby("state", as_index=False).agg(
    job_count=("job_count","sum"),
    avg_salary=("avg_salary","mean")
).sort_values("job_count", ascending=False)
df["avg_salary_fmt"] = df["avg_salary"].apply(
    lambda x: f"${int(x):,}" if pd.notna(x) and x else "N/A"
)

# ── Choropleth Map ─────────────────────────────────────────────
fig = go.Figure(data=go.Choropleth(
    locations    = df["state"],
    z            = df["job_count"],
    locationmode = "USA-states",
    colorscale   = [[0,"#e8f0fe"],[0.3,"#7aa2f7"],[0.7,"#4361ee"],[1,"#1a237e"]],
    colorbar     = dict(title="Jobs", thickness=15, len=0.6),
    text         = df.apply(
        lambda r: f"<b>{r['state']}</b><br>{int(r['job_count'])} jobs<br>Avg salary: {r['avg_salary_fmt']}",
        axis=1
    ),
    hovertemplate="%{text}<extra></extra>",
    marker_line_color="white",
    marker_line_width=0.8,
))
fig.update_layout(
    geo=dict(
        scope="usa", projection_type="albers usa",
        showlakes=True, lakecolor="white",
        bgcolor="white", framecolor="white",
    ),
    height=520, margin=dict(l=0, r=0, t=0, b=0),
    paper_bgcolor="white"
)
st.plotly_chart(fig, use_container_width=True)

# ── State Leaderboard ──────────────────────────────────────────
st.subheader("Top states by job count")
top10 = df.head(10).copy()
top10["avg_salary"] = top10["avg_salary_fmt"]
top10 = top10[["state","job_count","avg_salary"]]
top10.columns = ["State","Job Count","Avg Salary"]
st.dataframe(top10, use_container_width=True, hide_index=True)