# dashboard/pages/4_Skills_Demand.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import get_top_skills, get_skills_over_time

st.set_page_config(page_title="Skills Demand", page_icon="🧠", layout="wide")
st.title("Skills Demand Tracker")
st.caption("Most in-demand skills extracted from job descriptions")

st.info("Skills data is populated after running the NLP skill extractor (Phase 3). "
        "Once spaCy is set up, skills are auto-extracted after every scrape.", icon="ℹ️")

df_skills = get_top_skills(25)

if df_skills.empty:
    st.warning("No skills data yet. Complete Phase 3 (spaCy NER) to populate this page.")

    # Show placeholder UI
    st.subheader("Preview: what this page will show")
    sample = pd.DataFrame({
        "skill": ["Python","SQL","Spark","dbt","Airflow","AWS","Kubernetes",
                  "Kafka","Snowflake","Pandas","Docker","Terraform","PySpark","Git","Azure"],
        "count": [420,380,290,240,210,195,170,155,140,130,120,110,100,95,88]
    })
    fig = px.bar(sample.head(15), x="count", y="skill", orientation="h",
                 color="count", color_continuous_scale=["#c8d8ff","#4361ee"],
                 labels={"count":"Job postings","skill":""})
    fig.update_layout(height=420, plot_bgcolor="white", paper_bgcolor="white",
                      coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Sample data shown — run spaCy NER to see real skill counts")
    st.stop()

# ── Top Skills Bar Chart ───────────────────────────────────────
col1, col2 = st.columns([3,2])

with col1:
    st.subheader("Top 25 skills this week")
    fig = px.bar(
        df_skills.sort_values("count"), x="count", y="skill",
        orientation="h", color="count",
        color_continuous_scale=["#c8d8ff","#4361ee"],
        labels={"count":"Mentions","skill":""}
    )
    fig.update_layout(
        height=600, plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=0),
        xaxis=dict(gridcolor="#f1f3f5"), yaxis=dict(showgrid=False)
    )
    fig.update_traces(hovertemplate="<b>%{y}</b>: %{x} mentions<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Skill breakdown")
    fig2 = px.pie(
        df_skills.head(10), names="skill", values="count",
        color_discrete_sequence=px.colors.sequential.Blues_r,
        hole=0.45
    )
    fig2.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0))
    fig2.update_traces(textposition="inside", textinfo="percent",
                       hovertemplate="<b>%{label}</b>: %{value} jobs<extra></extra>")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Top 10 skills")
    st.dataframe(
        df_skills.head(10).rename(columns={"skill":"Skill","count":"Mentions"}),
        use_container_width=True, hide_index=True
    )

# ── Skill Trend Over Time ──────────────────────────────────────
st.subheader("Skill demand trend over time")
df_trend = get_skills_over_time(8)
if not df_trend.empty:
    fig3 = px.line(df_trend, x="week", y="count", color="skill",
                   labels={"count":"Mentions","week":"Week","skill":"Skill"},
                   color_discrete_sequence=px.colors.qualitative.Set2)
    fig3.update_layout(
        height=320, plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=0,r=0,t=10,b=0),
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor="#f1f3f5"),
        legend=dict(orientation="h", y=-0.2)
    )
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Trend data will appear after multiple scrape runs.")