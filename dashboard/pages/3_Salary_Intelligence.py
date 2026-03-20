# dashboard/pages/3_Salary_Intelligence.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.db import get_salary_by_role, get_salary_by_seniority, get_top_companies

st.set_page_config(page_title="Salary Intelligence", page_icon="💰", layout="wide")
st.title("Salary Intelligence")
st.caption("Salary analysis across roles, seniority levels, and companies")

df = get_salary_by_role()
if df.empty:
    st.info("No salary data yet. Many job postings don't list salaries — run more scrapes to build up data.")
    st.stop()

# ── Row 1: Box plot + Seniority bar ───────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("Salary range by role")
    fig = px.box(
        df, x="salary_mid", y="role",
        color="role", orientation="h",
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={"salary_mid":"Annual Salary ($)","role":""},
        points="outliers"
    )
    fig.update_layout(
        height=380, showlegend=False,
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=0,r=0,t=10,b=0),
        xaxis=dict(tickprefix="$", tickformat=",", gridcolor="#f1f3f5"),
        yaxis=dict(showgrid=False)
    )
    fig.update_traces(hovertemplate="<b>%{y}</b><br>$%{x:,.0f}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Avg salary by seniority")
    df_sen = get_salary_by_seniority()
    if not df_sen.empty:
        order = ["intern","junior","mid","senior","lead","principal"]
        df_sen["order"] = df_sen["seniority"].map(
            {v: i for i, v in enumerate(order)}
        ).fillna(99)
        df_sen = df_sen.sort_values("order")
        colors = ["#c8d8ff","#a5b9ff","#7aa2f7","#4361ee","#3a0ca3","#240046"]
        fig2 = go.Figure(go.Bar(
            x=df_sen["seniority"],
            y=df_sen["avg_salary"],
            marker_color=colors[:len(df_sen)],
            text=df_sen["avg_salary"].apply(lambda x: f"${int(x):,}"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>$%{y:,.0f}<extra></extra>"
        ))
        fig2.update_layout(
            height=380, plot_bgcolor="white", paper_bgcolor="white",
            margin=dict(l=0,r=0,t=10,b=0),
            yaxis=dict(tickprefix="$", tickformat=",", gridcolor="#f1f3f5", showgrid=True),
            xaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Salary histogram ────────────────────────────────────
st.subheader("Salary distribution")
role_opts = ["All roles"] + sorted(df["role"].unique().tolist())
selected  = st.selectbox("Filter role", role_opts)
plot_df   = df if selected == "All roles" else df[df["role"] == selected]

fig3 = px.histogram(
    plot_df, x="salary_mid", nbins=40,
    color_discrete_sequence=["#4361ee"],
    labels={"salary_mid": "Annual Salary ($)", "count": "Jobs"},
)
fig3.update_layout(
    height=280, plot_bgcolor="white", paper_bgcolor="white",
    margin=dict(l=0,r=0,t=10,b=0),
    xaxis=dict(tickprefix="$", tickformat=",", gridcolor="#f1f3f5"),
    yaxis=dict(gridcolor="#f1f3f5"), bargap=0.05
)
st.plotly_chart(fig3, use_container_width=True)

# ── Row 3: Top paying companies ────────────────────────────────
st.subheader("Top paying companies")
df_cos = get_top_companies(15)
if not df_cos.empty:
    df_cos = df_cos.sort_values("avg_salary", ascending=False).dropna(subset=["avg_salary"])
    df_cos["avg_salary"] = df_cos["avg_salary"].apply(lambda x: f"${int(x):,}" if x else "—")
    df_cos.columns = ["Company","Open Roles","Avg Salary"]
    st.dataframe(df_cos, use_container_width=True, hide_index=True)