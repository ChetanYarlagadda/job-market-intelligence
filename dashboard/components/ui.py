# dashboard/components/ui.py
# ─────────────────────────────────────────────
# Reusable UI building blocks used across all pages
# ─────────────────────────────────────────────

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# ── Color palette ──────────────────────────────────────────────
COLORS = {
    "primary":   "#00C896",
    "secondary": "#3B82F6",
    "amber":     "#F59E0B",
    "coral":     "#EF4444",
    "purple":    "#8B5CF6",
    "teal":      "#14B8A6",
    "bg":        "#0E1117",
    "card":      "#161B22",
    "border":    "#30363D",
    "text":      "#E6EDF3",
    "muted":     "#8B949E",
}

ROLE_COLORS = [
    "#00C896", "#3B82F6", "#F59E0B",
    "#8B5CF6", "#EF4444", "#14B8A6",
    "#EC4899", "#F97316", "#06B6D4",
]

# ── Plotly base layout ─────────────────────────────────────────
def base_layout(title="", height=400, showlegend=True) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=14, color=COLORS["text"])),
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="monospace", color=COLORS["muted"], size=12),
        showlegend=showlegend,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=COLORS["border"],
            borderwidth=0.5,
            font=dict(size=11),
        ),
        margin=dict(l=40, r=20, t=40, b=40),
        xaxis=dict(
            gridcolor=COLORS["border"],
            linecolor=COLORS["border"],
            tickfont=dict(size=11),
        ),
        yaxis=dict(
            gridcolor=COLORS["border"],
            linecolor=COLORS["border"],
            tickfont=dict(size=11),
        ),
    )


# ── Metric card ────────────────────────────────────────────────
def metric_card(label: str, value: str, delta: str = None,
                delta_good: bool = True, icon: str = ""):
    delta_color = "#00C896" if delta_good else "#EF4444"
    delta_html  = f'<p style="color:{delta_color};font-size:12px;margin:4px 0 0;">{delta}</p>' if delta else ""
    st.markdown(f"""
    <div style="
        background:{COLORS['card']};
        border:0.5px solid {COLORS['border']};
        border-radius:12px;
        padding:16px 20px;
        height:100%;
    ">
        <p style="color:{COLORS['muted']};font-size:11px;letter-spacing:0.06em;
                  text-transform:uppercase;margin:0 0 8px;">{icon} {label}</p>
        <p style="color:{COLORS['text']};font-size:26px;font-weight:500;
                  margin:0;font-family:monospace;">{value}</p>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


# ── Section header ─────────────────────────────────────────────
def section_header(title: str, subtitle: str = ""):
    st.markdown(f"""
    <div style="margin:1.5rem 0 1rem;">
        <h3 style="color:{COLORS['text']};font-size:15px;font-weight:500;
                   margin:0 0 4px;letter-spacing:0.02em;">{title}</h3>
        {"" if not subtitle else f'<p style="color:{COLORS["muted"]};font-size:12px;margin:0;">{subtitle}</p>'}
    </div>
    """, unsafe_allow_html=True)


# ── Status badge ───────────────────────────────────────────────
def status_badge(status: str) -> str:
    colors = {
        "success": ("#00C896", "#0D2420"),
        "failed":  ("#EF4444", "#2A1010"),
        "running": ("#F59E0B", "#2A1E08"),
    }
    fg, bg = colors.get(status.lower(), (COLORS["muted"], COLORS["card"]))
    return f'<span style="background:{bg};color:{fg};border:0.5px solid {fg}40;padding:2px 8px;border-radius:6px;font-size:11px;font-family:monospace;">{status}</span>'


# ── Divider ────────────────────────────────────────────────────
def divider():
    st.markdown(f'<hr style="border:none;border-top:0.5px solid {COLORS["border"]};margin:1.5rem 0;">', unsafe_allow_html=True)


# ── Empty state ────────────────────────────────────────────────
def empty_state(message: str = "No data yet. Run your first scrape to populate this view."):
    st.markdown(f"""
    <div style="
        text-align:center;
        padding:3rem 2rem;
        border:0.5px dashed {COLORS['border']};
        border-radius:12px;
        margin:1rem 0;
    ">
        <p style="color:{COLORS['muted']};font-size:14px;margin:0;">{message}</p>
    </div>
    """, unsafe_allow_html=True)


# ── Salary formatter ───────────────────────────────────────────
def fmt_salary(val) -> str:
    if not val or pd.isna(val):
        return "N/A"
    return f"${int(val):,}"


# ── Bar chart ──────────────────────────────────────────────────
def bar_chart(df: pd.DataFrame, x: str, y: str,
              color: str = None, title: str = "",
              height: int = 350, horizontal: bool = False) -> go.Figure:
    if horizontal:
        fig = px.bar(df, x=y, y=x, orientation="h", color=color,
                     color_discrete_sequence=ROLE_COLORS)
        fig.update_traces(marker_line_width=0)
    else:
        fig = px.bar(df, x=x, y=y, color=color,
                     color_discrete_sequence=ROLE_COLORS)
        fig.update_traces(marker_line_width=0)

    fig.update_layout(**base_layout(title=title, height=height))
    return fig


# ── Line chart ─────────────────────────────────────────────────
def line_chart(df: pd.DataFrame, x: str, y: str,
               color: str = None, title: str = "",
               height: int = 350) -> go.Figure:
    fig = px.line(df, x=x, y=y, color=color,
                  color_discrete_sequence=ROLE_COLORS,
                  markers=True)
    fig.update_traces(line_width=2, marker_size=4)
    fig.update_layout(**base_layout(title=title, height=height))
    return fig