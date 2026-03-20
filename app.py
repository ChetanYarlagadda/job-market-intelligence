# app.py — Main Streamlit entry point
# Run with: streamlit run app.py
import streamlit as st

st.set_page_config(
    page_title = "Job Market Intelligence",
    page_icon  = "📊",
    layout     = "wide",
    initial_sidebar_state = "expanded",
    menu_items = {
        "Get Help": None,
        "Report a bug": None,
        "About": "Job Market Intelligence Engine — built with python-jobspy, PostgreSQL & Streamlit"
    }
)

st.markdown("""
<style>
/* Sidebar nav styling */
[data-testid="stSidebarNav"] a { font-size: 14px; padding: 6px 12px; }
[data-testid="stSidebarNav"] a:hover { background: rgba(67,97,238,0.08); border-radius: 8px; }

/* Global font */
html, body, [class*="css"] { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }

/* Metric cards */
[data-testid="stMetric"] {
    background: #f8f9fa;
    border-radius: 10px;
    padding: 12px 16px;
    border: 1px solid #e9ecef;
}

/* Primary button */
.stButton button[kind="primary"] {
    background: #4361ee;
    border-color: #4361ee;
}
.stButton button[kind="primary"]:hover {
    background: #3a0ca3;
    border-color: #3a0ca3;
}

/* Hide default streamlit header */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar branding ───────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding: 12px 0 20px;">
        <div style="font-size:20px; font-weight:700; color:#1a1a2e;">📊 JobIntel</div>
        <div style="font-size:12px; color:#6c757d; margin-top:4px;">Real-time job market data</div>
    </div>
    """, unsafe_allow_html=True)

# ── Landing page ───────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding: 60px 0 40px;">
    <div style="font-size:48px; margin-bottom:12px;">📊</div>
    <h1 style="font-size:36px; font-weight:700; color:#1a1a2e; margin:0;">
        Job Market Intelligence
    </h1>
    <p style="font-size:18px; color:#6c757d; margin:12px 0 0; max-width:560px; margin-left:auto; margin-right:auto;">
        Real-time job data from Indeed & LinkedIn. Updated daily. No subscriptions.
    </p>
</div>
""", unsafe_allow_html=True)

col1, col2, col3, col4, col5, col6 = st.columns(6)
nav_items = [
    (col1, "🏠", "Home",             "Overview & KPIs"),
    (col2, "🗺️", "USA Map",          "Jobs by state"),
    (col3, "💰", "Salary",           "Pay intelligence"),
    (col4, "🧠", "Skills",           "Skill demand"),
    (col5, "🔍", "Explorer",         "Browse all jobs"),
    (col6, "⚙️", "Pipeline Control", "Run & schedule"),
]

for col, icon, name, desc in nav_items:
    with col:
        st.markdown(f"""
        <div style="text-align:center; padding:20px 8px; background:#f8f9fa;
                    border-radius:12px; border:1px solid #e9ecef; cursor:pointer;">
            <div style="font-size:28px;">{icon}</div>
            <div style="font-size:13px; font-weight:600; color:#1a1a2e; margin:8px 0 4px;">{name}</div>
            <div style="font-size:11px; color:#6c757d;">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.info("Use the sidebar to navigate between pages. "
        "Start with **Pipeline Control** to run your first scrape, then explore the results.", icon="👈")