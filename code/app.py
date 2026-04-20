import streamlit as st
import pandas as pd
import numpy as np
import re
from sklearn.linear_model import LinearRegression
import plotly.express as px
import plotly.graph_objects as go
import base64

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Fleet Intelligence System", layout="wide")

# =============================

# ==============================
# GLOBAL STYLES (SaaS UI)
# ==============================
st.markdown("""
<style>

.main {
    background: linear-gradient(135deg, #0f172a, #1e293b);
    color: white;
}

/* HEADER */
.header-box {
    padding: 20px;
    border-radius: 12px;
    background: linear-gradient(90deg, #36ac43, #000080);
    color: white;
    font-size: 26px;
    font-weight: bold;
    text-align: center;
    margin-bottom: 20px;
}

/* GLASS CARD */
.card {
    backdrop-filter: blur(12px);
    background: rgba(255, 255, 255, 0.08);
    border-radius: 15px;
    padding: 18px;
    box-shadow: 0 4px 30px rgba(0,0,0,0.2);
    text-align: center;
}

/* KPI VALUE */
.kpi-value {
    font-size: 28px;
    font-weight: bold;
    margin-top: 5px;
}

/* KPI LABEL */
.kpi-label {
    font-size: 14px;
    opacity: 0.8;
}

/* RISK */
.risk-box {
    padding: 15px;
    border-radius: 12px;
    font-weight: bold;
    text-align: center;
    font-size: 18px;
}

</style>
""", unsafe_allow_html=True)

# ==============================
# USERS
# ==============================
USERS = {
    "BASIL MURAGE": "1234",
    "CLEOPAS THIONGO": "1234",
    "PETER KIPYEGO": "1234",
    "ELIAS KANGA": "1234",
    "AGNES ALOO": "1234",
    "JOSEPHAT MUMO": "1234",
    "VIVIANNE ODUMBE": "1234",
    "SAMUEL WACHIRA": "1234",
    "FARIDAH SALIM": "1234",
    "NANCY KANANA": "1234",
    "STEPHEN MAINA": "1234",
    "RICHARD MUEKE MUASYA": "1234",
    "HILLARY THOMAS OTIENO": "1234",
    "NANCY WANJIKU MWANGI": "1234",
    "JAMES AKONDO MNYOLE": "1234",
    "FRANCIS KITHURE MUTIGA": "1234"
}

# ==============================
# FILE PATHS
# ==============================
base_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data"

revenue_path = base_path + r"\eMobility\mG4sData\mg4sRevenue\revenueData.xlsx"
mileage_path = base_path + r"\eMobility\mG4sData\mg4sMileage\mg4sMileage.xlsx"
charging_path = base_path + r"\eChargeAfrica\chargingInfraData\ops-Data\units-Data.xlsx"

# ==============================
# HELPERS
# ==============================
def clean_vrn(vrn):
    if pd.isna(vrn):
        return None
    vrn = str(vrn).upper()
    vrn = re.sub(r"\s+", "", vrn)
    m = re.match(r"([A-Z]{3})([A-Z0-9]+)", vrn)
    return f"{m.group(1)} {m.group(2)}" if m else vrn

# ==============================
# LOAD DATA
# ==============================
@st.cache_data
def load_data():
    revenue = pd.read_excel(revenue_path)
    mileage = pd.read_excel(mileage_path)
    charging = pd.read_excel(charging_path)

    for df in [revenue, mileage, charging]:
        df.columns = df.columns.str.strip()

    revenue["Driver"] = revenue["NAME"].astype(str).str.upper().str.strip()

    revenue["Vehicle"] = revenue["CAR REG"].apply(clean_vrn)
    mileage["Vehicle"] = mileage["Device Name"].apply(clean_vrn)
    charging["Vehicle"] = charging["VRN"].apply(clean_vrn)

    revenue["Date"] = pd.to_datetime(revenue["Date"], errors="coerce").dt.normalize()
    mileage["Date"] = pd.to_datetime(mileage["Date"], errors="coerce").dt.normalize()
    charging["Date"] = pd.to_datetime(charging["Transaction Date"], errors="coerce").dt.normalize()

    def num(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    revenue = num(revenue, ["REVENUE EARNED (TO INVOICE)", "Paid", "BAL B/D"])
    mileage = num(mileage, ["Mileage(km)"])
    charging = num(charging, ["Units Consumed(kWh)"])

    mileage_g = mileage.groupby(["Vehicle", "Date"], as_index=False).sum(numeric_only=True)
    charging_g = charging.groupby(["Vehicle", "Date"], as_index=False).sum(numeric_only=True)

    df = revenue.merge(mileage_g, on=["Vehicle", "Date"], how="left")
    df = df.merge(charging_g, on=["Vehicle", "Date"], how="left")

    return df

df = load_data()

# ==============================
# HEADER
# ==============================
import base64

def get_base64_image(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

logo_base64 = get_base64_image("logo.jpeg")

st.markdown(f"""
<div style="
    display:flex;
    align-items:center;
    justify-content:center;
    gap:15px;
    padding:15px;
    border-radius:12px;
    background: #ffffff;
">
    <img src="data:image/jpeg;base64,{logo_base64}" width="280">
    <h2 style="color:#36ac43; margin:0;">Fleet Intelligence System</h2>
</div>
""", unsafe_allow_html=True)

# ==============================
# LOGIN
# ==============================
if "driver" not in st.session_state:
    st.session_state.driver = None

if st.session_state.driver is None:
    driver = st.selectbox("Select Driver", list(USERS.keys()))
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if USERS.get(driver) == password:
            st.session_state.driver = driver
            st.rerun()
        else:
            st.error("Invalid login")

else:
    driver = st.session_state.driver

    if st.sidebar.button("Logout"):
        st.session_state.driver = None
        st.rerun()

    # FILTER
    min_d, max_d = df["Date"].min(), df["Date"].max()
    dr = st.sidebar.date_input("Select Period", (min_d, max_d))
    start, end = dr if len(dr) == 2 else (min_d, max_d)

    data = df[
        (df["Driver"] == driver) &
        (df["Date"] >= pd.to_datetime(start)) &
        (df["Date"] <= pd.to_datetime(end))
    ]

    if data.empty:
        st.warning("No data found")
        st.stop()

    # METRICS
    revenue = data["REVENUE EARNED (TO INVOICE)"].sum()
    paid = data["Paid"].sum()
    mileage = data["Mileage(km)"].sum()
    units = data["Units Consumed(kWh)"].sum()
    outstanding = data["BAL B/D"].dropna().iloc[-1]
    efficiency = (paid / revenue * 100) if revenue else 0

    # RISK
    if outstanding < 6000:
        risk = "🟢 VERY LOW"
    elif outstanding < 11599:
        risk = "🟡 LOW"
    elif outstanding < 19999:
        risk = "🟠 MEDIUM"
    elif outstanding < 23999:
        risk = "🔴 HIGH"
    else:
        risk = "🔴 EXTREME"

    # KPI CARDS
    st.subheader("📊 Driver Performance Overview")

    col1, col2, col3 = st.columns(3)

    col1.markdown(f'<div class="card"><div class="kpi-label">💰 Revenue</div><div class="kpi-value">KES {revenue:,.0f}</div></div>', unsafe_allow_html=True)
    col2.markdown(f'<div class="card"><div class="kpi-label">💵 Paid</div><div class="kpi-value">KES {paid:,.0f}</div></div>', unsafe_allow_html=True)
    col3.markdown(f'<div class="card"><div class="kpi-label">⚠️ Outstanding</div><div class="kpi-value">KES {outstanding:,.0f}</div></div>', unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)

    col4.markdown(f'<div class="card"><div class="kpi-label">🚗 Mileage</div><div class="kpi-value">{mileage:,.1f} km</div></div>', unsafe_allow_html=True)
    col5.markdown(f'<div class="card"><div class="kpi-label">⚡ Units</div><div class="kpi-value">{units:,.1f}</div></div>', unsafe_allow_html=True)
    col6.markdown(f'<div class="card"><div class="kpi-label">📊 Efficiency</div><div class="kpi-value">{efficiency:.1f}%</div></div>', unsafe_allow_html=True)

    risk_colors = {
        "🟢 VERY LOW": {"bg": "#16a34a", "text": "#ffffff"},
        "🟡 LOW": {"bg": "#facc15", "text": "#000000"},
        "🟠 MEDIUM": {"bg": "#fb923c", "text": "#000000"},
        "🔴 HIGH": {"bg": "#ef4444", "text": "#ffffff"},
        "🔴 EXTREME": {"bg": "#7f1d1d", "text": "#ffffff"}
    }

    st.markdown("### 🚨 Risk Assessment")
    st.markdown(f'<div class="risk-box" style="background:{risk_colors[risk]}">{risk}</div>', unsafe_allow_html=True)

    # ==============================
    # CHARTS
    # ==============================

    st.subheader("📊 Performance Trends")

    # ------------------------------
    # 1. Revenue vs Paid
    # ------------------------------
    fig1 = go.Figure()

    fig1.add_trace(go.Scatter(
        x=data["Date"],
        y=data["REVENUE EARNED (TO INVOICE)"],
        mode="lines+markers",
        name="Revenue",
        line=dict(color="#36ac43", width=3)
    ))

    fig1.add_trace(go.Scatter(
        x=data["Date"],
        y=data["Paid"],
        mode="lines+markers",
        name="Paid",
        line=dict(color="#000080", width=3)
    ))

    fig1.update_layout(
        title="Revenue vs Payments",
        xaxis_title="Date",
        yaxis_title="KES",
        hovermode="x unified",
        template="plotly_dark"
    )

    st.plotly_chart(fig1, use_container_width=True)

    # ------------------------------
    # 2. Mileage Trend
    # ------------------------------
    fig2 = px.line(
        data,
        x="Date",
        y="Mileage(km)",
        title="Mileage Trend",
        markers=True,
        template="plotly_dark"
    )

    fig2.update_traces(line=dict(color="#36ac43", width=3))

    st.plotly_chart(fig2, use_container_width=True)

    # ------------------------------
    # 3. Outstanding Trend
    # ------------------------------
    # ==============================
# DEBT TREND (REQUIRED FOR CHART)
# ==============================
    data["Date"] = pd.to_datetime(data["Date"])

    debt_trend = (
        data.sort_values("Date")
            .groupby("Date")["BAL B/D"]
            .last()
            .fillna(0)
    )
    fig3 = px.line(
        x=debt_trend.index,
        y=debt_trend.values,
        title="Outstanding Balance Trend",
        markers=True,
        template="plotly_dark"
    )

    fig3.update_traces(line=dict(color="#C0392B", width=3))

    st.plotly_chart(fig3, use_container_width=True)

# ==============================
# DATE-FILTERED FLEET RANKING
# ==============================
st.subheader("🏆 Fleet Ranking (Filtered by Period)")

# Apply SAME date filter used in dashboard
filtered_df = df[
    (df["Date"] >= pd.to_datetime(start)) &
    (df["Date"] <= pd.to_datetime(end))
]

# Aggregate per driver
ranks = filtered_df.groupby("Driver", as_index=False).agg({
    "REVENUE EARNED (TO INVOICE)": "sum",
    "Paid": "sum",
    "Mileage(km)": "sum",
    "Units Consumed(kWh)": "sum"
})

# Efficiency metric
ranks["Efficiency"] = np.where(
    ranks["REVENUE EARNED (TO INVOICE)"] > 0,
    (ranks["Paid"] / ranks["REVENUE EARNED (TO INVOICE)"]) * 100,
    0
)

# Sort by efficiency (or change to Revenue if needed)
ranks = ranks.sort_values("Efficiency", ascending=False).reset_index(drop=True)

# Get driver rank
driver_row = ranks[ranks["Driver"] == driver]

if not driver_row.empty:
    my_rank = driver_row.index[0] + 1
else:
    my_rank = "N/A"

# Show ONLY user's rank (privacy maintained)
st.success(f"Your Rank: #{my_rank}")

# Optional: show your own stats in ranking context
st.markdown("### 📊 Your Position Breakdown")

st.dataframe(
    ranks[ranks["Driver"] == driver],
    use_container_width=True
)

# ==============================
# FILTERED DATA TABLE
# ==============================
st.subheader("📄 Detailed Activity (Filtered)")

# Select important columns only (clean view)
columns_to_show = [
    "Date",
    "Vehicle",
    "REVENUE EARNED (TO INVOICE)",
    "Paid",
    "Mileage(km)",
    "Units Consumed(kWh)",
    "BAL B/D"
]

# Keep only available columns (safe)
columns_to_show = [col for col in columns_to_show if col in data.columns]

table_df = data[columns_to_show].sort_values("Date", ascending=False)

# Rename for UI clarity
table_df = table_df.rename(columns={
    "REVENUE EARNED (TO INVOICE)": "Revenue",
    "Mileage(km)": "Mileage",
    "Units Consumed(kWh)": "Units",
    "BAL B/D": "Outstanding"
})

st.dataframe(
    table_df,
    use_container_width=True,
    height=400
)