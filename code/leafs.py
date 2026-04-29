import streamlit as st
import pandas as pd
import numpy as np
import re
import plotly.graph_objects as go
import plotly.express as px
import base64

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Nissan Leafs Fleet Intelligence", layout="wide")

# ==============================
# GLOBAL STYLES
# ==============================
st.markdown("""
<style>
.main {
    background: linear-gradient(135deg, #0f172a, #1e293b);
    color: white;
}
.card {
    backdrop-filter: blur(12px);
    background: rgba(255, 255, 255, 0.08);
    border-radius: 15px;
    padding: 18px;
    box-shadow: 0 4px 30px rgba(0,0,0,0.2);
    text-align: center;
}
.kpi-value {
    font-size: 28px;
    font-weight: bold;
    margin-top: 5px;
}
.kpi-label {
    font-size: 14px;
    opacity: 0.8;
}
.risk-box {
    padding: 15px;
    border-radius: 12px;
    font-weight: bold;
    text-align: center;
    font-size: 18px;
}
.period-banner {
    padding: 10px 18px;
    border-radius: 10px;
    background: rgba(54, 172, 67, 0.15);
    border: 1px solid rgba(54, 172, 67, 0.4);
    color: #7CFC00;
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 18px;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

# ==============================
# USERS
# ==============================
USERS = {
    "WALTER MULUVI MWALIMU": "1234",
    "STEVEN WASIKE KHAEMBA": "1234",
    "MBUVI KASYOKA": "1234",
    "SIMON RAGUI KARIUKI": "1234",
    "KENNETH MUMENYA": "1234",
    "JOSEPH GATHUMA KINUTHIA": "1234",
    "EDWARD GONDI": "1234",
    "MILTON MAU": "1234",
    "NICHOLAS GITAU": "1234",
    "MICHAEL GENESIS": "1234",
    "SHELEMIAH OTIENO": "1234",
    "DANIEL KIMANI": "1234",
    "LEONARD MUGAMBI": "1234",
    "SULTAN KENNEDY TOO": "1234",
    "CEDRIC ANYONA": "1234",
    "WILLIAM NGETHE": "1234",
    "JOSEPH MACHARIA": "1234",
    "EVANS KIPKOSGEI": "1234"
}

# ==============================
# FILE PATHS
# ==============================
base = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data"

revenue_path = base + r"\eMobility\nissanLeafsData\revenue\revenueData.xlsx"
mileage_path = base + r"\eMobility\nissanLeafsData\mileage\mileageData.csv"
charging_path = base + r"\eChargeAfrica\chargingInfraData\ops-Data\units-Data.xlsx"

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
    revenue  = pd.read_excel(revenue_path, engine="openpyxl")
    mileage  = pd.read_csv(mileage_path)
    charging = pd.read_excel(charging_path, engine="openpyxl")

    for df in [revenue, mileage, charging]:
        df.columns = df.columns.str.strip()

    revenue["Driver"]   = revenue["Name"].astype(str).str.upper().str.strip()
    revenue["Vehicle"]  = revenue["Car Reg"].apply(clean_vrn)
    mileage["Vehicle"]  = mileage["Vehicle"].apply(clean_vrn)
    charging["Vehicle"] = charging["VRN"].apply(clean_vrn)

    revenue["Date"]  = pd.to_datetime(revenue["Date"],                errors="coerce").dt.normalize()
    mileage["Date"]  = pd.to_datetime(mileage["StartLocationTime"],   errors="coerce").dt.normalize()
    charging["Date"] = pd.to_datetime(charging["Transaction Date"],   errors="coerce").dt.normalize()

    def num(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    revenue  = num(revenue,  ["REVENUE EARNED (TO INVOICE)", "Paid", "BAL B/D"])
    mileage  = num(mileage,  ["Mileage"])
    charging = num(charging, ["Units Consumed(kWh)"])

    mileage_g  = mileage.groupby(["Vehicle","Date"],  as_index=False).agg({"Mileage": "sum"})
    charging_g = charging.groupby(["Vehicle","Date"], as_index=False).agg({"Units Consumed(kWh)": "sum"})

    df = revenue.merge(mileage_g,  on=["Vehicle","Date"], how="left")
    df = df.merge(charging_g,      on=["Vehicle","Date"], how="left")

    return df

df = load_data()

# ==============================
# HEADER WITH LOGO
# ==============================
def get_base64_image(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except FileNotFoundError:
        return None

logo_base64 = get_base64_image("logo.jpeg")

if logo_base64:
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:center;gap:15px;
        padding:15px;border-radius:12px;background:#ffffff;margin-bottom:20px;">
        <img src="data:image/jpeg;base64,{logo_base64}" width="280">
        <h2 style="color:#36ac43;margin:0;">Nissan Leafs Fleet Intelligence</h2>
    </div>""", unsafe_allow_html=True)
else:
    st.markdown("""
    <div style="padding:20px;border-radius:12px;
        background:linear-gradient(90deg,#36ac43,#000080);
        color:white;font-size:26px;font-weight:bold;text-align:center;margin-bottom:20px;">
        🚗 Nissan Leafs Fleet Intelligence
    </div>""", unsafe_allow_html=True)

# ==============================
# LOGIN
# ==============================
if "driver" not in st.session_state:
    st.session_state.driver = None

if st.session_state.driver is None:
    driver   = st.selectbox("Select Driver", list(USERS.keys()))
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

    # ==============================
    # SIDEBAR — DATE FILTER (single source of truth)
    # ==============================
    st.sidebar.markdown("### 📅 Filter Period")

    min_d = df["Date"].min().date()
    max_d = df["Date"].max().date()

    # Initialise preset state
    if "preset_start" not in st.session_state:
        st.session_state["preset_start"] = min_d
    if "preset_end" not in st.session_state:
        st.session_state["preset_end"] = max_d

    dr = st.sidebar.date_input(
        "Select Period",
        (st.session_state["preset_start"], st.session_state["preset_end"]),
        min_value=min_d,
        max_value=max_d
    )
    start_ts = pd.Timestamp(dr[0] if len(dr) >= 1 else min_d)
    end_ts   = pd.Timestamp(dr[1] if len(dr) == 2 else max_d)

    # Quick presets
    st.sidebar.markdown("**Quick Presets**")
    col_a, col_b = st.sidebar.columns(2)
    col_c, col_d = st.sidebar.columns(2)

    today = pd.Timestamp.today()

    if col_a.button("This Month"):
        st.session_state["preset_start"] = today.replace(day=1).date()
        st.session_state["preset_end"]   = today.date()
        st.rerun()
    if col_b.button("Last 30 Days"):
        st.session_state["preset_start"] = (today - pd.Timedelta(days=30)).date()
        st.session_state["preset_end"]   = today.date()
        st.rerun()
    if col_c.button("Last 90 Days"):
        st.session_state["preset_start"] = (today - pd.Timedelta(days=90)).date()
        st.session_state["preset_end"]   = today.date()
        st.rerun()
    if col_d.button("All Time"):
        st.session_state["preset_start"] = min_d
        st.session_state["preset_end"]   = max_d
        st.rerun()

    # ==============================
    # PERIOD BANNER — always visible above every section
    # ==============================
    st.markdown(
        f'<div class="period-banner">📅 Showing data: '
        f'<strong>{start_ts.strftime("%d %b %Y")}</strong> &nbsp;→&nbsp; '
        f'<strong>{end_ts.strftime("%d %b %Y")}</strong></div>',
        unsafe_allow_html=True
    )

    # ==============================
    # FILTERED DATA — single source used everywhere
    # ==============================
    data = df[
        (df["Driver"] == driver) &
        (df["Date"]   >= start_ts) &
        (df["Date"]   <= end_ts)
    ].copy()

    # Fleet-wide: same period, all drivers (for ranking)
    fleet_data = df[
        (df["Date"] >= start_ts) &
        (df["Date"] <= end_ts)
    ].copy()

    if data.empty:
        st.warning("⚠️ No data found for the selected period. Try adjusting the date range.")
        st.stop()

    # ==============================
    # METRICS
    # ==============================
    revenue     = data["REVENUE EARNED (TO INVOICE)"].sum()
    paid        = data["Paid"].sum()
    mileage_km  = data["Mileage"].sum()
    units_kwh   = data["Units Consumed(kWh)"].sum()
    outstanding = (
        data.sort_values("Date")["BAL B/D"].dropna().iloc[-1]
        if not data["BAL B/D"].dropna().empty else 0
    )
    efficiency = (paid / revenue * 100) if revenue else 0

    # ==============================
    # RISK
    # ==============================
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

    risk_colors = {
        "🟢 VERY LOW": {"bg": "#16a34a", "text": "#ffffff"},
        "🟡 LOW":      {"bg": "#facc15", "text": "#000000"},
        "🟠 MEDIUM":   {"bg": "#fb923c", "text": "#000000"},
        "🔴 HIGH":     {"bg": "#ef4444", "text": "#ffffff"},
        "🔴 EXTREME":  {"bg": "#7f1d1d", "text": "#ffffff"}
    }

    # ==============================
    # KPI CARDS
    # ==============================
    st.subheader("📊 Driver Performance Overview")

    col1, col2, col3 = st.columns(3)
    col1.markdown(f'<div class="card"><div class="kpi-label">💰 Revenue</div><div class="kpi-value">KES {revenue:,.0f}</div></div>',               unsafe_allow_html=True)
    col2.markdown(f'<div class="card"><div class="kpi-label">💵 Paid</div><div class="kpi-value">KES {paid:,.0f}</div></div>',                       unsafe_allow_html=True)
    col3.markdown(f'<div class="card"><div class="kpi-label">⚠️ Outstanding</div><div class="kpi-value">KES {outstanding:,.0f}</div></div>',         unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)
    col4.markdown(f'<div class="card"><div class="kpi-label">🚗 Mileage</div><div class="kpi-value">{mileage_km:,.1f} km</div></div>',               unsafe_allow_html=True)
    col5.markdown(f'<div class="card"><div class="kpi-label">⚡ Units (kWh)</div><div class="kpi-value">{units_kwh:,.1f}</div></div>',                unsafe_allow_html=True)
    col6.markdown(f'<div class="card"><div class="kpi-label">📊 Efficiency</div><div class="kpi-value">{efficiency:.1f}%</div></div>',               unsafe_allow_html=True)

    # ==============================
    # RISK BOX
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 🚨 Risk Assessment")
    rc = risk_colors[risk]
    st.markdown(
        f'<div class="risk-box" style="background:{rc["bg"]};color:{rc["text"]};">{risk}</div>',
        unsafe_allow_html=True
    )

    # ==============================
    # CHARTS
    # ==============================
    period_label = f"{start_ts.strftime('%d %b %Y')} – {end_ts.strftime('%d %b %Y')}"

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("📈 Performance Trends")

    # 1. Revenue vs Paid
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=data["Date"], y=data["REVENUE EARNED (TO INVOICE)"],
        mode="lines+markers", name="Revenue",
        line=dict(color="#36ac43", width=3)
    ))
    fig1.add_trace(go.Scatter(
        x=data["Date"], y=data["Paid"],
        mode="lines+markers", name="Paid",
        line=dict(color="#000080", width=3)
    ))
    fig1.update_layout(
        title=f"Revenue vs Payments  ·  {period_label}",
        xaxis_title="Date", yaxis_title="KES",
        hovermode="x unified", template="plotly_dark"
    )
    st.plotly_chart(fig1, use_container_width=True)

    # 2. Mileage Trend
    fig2 = px.line(
        data, x="Date", y="Mileage",
        title=f"Mileage Trend  ·  {period_label}",
        markers=True, template="plotly_dark"
    )
    fig2.update_traces(line=dict(color="#36ac43", width=3))
    st.plotly_chart(fig2, use_container_width=True)

    # 3. Outstanding Balance Trend
    debt_trend = (
        data.sort_values("Date")
            .groupby("Date")["BAL B/D"]
            .last()
            .fillna(method="ffill")
            .fillna(0)
            .reset_index()
    )
    fig3 = px.line(
        debt_trend, x="Date", y="BAL B/D",
        title=f"Outstanding Balance Trend  ·  {period_label}",
        markers=True, template="plotly_dark"
    )
    fig3.update_traces(line=dict(color="#C0392B", width=3))
    fig3.update_layout(xaxis_title="Date", yaxis_title="KES")
    st.plotly_chart(fig3, use_container_width=True)

    # ==============================
    # FLEET RANKING — fleet_data (all drivers, same period)
    # ==============================
    st.subheader(f"🏆 Fleet Ranking  ·  {period_label}")

    ranks = fleet_data.groupby("Driver", as_index=False).agg({
        "REVENUE EARNED (TO INVOICE)": "sum",
        "Paid": "sum",
        "Mileage": "sum",
        "Units Consumed(kWh)": "sum"
    })

    ranks["Efficiency (%)"] = np.where(
        ranks["REVENUE EARNED (TO INVOICE)"] > 0,
        (ranks["Paid"] / ranks["REVENUE EARNED (TO INVOICE)"]) * 100,
        0
    ).round(1)

    ranks = ranks.sort_values("Efficiency (%)", ascending=False).reset_index(drop=True)
    ranks.index = ranks.index + 1  # 1-based rank

    driver_row = ranks[ranks["Driver"] == driver]
    my_rank    = driver_row.index[0] if not driver_row.empty else "N/A"

    st.success(f"Your Rank: #{my_rank} out of {len(ranks)} drivers")

    st.markdown("### 📊 Your Position Breakdown")
    st.dataframe(
        driver_row.rename(columns={
            "REVENUE EARNED (TO INVOICE)": "Revenue (KES)",
            "Paid":                        "Paid (KES)",
            "Mileage":                     "Mileage (km)",
            "Units Consumed(kWh)":         "Units (kWh)"
        }),
        use_container_width=True
    )

    # ==============================
    # DETAILED ACTIVITY TABLE
    # ==============================
    st.subheader(f"📄 Detailed Activity  ·  {period_label}")

    columns_to_show = [
        "Date", "Vehicle",
        "REVENUE EARNED (TO INVOICE)", "Paid",
        "Mileage", "Units Consumed(kWh)", "BAL B/D"
    ]
    columns_to_show = [c for c in columns_to_show if c in data.columns]

    table_df = data[columns_to_show].sort_values("Date", ascending=False).rename(columns={
        "REVENUE EARNED (TO INVOICE)": "Revenue (KES)",
        "Paid":                        "Paid (KES)",
        "Mileage":                     "Mileage (km)",
        "Units Consumed(kWh)":         "Units (kWh)",
        "BAL B/D":                     "Outstanding (KES)"
    })

    st.dataframe(table_df, use_container_width=True, height=400)