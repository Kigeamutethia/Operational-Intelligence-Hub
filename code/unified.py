import streamlit as st
import pandas as pd
import numpy as np
import re
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import base64

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="Fleet Admin Intelligence System", layout="wide")

# ==============================
# STYLES
# ==============================
st.markdown("""
<style>
.main { background: linear-gradient(135deg, #0f172a, #1e293b); color: white; }
.card { backdrop-filter: blur(12px); background: rgba(255,255,255,0.08); border-radius:15px; padding:18px; text-align:center; }
.kpi-value { font-size:28px; font-weight:bold; }
.kpi-label { font-size:14px; opacity:0.8; }
</style>
""", unsafe_allow_html=True)

# ==============================
# USERS
# ==============================
USERS = {"ADMIN": "1234"}

# ==============================
# PORTABLE PATHS
# ==============================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"
ONEDRIVE_DIR = Path.home() / "OneDrive" / "Operational Intelligence Hub" / "Data"
BASE = DATA_DIR if DATA_DIR.exists() else ONEDRIVE_DIR

if not BASE.exists():
    st.error("Data folder not found")
    st.stop()

# ==============================
# FILES
# ==============================
mg4s_revenue = BASE / "eMobility" / "mG4sData" / "mg4sRevenue" / "revenueData.xlsx"
mg4s_mileage = BASE / "eMobility" / "mG4sData" / "mg4sMileage" / "mg4sMileage.xlsx"
leaf_revenue = BASE / "eMobility" / "nissanLeafsData" / "revenue" / "revenueData.xlsx"
leaf_mileage = BASE / "eMobility" / "nissanLeafsData" / "mileage" / "mileageData.csv"
charging_path = BASE / "eChargeAfrica" / "chargingInfraData" / "ops-Data" / "units-Data.xlsx"

# ==============================
# HELPERS
# ==============================
def clean_vrn(vrn):
    if pd.isna(vrn): return None
    vrn = re.sub(r"\\s+", "", str(vrn).upper())
    m = re.match(r"([A-Z]{3})([A-Z0-9]+)", vrn)
    return f"{m.group(1)} {m.group(2)}" if m else vrn


def find_date_column(df):
    for c in df.columns:
        if "date" in c.lower() or "time" in c.lower():
            return c
    return None


def force_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                .str.replace(",", "")
                .str.replace("KES", "")
                .str.replace(" ", "")
            )
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

# ==============================
# LOAD DATA
# ==============================
@st.cache_data
def load_data():
    mg4s_rev = pd.read_excel(mg4s_revenue)
    mg4s_mil = pd.read_excel(mg4s_mileage)
    leaf_rev = pd.read_excel(leaf_revenue)
    leaf_mil = pd.read_csv(leaf_mileage)
    charging = pd.read_excel(charging_path)

    for df in [mg4s_rev, mg4s_mil, leaf_rev, leaf_mil, charging]:
        df.columns = df.columns.str.strip()

    mg4s_rev["Fleet"] = "MG4S"
    mg4s_mil["Fleet"] = "MG4S"
    leaf_rev["Fleet"] = "LEAF"
    leaf_mil["Fleet"] = "LEAF"

    mg4s_rev["Vehicle"] = mg4s_rev["CAR REG"].apply(clean_vrn)
    leaf_rev["Vehicle"] = leaf_rev["Car Reg"].apply(clean_vrn)
    mg4s_mil["Vehicle"] = mg4s_mil["Device Name"].apply(clean_vrn)
    leaf_mil["Vehicle"] = leaf_mil["Vehicle"].apply(clean_vrn)

    def parse_date(df):
        col = find_date_column(df)
        if col is None:
            return pd.Series([pd.NaT]*len(df))
        return pd.to_datetime(df[col], errors="coerce", dayfirst=True).dt.normalize()

    mg4s_rev["Date"] = parse_date(mg4s_rev)
    leaf_rev["Date"] = parse_date(leaf_rev)
    mg4s_mil["Date"] = parse_date(mg4s_mil)
    leaf_mil["Date"] = parse_date(leaf_mil)

    charging_col = find_date_column(charging)
    charging["Date"] = pd.to_datetime(charging[charging_col], errors="coerce", dayfirst=True).dt.normalize()

    revenue = pd.concat([mg4s_rev, leaf_rev], ignore_index=True)
    mileage = pd.concat([mg4s_mil, leaf_mil], ignore_index=True)

    mileage = mileage.groupby(["Vehicle","Date","Fleet"], as_index=False).sum(numeric_only=True)

    df = revenue.merge(mileage, on=["Vehicle","Date","Fleet"], how="left")

    df = force_numeric(df, ["Paid","REVENUE EARNED (TO INVOICE)","Mileage"])

    return df

# ==============================
# LOAD
# ==============================
df = load_data()

# ==============================
# LOGIN
# ==============================
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    u = st.text_input("Admin Username")
    p = st.text_input("Password", type="password")
    if st.button("Login"):
        if USERS.get(u) == p:
            st.session_state.user = u
            st.rerun()
        else:
            st.error("Invalid login")
    st.stop()

# ==============================
# FILTERS
# ==============================
st.sidebar.title("Admin Filters")
fleet_filter = st.sidebar.selectbox("Fleet Type", ["ALL","MG4S","LEAF"])

min_d, max_d = df["Date"].min(), df["Date"].max()
dates = st.sidebar.date_input("Period", (min_d, max_d))
start, end = pd.to_datetime(dates[0]), pd.to_datetime(dates[1])

filtered = df[df["Date"].between(start, end)]
if fleet_filter != "ALL":
    filtered = filtered[filtered["Fleet"] == fleet_filter]

if filtered.empty:
    st.warning("No data")
    st.stop()

# ==============================
# KPIs
# ==============================
rev = filtered["REVENUE EARNED (TO INVOICE)"].sum()
paid = filtered["Paid"].sum()
mileage = filtered.get("Mileage", pd.Series([0])).sum()
eff = (paid / rev * 100) if rev else 0

st.metric("Revenue", f"KES {rev:,.0f}")
st.metric("Paid", f"KES {paid:,.0f}")
st.metric("Mileage", f"{mileage:,.0f} km")
st.metric("Efficiency", f"{eff:.1f}%")

# ==============================
# OVERVIEW
# ==============================
agg = filtered.groupby("Fleet", as_index=False).agg({
    "REVENUE EARNED (TO INVOICE)": "sum",
    "Paid": "sum",
    "Mileage": "sum"
})

st.subheader("Fleet Overview")
st.dataframe(agg)

# ==============================
# OUTSTANDING BALANCE (AS AT END OF PERIOD)
# ==============================

balance_df = filtered.copy()

# ensure proper sorting
balance_df = balance_df.sort_values(["Vehicle", "Date"])

# take last known balance per vehicle within selected period
latest_balance = balance_df.groupby("Vehicle", as_index=False).tail(1)

# safe numeric conversion
if "BAL B/D" in latest_balance.columns:
    latest_balance["BAL B/D"] = (
        latest_balance["BAL B/D"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("KES", "")
        .str.strip()
    )
    latest_balance["BAL B/D"] = pd.to_numeric(latest_balance["BAL B/D"], errors="coerce").fillna(0)
else:
    latest_balance["BAL B/D"] = 0

# FINAL FLEET OUTSTANDING
total_outstanding = latest_balance["BAL B/D"].sum()

# ==============================
# TRENDLINES (FIXED SAFE AGGREGATION)
# ==============================
st.subheader("📈 Fleet Trendlines (Aggregated)")

trend = filtered.copy()
trend["Date"] = pd.to_datetime(trend["Date"])

trend_agg = trend.groupby("Date", as_index=False).agg({
    "REVENUE EARNED (TO INVOICE)": "sum",
    "Paid": "sum",
    "Mileage": "sum"
})

# optional energy column safe handling
if "Units Consumed(kWh)" in trend.columns:
    energy = trend.groupby("Date")["Units Consumed(kWh)"].sum().reset_index()
    trend_agg = trend_agg.merge(energy, on="Date", how="left")
else:
    trend_agg["Units Consumed(kWh)"] = 0

fig1 = px.line(trend_agg, x="Date", y="REVENUE EARNED (TO INVOICE)", title="Revenue Trend", markers=True)
st.plotly_chart(fig1, use_container_width=True)

fig2 = px.line(trend_agg, x="Date", y="Paid", title="Payments Trend", markers=True)
st.plotly_chart(fig2, use_container_width=True)

fig3 = px.line(trend_agg, x="Date", y="Mileage", title="Mileage Trend", markers=True)
st.plotly_chart(fig3, use_container_width=True)

if trend_agg["Units Consumed(kWh)"].sum() > 0:
    fig4 = px.line(trend_agg, x="Date", y="Units Consumed(kWh)", title="Energy Trend (kWh)", markers=True)
    st.plotly_chart(fig4, use_container_width=True)

# ==============================
# DETAIL
# ==============================
st.subheader("Detailed Data")
st.dataframe(filtered.sort_values("Date", ascending=False))