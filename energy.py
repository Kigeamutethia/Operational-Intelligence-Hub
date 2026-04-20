import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests

# =============================
# CONFIG
# =============================
st.set_page_config(page_title="Energy Intelligence Control Room", layout="wide")

st.title("⚡ Energy Intelligence Control Room")
st.caption("Revenue • Expenses • Profit • AI Analyst (Ollama)")


# =============================
# PATHS
# =============================
REVENUE_FOLDER = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data\eChargeAfrica\chargingInfraData\Revenue-Data"

BILLS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQbXdwATZriOICgr1wEN7gxfdQug2pH43HxTisB6TfTA0jOCOQCE6qB2Yjv7X1U7IDQlh0Qp_Kl-j_T/pub?gid=1814301307&single=true&output=csv"

RENTS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQbXdwATZriOICgr1wEN7gxfdQug2pH43HxTisB6TfTA0jOCOQCE6qB2Yjv7X1U7IDQlh0Qp_Kl-j_T/pub?gid=1973345800&single=true&output=csv"


# =============================
# OLLAMA CONFIG
# =============================
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3"


def call_ollama(prompt):
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False
    }

    try:
        r = requests.post(OLLAMA_URL, json=payload, timeout=120)

        if r.status_code != 200:
            return f"⚠️ Ollama error: {r.text}"

        return r.json().get("response", "")

    except Exception as e:
        return f"⚠️ Connection error: {str(e)}"


# =============================
# HELPERS
# =============================
def normalize(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def to_num(series):
    return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce")


def safe_sum(df, col):
    if df is None or df.empty:
        return 0
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
    return 0


# =============================
# LOAD REVENUE
# =============================
def load_revenue():
    file = [f for f in os.listdir(REVENUE_FOLDER) if f.endswith(".xlsx")][0]
    df = pd.read_excel(os.path.join(REVENUE_FOLDER, file), engine="openpyxl")

    df = normalize(df)

    df["transdate"] = pd.to_datetime(df["transdate"], errors="coerce")

    for col in df.columns:
        if "amount" in col:
            df[col] = to_num(df[col])

    df["year"] = df["transdate"].dt.year
    df["month"] = df["transdate"].dt.month_name()

    return df


# =============================
# LOAD EXPENSES
# =============================
def load_expenses(url):
    df = pd.read_csv(url)
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "monthlyamount": "monthly_amount",
        "amount_paid": "amount_paid",
        "balanceb/f": "balance_bf"
    })

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in df.columns:
        if "amount" in col or "balance" in col:
            df[col] = to_num(df[col])

    return df


# =============================
# DATA LOAD
# =============================
rev = load_revenue()
bills = load_expenses(BILLS_URL)
rents = load_expenses(RENTS_URL)


# =============================
# FILTERS (GLOBAL)
# =============================
st.sidebar.title("🎛️ Filters")

year_filter = st.sidebar.selectbox("Year", sorted(rev["year"].dropna().unique()))
month_filter = st.sidebar.multiselect("Month", sorted(rev["month"].dropna().unique()), default=sorted(rev["month"].dropna().unique()))
station_filter = st.sidebar.multiselect("Station", sorted(rev["marketname"].dropna().unique()), default=sorted(rev["marketname"].dropna().unique()))


filtered_rev = rev[
    (rev["year"] == year_filter) &
    (rev["month"].isin(month_filter)) &
    (rev["marketname"].isin(station_filter))
]


# =============================
# KPIs
# =============================
st.subheader("📊 Executive KPIs")

total_revenue = filtered_rev["grossamount"].sum()

total_bills = safe_sum(bills, "monthly_amount") + safe_sum(bills, "amount_paid")
total_rent = safe_sum(rents, "monthly_amount") + safe_sum(rents, "amount_paid")

total_costs = total_bills + total_rent
profit = total_revenue - total_costs
margin = (profit / total_revenue * 100) if total_revenue else 0

c1, c2, c3, c4 = st.columns(4)

c1.metric("💰 Revenue", f"{total_revenue:,.0f}")
c2.metric("🧾 Costs", f"{total_costs:,.0f}")
c3.metric("📈 Profit", f"{profit:,.0f}")
c4.metric("📊 Margin %", f"{margin:.2f}%")


# =============================
# REVENUE TREND
# =============================
st.subheader("📈 Revenue Trend")

filtered_rev["date"] = pd.to_datetime(filtered_rev["transdate"], errors="coerce")

trend = filtered_rev.groupby("date")["grossamount"].sum().reset_index()

fig = px.line(trend, x="date", y="grossamount", markers=True)
st.plotly_chart(fig, use_container_width=True)


# =============================
# STATION PERFORMANCE
# =============================
st.subheader("🏢 Station Performance")

station_df = filtered_rev.groupby("marketname")["grossamount"].sum().reset_index()

fig = px.bar(station_df, x="marketname", y="grossamount")
st.plotly_chart(fig, use_container_width=True)


# =============================
# 🚨 ALERTS ENGINE
# =============================
st.subheader("🚨 Alerts")

alerts = []

if total_costs > total_revenue:
    alerts.append("Costs exceed revenue — negative margin risk")

if not station_df.empty:
    worst_station = station_df.loc[station_df["grossamount"].idxmin(), "marketname"]
    alerts.append(f"Lowest performing station: {worst_station}")

for a in alerts:
    st.warning(a)

if not alerts:
    st.success("System stable — no major risks detected")


# =============================
# 🤖 OLLAMA AI ANALYST
# =============================
st.subheader("🤖 AI Analyst (Ollama Llama3)")

def build_prompt():
    return f"""
You are a senior CFO for an EV charging energy company.

Analyze the following:

Revenue: {total_revenue}
Costs: {total_costs}
Profit: {profit}
Margin: {margin}

Top Stations:
{station_df.head(10).to_string(index=False)}

Alerts:
{alerts}

Provide:
1. Business summary
2. Key drivers
3. Risks
4. Opportunities
5. 3 executive recommendations
"""


col1, col2 = st.columns([1, 2])

with col1:
    run_ai = st.button("Generate AI Insight")

with col2:
    st.info("Local AI via Ollama (fast, free, offline)")

if run_ai:
    with st.spinner("Ollama analyzing your data..."):

        prompt = build_prompt()
        ai_output = call_ollama(prompt)

    st.success("AI Insight Ready")
    st.write(ai_output)