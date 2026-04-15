import streamlit as st
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# =============================
# CONFIG
# =============================
BASE_DIR = "Data"
DB_PATH = "metadata.db"

st.set_page_config(
    page_title="Enterprise Data Platform",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Enterprise Data Governance & Analytics Platform")

# =============================
# LOGIN SYSTEM
# =============================
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "analyst": {"password": "analyst123", "role": "analyst"},
    "viewer": {"password": "viewer123", "role": "viewer"}
}

if "role" not in st.session_state:
    st.session_state["role"] = None

if st.session_state["role"] is None:
    st.subheader("🔐 Login")

    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        if u in USERS and USERS[u]["password"] == p:
            st.session_state["role"] = USERS[u]["role"]
            st.success(f"Logged in as {st.session_state['role']}")
        else:
            st.error("Invalid credentials")

    st.stop()

role = st.session_state["role"]

# =============================
# INIT DB
# =============================
def init_db():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS files (
        file_name TEXT,
        file_path TEXT,
        extension TEXT,
        size_kb REAL,
        tag TEXT,
        scanned_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS file_columns (
        file_name TEXT,
        column_name TEXT,
        data_type TEXT,
        missing_pct REAL,
        unique_count INTEGER
    )
    """)

    conn.close()

init_db()

# =============================
# HELPERS
# =============================
def load_file(path):
    ext = os.path.splitext(path)[1].lower()

    try:
        if ext == ".csv":
            return pd.read_csv(path, low_memory=False)
        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(path, engine="openpyxl")
    except:
        return None

    return None


def auto_tag(file_name, cols):
    text = (file_name + " " + " ".join(cols)).lower()

    if "kwh" in text or "energy" in text:
        return "Energy"
    elif "driver" in text or "vehicle" in text:
        return "Fleet"
    elif "cost" in text or "invoice" in text:
        return "Finance"
    return "General"


def profile(df):
    return {
        c: {
            "missing_%": round(df[c].isna().mean() * 100, 2),
            "unique": int(df[c].nunique())
        }
        for c in df.columns
    }

# =============================
# SCAN DATA
# =============================
def scan():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("DELETE FROM files")
    conn.execute("DELETE FROM file_columns")

    for root, _, files in os.walk(BASE_DIR):
        for f in files:

            if f.startswith("~$"):
                continue

            path = os.path.join(root, f)
            ext = os.path.splitext(f)[1].lower()

            try:
                size = round(os.path.getsize(path) / 1024, 2)

                df = load_file(path)

                cols = list(df.columns) if df is not None else []
                tag = auto_tag(f, cols)

                conn.execute("""
                INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    f, path, ext, size, tag,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))

                if df is not None:
                    prof = profile(df)

                    for c in df.columns:
                        conn.execute("""
                        INSERT INTO file_columns VALUES (?, ?, ?, ?, ?)
                        """, (
                            f,
                            c,
                            str(df[c].dtype),
                            prof[c]["missing_%"],
                            prof[c]["unique"]
                        ))

            except:
                continue

    conn.commit()
    conn.close()

# =============================
# LOAD DATA
# =============================
def load_files():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM files", conn)
    conn.close()
    return df


def load_columns(file):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM file_columns WHERE file_name=?",
        conn, params=(file,)
    )
    conn.close()
    return df

# =============================
# OUTLIERS
# =============================
def detect_outliers(df):
    res = {}
    nums = df.select_dtypes(include=np.number)

    for c in nums:
        q1 = nums[c].quantile(0.25)
        q3 = nums[c].quantile(0.75)
        iqr = q3 - q1

        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr

        out = nums[(nums[c] < low) | (nums[c] > high)]

        res[c] = {
            "count": len(out),
            "low": float(low),
            "high": float(high)
        }

    return res

# =============================
# FORECASTING (SIMPLE TREND)
# =============================
def forecast(df, date_col, value_col):
    df = df[[date_col, value_col]].dropna()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)

    base = df[value_col].tail(3).mean()

    future = [base * (1 + i * 0.02) for i in range(1, 6)]
    dates = pd.date_range(df[date_col].max(), periods=6)[1:]

    return pd.DataFrame({"date": dates, "forecast": future})

# =============================
# ANOMALIES
# =============================
def anomalies(df):
    alerts = []
    nums = df.select_dtypes(include=np.number)

    for c in nums:
        m = nums[c].mean()
        s = nums[c].std()

        high = m + 2 * s
        low = m - 2 * s

        a = nums[(nums[c] > high) | (nums[c] < low)]

        if not a.empty:
            alerts.append({
                "column": c,
                "count": len(a)
            })

    return alerts

# =============================
# UI CONTROLS
# =============================
st.sidebar.header("⚙️ Controls")

if role == "admin":
    if st.sidebar.button("🔄 Build Catalogue"):
        scan()
        st.success("Updated!")

df = load_files()

if df.empty:
    st.warning("No data found")
    st.stop()

# =============================
# SEARCH + FILTER
# =============================
q = st.sidebar.text_input("Search")

if q:
    df = df[df["file_name"].str.contains(q, case=False, na=False)]

tags = df["tag"].unique().tolist()
selected = st.sidebar.multiselect("Tags", tags, default=tags)

df = df[df["tag"].isin(selected)]

# =============================
# MAIN TABLE
# =============================
st.subheader("📁 Catalogue")
st.dataframe(df, use_container_width=True)

file = st.selectbox("Select File", df["file_name"])

if file:

    path = df[df["file_name"] == file]["file_path"].values[0]

    st.subheader("📌 Metadata")
    st.json(df[df["file_name"] == file].iloc[0].to_dict())

    data = load_file(path)

    if data is not None:

        # ================= ANALYSIS =================
        st.subheader("📊 Data Dictionary")
        st.dataframe(pd.DataFrame(profile(data)).T)

        st.subheader("🧪 Outliers")
        st.json(detect_outliers(data))

        st.subheader("🚨 Anomaly Alerts")
        st.json(anomalies(data))

        # ================= FORECAST =================
        st.subheader("📈 Forecasting")

        cols = data.columns.tolist()

        date_col = st.selectbox("Date Column", cols)
        value_col = st.selectbox("Value Column", cols)

        if st.button("Run Forecast"):
            try:
                st.line_chart(forecast(data, date_col, value_col).set_index("date"))
            except:
                st.error("Invalid columns")

        # ================= PREVIEW =================
        st.subheader("👀 Preview")
        st.dataframe(data)

# =============================
# KPI DASHBOARD
# =============================
st.subheader("📈 KPI Dashboard")

conn = sqlite3.connect(DB_PATH)
files_df = pd.read_sql_query("SELECT * FROM files", conn)
conn.close()

st.metric("Files", len(files_df))
st.metric("Tags", files_df["tag"].nunique())
st.metric("Size (KB)", round(files_df["size_kb"].sum(), 2))