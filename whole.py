import streamlit as st
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

import networkx as nx
import plotly.express as px

# =============================
# OPTIONAL PYVIS (SAFE)
# =============================
try:
    from pyvis.network import Network
    PYVIS_AVAILABLE = True
except:
    PYVIS_AVAILABLE = False


# =============================
# CONFIG
# =============================
BASE_DIR = os.path.join(os.getcwd(), "Data")
DB_PATH = "metadata.db"

st.set_page_config(
    page_title="Drivelectric Enterprise Data Platform",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Drivelectric Enterprise Data Intelligence Platform")


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

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username in USERS and USERS[username]["password"] == password:
            st.session_state["role"] = USERS[username]["role"]
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

    conn.execute("""
    CREATE TABLE IF NOT EXISTS lineage (
        source_file TEXT,
        transformation TEXT,
        output TEXT,
        created_at TEXT
    )
    """)

    conn.close()

init_db()


# =============================
# HELPERS
# =============================
def auto_tag(file_name, columns):
    text = (file_name + " " + " ".join(columns)).lower()

    if "fuel" in text or "kwh" in text:
        return "Energy"
    elif "driver" in text or "vehicle" in text:
        return "Fleet"
    elif "invoice" in text or "cost" in text:
        return "Finance"
    return "General"


def load_file(file_path):
    full_path = os.path.join(BASE_DIR, file_path)
    full_path = os.path.normpath(full_path)

    if not os.path.exists(full_path):
        return None

    try:
        ext = os.path.splitext(full_path)[1].lower()

        if ext == ".csv":
            return pd.read_csv(full_path, low_memory=False)
        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(full_path, engine="openpyxl")
    except:
        return None

    return None


def data_quality(df):
    return {
        col: {
            "missing_%": round(df[col].isna().mean() * 100, 2),
            "unique": int(df[col].nunique())
        }
        for col in df.columns
    }


def detect_outliers(df):
    results = {}
    nums = df.select_dtypes(include=np.number)

    for col in nums:
        q1 = nums[col].quantile(0.25)
        q3 = nums[col].quantile(0.75)
        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outliers = nums[(nums[col] < lower) | (nums[col] > upper)]

        results[col] = {
            "outliers": int(len(outliers)),
            "lower": float(lower),
            "upper": float(upper)
        }

    return results


def data_dictionary(df):
    return pd.DataFrame([
        {
            "column": c,
            "type": str(df[c].dtype),
            "missing_%": round(df[c].isna().mean() * 100, 2),
            "unique": df[c].nunique(),
            "sample": str(df[c].dropna().iloc[0]) if df[c].notna().any() else None
        }
        for c in df.columns
    ])


# =============================
# SCAN FOLDER
# =============================
def scan_folder():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM files")
    conn.execute("DELETE FROM file_columns")

    for root, _, files in os.walk(BASE_DIR):
        for f in files:

            if f.startswith("~$"):
                continue

            try:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, BASE_DIR).replace("\\", "/")

                size = round(os.path.getsize(abs_path) / 1024, 2)

                df = load_file(rel_path)
                cols = list(df.columns) if df is not None else []

                tag = auto_tag(f, cols)

                conn.execute("""
                INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    f,
                    rel_path,
                    os.path.splitext(f)[1].lower(),
                    size,
                    tag,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))

                if df is not None:
                    prof = data_quality(df)

                    for col in df.columns:
                        conn.execute("""
                        INSERT INTO file_columns VALUES (?, ?, ?, ?, ?)
                        """, (
                            f,
                            col,
                            str(df[col].dtype),
                            prof[col]["missing_%"],
                            prof[col]["unique"]
                        ))

            except:
                pass

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


# =============================
# SIDEBAR CONTROLS
# =============================
st.sidebar.header("⚙️ Controls")

if role == "admin":
    if st.sidebar.button("🔄 Build Catalogue"):
        scan_folder()
        st.success("Catalogue updated!")

    if st.sidebar.button("⚠️ Reset DB"):
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM files")
        conn.execute("DELETE FROM file_columns")
        conn.commit()
        conn.close()
        st.warning("DB reset")


df = load_files()

if df.empty:
    st.warning("No data found. Build catalogue first.")
    st.stop()


# =============================
# FILTERS (POWER BI STYLE)
# =============================
st.sidebar.header("🎛 Filters")

tag_filter = st.sidebar.multiselect(
    "Tags",
    df["tag"].unique().tolist(),
    default=df["tag"].unique().tolist()
)

type_filter = st.sidebar.multiselect(
    "File Type",
    df["extension"].unique().tolist(),
    default=df["extension"].unique().tolist()
)

df = df[(df["tag"].isin(tag_filter)) & (df["extension"].isin(type_filter))]


# =============================
# MAIN TABLE
# =============================
st.subheader("📁 Data Catalogue")
st.dataframe(df, use_container_width=True)


# =============================
# FILE DETAIL VIEW
# =============================
file = st.selectbox("Select file", df["file_name"])

if file:
    rel_path = df[df["file_name"] == file]["file_path"].values[0]

    st.subheader("📌 Metadata")
    st.json(df[df["file_name"] == file].iloc[0].to_dict())

    df_file = load_file(rel_path)

    if df_file is not None:
        st.subheader("🧾 Data Dictionary")
        st.dataframe(data_dictionary(df_file))

        st.subheader("🧪 Data Quality")
        st.json(data_quality(df_file))

        st.subheader("⚠️ Outliers")
        st.json(detect_outliers(df_file))

        st.subheader("👀 Preview")
        st.dataframe(df_file)


# =============================
# 🏢 ENTERPRISE DASHBOARD
# =============================
st.subheader("🏢 Executive Intelligence Dashboard")

conn = sqlite3.connect(DB_PATH)
files_df = pd.read_sql_query("SELECT * FROM files", conn)
cols_df = pd.read_sql_query("SELECT * FROM file_columns", conn)
conn.close()


# KPI ROW
c1, c2, c3, c4 = st.columns(4)

c1.metric("Files", len(files_df))
c2.metric("Columns", len(cols_df))
c3.metric("Tags", files_df["tag"].nunique())
c4.metric("Avg Size KB", round(files_df["size_kb"].mean(), 2))


st.divider()


# =============================
# TREND
# =============================
files_df["scanned_at"] = pd.to_datetime(files_df["scanned_at"])

trend = files_df.groupby(files_df["scanned_at"].dt.date).size().reset_index(name="count")

fig1 = px.line(trend, x="scanned_at", y="count", markers=True, title="Ingestion Trend")
st.plotly_chart(fig1, use_container_width=True)

st.divider()


# =============================
# TAG DISTRIBUTION
# =============================
fig2 = px.bar(files_df["tag"].value_counts().reset_index(),
              x="tag", y="count", title="Data Classification Tags")

st.plotly_chart(fig2, use_container_width=True)


# =============================
# DATA QUALITY VIEW
# =============================
if not cols_df.empty:
    q = cols_df.groupby("file_name").agg(
        missing=("missing_pct", "mean"),
        unique=("unique_count", "mean")
    ).reset_index()

    fig3 = px.scatter(q, x="unique", y="missing",
                      size="missing",
                      color="file_name",
                      title="Data Quality Heatmap")

    st.plotly_chart(fig3, use_container_width=True)


# =============================
# EXECUTIVE INSIGHTS
# =============================
st.subheader("🧾 Executive Insights")

avg_missing = cols_df["missing_pct"].mean() if not cols_df.empty else 0

if avg_missing < 10:
    st.success("🟢 Data ecosystem is healthy")
elif avg_missing < 25:
    st.warning("🟠 Moderate data quality issues detected")
else:
    st.error("🔴 Critical data quality issues detected")

largest = files_df.sort_values("size_kb", ascending=False).head(1)

if not largest.empty:
    st.info(f"📦 Largest dataset: {largest.iloc[0]['file_name']}")


# =============================
# LINEAGE GRAPH
# =============================
st.subheader("🌐 Data Lineage")

conn = sqlite3.connect(DB_PATH)
lineage = pd.read_sql_query("SELECT * FROM lineage", conn)
conn.close()

if not lineage.empty:

    if PYVIS_AVAILABLE:
        G = nx.DiGraph()

        for _, r in lineage.iterrows():
            G.add_edge(r["source_file"], r["output"])

        net = Network(height="500px", width="100%", directed=True)

        for n in G.nodes:
            net.add_node(n, label=n)

        for e in G.edges:
            net.add_edge(e[0], e[1])

        st.components.v1.html(net.generate_html(), height=500)

    else:
        st.graphviz_chart(
            "digraph { " +
            " ".join([f'"{r.source_file}" -> "{r.output}"' for _, r in lineage.iterrows()]) +
            " }"
        )

else:
    st.info("No lineage data available.")