import streamlit as st
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

import networkx as nx
from pyvis.network import Network

# =============================
# CONFIG
# =============================
BASE_DIR = os.path.join(os.getcwd(), "Data")
DB_PATH = "metadata.db"

st.set_page_config(
    page_title="Drivelectric Enterprise Data Governance Platform",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Drivelectric Purview-Lite++")

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
    """
    file_path is RELATIVE to BASE_DIR (NO 'Data/' prefix stored in DB)
    """
    full_path = os.path.join(BASE_DIR, file_path)
    full_path = os.path.normpath(full_path)

    if not os.path.exists(full_path):
        st.error(f"File not found: {full_path}")
        return None

    try:
        ext = os.path.splitext(full_path)[1].lower()

        if ext == ".csv":
            return pd.read_csv(full_path, low_memory=False)
        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(full_path, engine="openpyxl")
    except Exception as e:
        st.error(f"Error loading file {full_path}: {e}")

    return None

# =============================
# DATA QUALITY
# =============================
def data_quality(df):
    return {
        col: {
            "missing_%": round(df[col].isna().mean() * 100, 2),
            "unique": int(df[col].nunique())
        }
        for col in df.columns
    }

# =============================
# OUTLIERS
# =============================
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

# =============================
# DATA DICTIONARY
# =============================
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
# SCAN DATASET (FIXED PATH LOGIC)
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

                # IMPORTANT: store path RELATIVE to BASE_DIR ONLY
                rel_path = os.path.relpath(abs_path, BASE_DIR)
                rel_path = rel_path.replace("\\", "/")

                size = round(os.path.getsize(abs_path) / 1024, 2)

                df = None
                try:
                    df = load_file(rel_path)
                except:
                    pass

                columns = list(df.columns) if df is not None else []
                tag = auto_tag(f, columns)

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

            except Exception as e:
                st.warning(f"Failed processing {f}: {e}")

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
        conn,
        params=(file,)
    )
    conn.close()
    return df

# =============================
# UI CONTROLS
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
    st.warning("No metadata found. Build catalogue first.")
    st.stop()

# =============================
# SEARCH
# =============================
st.sidebar.header("🔍 Search")
query = st.sidebar.text_input("Search")

if query:
    df = df[df["file_name"].str.contains(query, case=False, na=False)]

# =============================
# FILTERS
# =============================
st.sidebar.header("🏷 Tags")

tags = df["tag"].dropna().unique().tolist()
selected_tags = st.sidebar.multiselect("Filter Tags", tags, default=tags)

df = df[df["tag"].isin(selected_tags)]

# =============================
# MAIN TABLE
# =============================
st.subheader("📁 Data Catalogue")
st.dataframe(df, use_container_width=True)

# =============================
# FILE EXPLORER
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

        st.subheader("🧪 Outliers")
        st.json(detect_outliers(df_file))

        st.subheader("👀 Preview")
        st.dataframe(df_file)

# =============================
# KPI DASHBOARD
# =============================
st.subheader("📈 KPI Dashboard")

conn = sqlite3.connect(DB_PATH)
files_df = pd.read_sql_query("SELECT * FROM files", conn)
cols_df = pd.read_sql_query("SELECT * FROM file_columns", conn)
conn.close()

st.metric("Total Files", len(files_df))
st.metric("Total Columns", len(cols_df))
st.metric("Tags", files_df["tag"].nunique())

# =============================
# LINEAGE GRAPH
# =============================
st.subheader("🌐 Lineage Graph")

conn = sqlite3.connect(DB_PATH)
lineage = pd.read_sql_query("SELECT * FROM lineage", conn)
conn.close()

if not lineage.empty:
    G = nx.DiGraph()

    for _, r in lineage.iterrows():
        G.add_edge(r["source_file"], r["output"])

    net = Network(height="500px", width="100%", directed=True)

    for node in G.nodes:
        net.add_node(node, label=node)

    for edge in G.edges:
        net.add_edge(edge[0], edge[1])

    net.save_graph("graph.html")

    st.components.v1.html(open("graph.html").read(), height=500)

else:
    st.info("No lineage data yet.")
