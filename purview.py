import streamlit as st
import os
import sqlite3
import pandas as pd
from datetime import datetime

# =============================
# CONFIG
# =============================
BASE_DIR = "Data"
DB_PATH = "metadata.db"

st.set_page_config(
    page_title="Enterprise Data Catalogue",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Enterprise Data Catalogue (Stable Version)")

# =============================
# LOGIN SYSTEM (simple)
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
# INIT DB (SAFE SCHEMA)
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
# AUTO TAGGING
# =============================
def auto_tag(file_name, columns):
    text = (file_name + " " + " ".join(columns)).lower()

    if "fuel" in text or "kwh" in text or "energy" in text:
        return "Energy"
    elif "driver" in text or "vehicle" in text or "fleet" in text:
        return "Fleet"
    elif "invoice" in text or "cost" in text or "payment" in text:
        return "Finance"
    else:
        return "General"

# =============================
# COLUMN PROFILING
# =============================
def profile_columns(df):
    profile = {}
    for col in df.columns:
        profile[col] = {
            "missing_pct": round(df[col].isna().mean() * 100, 2),
            "unique_count": int(df[col].nunique())
        }
    return profile

# =============================
# SCAN FILES (FULL REBUILD)
# =============================
def scan_folder():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("DELETE FROM files")
    conn.execute("DELETE FROM file_columns")

    for root, dirs, files in os.walk(BASE_DIR):
        for f in files:

            if f.startswith("~$"):
                continue

            path = os.path.join(root, f)
            ext = os.path.splitext(f)[1].lower()

            try:
                size_kb = round(os.path.getsize(path) / 1024, 2)

                df = None
                columns = []

                # read safely
                if ext == ".csv":
                    df = pd.read_csv(path, low_memory=False)

                elif ext in [".xlsx", ".xls"]:
                    df = pd.read_excel(path, engine="openpyxl")

                if df is not None:
                    columns = list(df.columns)
                    profile = profile_columns(df)
                else:
                    profile = {}

                tag = auto_tag(f, columns)

                # insert file metadata
                conn.execute("""
                INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    f,
                    path,
                    ext,
                    size_kb,
                    tag,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))

                # insert column metadata
                for col in columns:
                    conn.execute("""
                    INSERT INTO file_columns VALUES (?, ?, ?, ?, ?)
                    """, (
                        f,
                        col,
                        str(df[col].dtype),
                        profile[col]["missing_pct"],
                        profile[col]["unique_count"]
                    ))

            except Exception as e:
                st.warning(f"Skipped {f}: {e}")

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


def load_columns(file_name):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM file_columns WHERE file_name = ?",
        conn,
        params=(file_name,)
    )
    conn.close()
    return df

# =============================
# CONTROLS
# =============================
st.sidebar.header("⚙️ Controls")

if role == "admin":
    if st.sidebar.button("🔄 Build / Refresh Catalogue"):
        scan_folder()
        st.success("Catalogue rebuilt!")

df = load_files()

if df.empty:
    st.info("Click 'Build Catalogue' (Admin only)")
    st.stop()

# =============================
# SEARCH
# =============================
st.sidebar.header("🔍 Search")
query = st.sidebar.text_input("Search files")

if query:
    df = df[df["file_name"].str.contains(query, case=False, na=False)]

# =============================
# TAG FILTER (SAFE FIX)
# =============================
st.sidebar.header("🏷 Filters")

if "tag" in df.columns:
    tags = df["tag"].dropna().unique().tolist()
else:
    tags = []

selected_tags = st.sidebar.multiselect("Tags", tags, default=tags)

if "tag" in df.columns and selected_tags:
    df = df[df["tag"].isin(selected_tags)]

# =============================
# MAIN TABLE
# =============================
st.subheader("📁 Data Catalogue")
st.dataframe(df, use_container_width=True)

# =============================
# FILE EXPLORER
# =============================
st.subheader("📄 Explorer")

selected_file = st.selectbox("Select file", df["file_name"])

if selected_file:

    file_path = df[df["file_name"] == selected_file]["file_path"].values[0]

    st.markdown("### 📌 Metadata")
    st.json(df[df["file_name"] == selected_file].iloc[0].to_dict())

    st.markdown("### 🧠 Column Metadata")
    st.dataframe(load_columns(selected_file), use_container_width=True)

    st.markdown("### 👀 Preview")

    try:
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".csv":
            st.dataframe(pd.read_csv(file_path, low_memory=False))

        elif ext in [".xlsx", ".xls"]:
            st.dataframe(pd.read_excel(file_path, engine="openpyxl"))

        else:
            st.warning("Preview not supported.")

    except Exception as e:
        st.error(f"Error reading file: {e}")