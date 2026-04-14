import streamlit as st
import os
import pandas as pd

# -----------------------------
# CONFIG (CLOUD FIX APPLIED)
# -----------------------------
BASE_DIR = "Data"  # ✅ FIX: use relative path for Streamlit Cloud

st.set_page_config(page_title="Data Catalogue", layout="wide")
st.title("📊 Data Catalogue Explorer")

# -----------------------------
# CHECK DIRECTORY
# -----------------------------
if not os.path.exists(BASE_DIR):
    st.error("❌ Folder path does not exist. Please ensure 'Data' folder exists in your repository.")
    st.stop()

# -----------------------------
# FUNCTIONS
# -----------------------------

def list_files(folder):
    file_list = []
    for root, dirs, files in os.walk(folder):
        for f in files:
            full_path = os.path.join(root, f)
            file_list.append(full_path)
    return file_list


def file_info(file_path):
    return {
        "File Name": os.path.basename(file_path),
        "Path": file_path,
        "Size (KB)": round(os.path.getsize(file_path) / 1024, 2),
        "Extension": os.path.splitext(file_path)[1].lower()
    }


def read_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".csv":
            return pd.read_csv(file_path)

        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(file_path)

        elif ext == ".txt":
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()

        else:
            return None
    except Exception as e:
        return f"Error reading file: {e}"


# -----------------------------
# LOAD FILES
# -----------------------------
all_files = list_files(BASE_DIR)

# -----------------------------
# SIDEBAR - FILTERS
# -----------------------------
st.sidebar.header("📁 Catalogue Filters")

search_query = st.sidebar.text_input("🔍 Search file name")

file_types = sorted(list(set([os.path.splitext(f)[1].lower() for f in all_files])))
selected_types = st.sidebar.multiselect(
    "📂 File types",
    file_types,
    default=file_types
)

filtered_files = [
    f for f in all_files
    if (search_query.lower() in os.path.basename(f).lower())
    and (os.path.splitext(f)[1].lower() in selected_types)
]

st.sidebar.write(f"📄 Total Files: {len(filtered_files)}")

selected_file = st.sidebar.selectbox(
    "Select a file",
    filtered_files if filtered_files else ["No files found"]
)

# -----------------------------
# MAIN VIEW
# -----------------------------
if selected_file and selected_file != "No files found":

    st.subheader("📄 File Metadata")
    st.json(file_info(selected_file))

    st.subheader("👀 Preview")

    content = read_file(selected_file)

    if isinstance(content, pd.DataFrame):
        st.dataframe(content, use_container_width=True)

    elif isinstance(content, str):
        st.text(content)

    else:
        st.warning("⚠️ Preview not supported for this file type.")

else:
    st.info("No file selected or no matching files found.")