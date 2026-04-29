import pandas as pd
import os

# ==============================
# 1. FILE PATH
# ==============================
base_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data"

charging_path = base_path + r"\eChargeAfrica\chargingInfraData\ops-Data\units-Data.xlsx"

print("Working directory:", os.getcwd())

# ==============================
# 2. LOAD DATA
# ==============================
charging = pd.read_excel(charging_path)

# ==============================
# 3. CLEAN COLUMN NAMES
# ==============================
charging.columns = charging.columns.str.strip()

# ==============================
# 4. VRN CLEANING FUNCTION
# ==============================
def clean_vrn(vrn):
    if pd.isna(vrn):
        return None
    
    vrn = str(vrn).upper().replace(" ", "")
    
    # Format: first 3 letters + space + rest
    if len(vrn) >= 6:
        return vrn[:3] + " " + vrn[3:]
    
    return vrn

charging['Vehicle'] = charging['VRN'].apply(clean_vrn)

# ==============================
# 5. FIX DATES
# ==============================
charging['Transaction Date'] = pd.to_datetime(charging['Transaction Date'], errors='coerce')

# Remove time → keep only date
charging['Date'] = charging['Transaction Date'].dt.date
charging['Date'] = pd.to_datetime(charging['Date'])

# ==============================
# 6. CLEAN NUMERIC COLUMNS
# ==============================
def to_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

charging = to_numeric(charging, ['Units Consumed(kWh)', 'Total Amount'])

# ==============================
# 7. AGGREGATE DATA (PER VEHICLE PER DAY)
# ==============================
charging_grouped = charging.groupby(['Vehicle', 'Date'], as_index=False).agg({
    'Units Consumed(kWh)': 'sum',
    'Total Amount': 'sum'
})

# ==============================
# 8. DEBUG CHECKS
# ==============================
print("\n--- Sample Cleaned VRNs ---")
print(charging[['VRN', 'Vehicle']].drop_duplicates().head(10))

invalid_vrns = charging[charging['Vehicle'].str.len() < 7]
print("\n--- Invalid VRNs ---")
print(invalid_vrns[['VRN', 'Vehicle']].drop_duplicates())

# ==============================
# 9. SORT DATA
# ==============================
charging_grouped = charging_grouped.sort_values(by=['Vehicle', 'Date'])

# ==============================
# 10. EXPORT
# ==============================
output_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\code\FleetCharging.xlsx"
charging_grouped.to_excel(output_path, index=False)

print("\n✅ Charging data processing complete!")
print(f"📁 File saved to: {output_path}")

# ==============================
# 11. PREVIEW
# ==============================
print("\n--- Final Data Preview ---")
print(charging_grouped.head())