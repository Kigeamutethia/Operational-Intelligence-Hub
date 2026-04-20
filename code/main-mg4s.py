import pandas as pd
import os
import numpy as np
import re

# ==============================
# 1. FILE PATHS
# ==============================
base_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data"

revenue_path = base_path + r"\eMobility\mG4sData\mg4sRevenue\revenueData.xlsx"
mileage_path = base_path + r"\eMobility\mG4sData\mg4sMileage\mg4sMileage.xlsx"
charging_path = base_path + r"\eChargeAfrica\chargingInfraData\ops-Data\units-Data.xlsx"

print("Working directory:", os.getcwd())

# ==============================
# 2. LOAD DATA
# ==============================
revenue = pd.read_excel(revenue_path)
mileage = pd.read_excel(mileage_path)
charging = pd.read_excel(charging_path)

# ==============================
# 3. CLEAN COLUMN NAMES
# ==============================
for df in [revenue, mileage, charging]:
    df.columns = df.columns.str.strip()

# ==============================
# 4. VRN CLEANING (ROBUST)
# ==============================
def clean_vrn(vrn):
    if pd.isna(vrn):
        return None
    
    vrn = str(vrn).upper()
    vrn = re.sub(r"\s+", "", vrn)  # remove all spaces
    
    # format: 3 letters + rest
    match = re.match(r"([A-Z]{3})([A-Z0-9]+)", vrn)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    
    return vrn

revenue['Vehicle'] = revenue['CAR REG'].apply(clean_vrn)
mileage['Vehicle'] = mileage['Device Name'].apply(clean_vrn)
charging['Vehicle'] = charging['VRN'].apply(clean_vrn)

# ==============================
# 5. DATE FIX (STANDARDIZED)
# ==============================
revenue['Date'] = pd.to_datetime(revenue['Date'], errors='coerce').dt.normalize()
mileage['Date'] = pd.to_datetime(mileage['Date'], errors='coerce').dt.normalize()
charging['Date'] = pd.to_datetime(charging['Transaction Date'], errors='coerce').dt.normalize()

# ==============================
# 6. NUMERIC CLEANING
# ==============================
def to_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

revenue = to_numeric(revenue, [
    'TARGET', 'REBATE', 'REVENUE EARNED (TO INVOICE)',
    'BAL C/D', 'Paid', 'BAL B/D'
])

mileage = to_numeric(mileage, ['Mileage(km)', 'Fuel(L)'])
charging = to_numeric(charging, ['Units Consumed(kWh)', 'Total Amount'])

# ==============================
# 7. AGGREGATION
# ==============================
charging_grouped = charging.groupby(['Vehicle', 'Date'], as_index=False).sum(numeric_only=True)
mileage_grouped = mileage.groupby(['Vehicle', 'Date'], as_index=False).sum(numeric_only=True)

# ==============================
# 8. MERGE
# ==============================
df = revenue.merge(mileage_grouped, on=['Vehicle', 'Date'], how='left')
df = df.merge(charging_grouped, on=['Vehicle', 'Date'], how='left')

# ==============================
# 9. KPIs (SAFE CALCULATION)
# ==============================
df['Revenue_per_km'] = np.where(
    df['Mileage(km)'] > 0,
    df['REVENUE EARNED (TO INVOICE)'] / df['Mileage(km)'],
    np.nan
)

df['Cost_per_km'] = np.where(
    df['Mileage(km)'] > 0,
    df['Total Amount'] / df['Mileage(km)'],
    np.nan
)

df['km_per_kWh'] = np.where(
    df['Units Consumed(kWh)'] > 0,
    df['Mileage(km)'] / df['Units Consumed(kWh)'],
    np.nan
)

df['Profit'] = df['REVENUE EARNED (TO INVOICE)'] - df['Total Amount']

# ==============================
# 10. CLEAN INF
# ==============================
df.replace([np.inf, -np.inf], np.nan, inplace=True)

# ==============================
# 11. SORT
# ==============================
df = df.sort_values(['Vehicle', 'Date'])

# ==============================
# 12. EXPORT
# ==============================
output_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\code\mg4s_master_data.xlsx"
df.to_excel(output_path, index=False)

print("\n✅ Processing complete!")
print(f"📁 File saved to: {output_path}")

# ==============================
# 13. DEBUG
# ==============================
print("\n--- Sample VRNs ---")
print(charging[['VRN', 'Vehicle']].drop_duplicates().head(10))

print("\n--- Missing KPI risk rows ---")
print(df[df['Mileage(km)'].isna()][['Vehicle', 'Date']].head())