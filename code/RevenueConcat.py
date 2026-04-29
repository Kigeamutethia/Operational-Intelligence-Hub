import pandas as pd

def load_data():
    leaf_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data\eMobility\nissanLeafsData\revenue\revenueData.xlsx"
    mg4_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data\eMobility\mG4sData\mg4sRevenue\revenueData.xlsx"

    leaf = pd.read_excel(leaf_path)
    mg4 = pd.read_excel(mg4_path)

    # 1. Clean column names (VERY IMPORTANT)
    leaf.columns = leaf.columns.str.strip().str.upper()
    mg4.columns = mg4.columns.str.strip().str.upper()

    # 2. Standard column set (force alignment)
    base_cols = [
        "DATE",
        "CAR REG",
        "NAME",
        "FLAT RATE",
        "REBATE",
        "REVENUE EARNED (TO INVOICE)",
        "BAL C/D",
        "PAID",
        "BAL B/D"
    ]

    # 3. Keep only needed columns (avoid duplication chaos)
    leaf = leaf[[c for c in base_cols if c in leaf.columns]].copy()
    mg4 = mg4[[c for c in base_cols if c in mg4.columns]].copy()

    # 4. Add vehicle type
    leaf["VEHICLE_TYPE"] = "NISSAN LEAF"
    mg4["VEHICLE_TYPE"] = "MG4"

    # 5. Ensure same column order
    leaf = leaf.reindex(columns=base_cols + ["VEHICLE_TYPE"])
    mg4 = mg4.reindex(columns=base_cols + ["VEHICLE_TYPE"])

    # 6. Combine cleanly
    df = pd.concat([leaf, mg4], ignore_index=True)

    return df

df = load_data()

# Save clean master file
df.to_excel("FleetRevenue.xlsx", index=False)