import pandas as pd

def load_and_clean_data():

    # Paths
    leaf_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data\eMobility\nissanLeafsData\mileage\mileageData.csv"
    mg4_path = r"C:\Users\Admin\OneDrive\Operational Intelligence Hub\Data\eMobility\mG4sData\mg4sMileage\mg4sMileage.xlsx"

    # Load data
    leaf = pd.read_csv(leaf_path)
    mg4 = pd.read_excel(mg4_path)

    # -----------------------------
    # 1. CLEAN NISSAN LEAF DATA
    # -----------------------------

    leaf = leaf.copy()

    # Rename columns
    leaf = leaf.rename(columns={
        "Vehicle": "CAR REG",
        "Mileage": "Mileage",
        "Violations": "Violations",
        "CurrentTime": "Date"
    })

    # Convert date
    leaf["Date"] = pd.to_datetime(leaf["Date"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Select required columns only
    leaf = leaf[["Date", "CAR REG", "Mileage", "Violations"]]

    # Add vehicle type
    leaf["Vehicle_Type"] = "Nissan Leaf"


    # -----------------------------
    # 2. CLEAN MG4 DATA
    # -----------------------------

    mg4 = mg4.copy()

    mg4 = mg4.rename(columns={
        "Device Name": "CAR REG",
        "Mileage(km)": "Mileage",
        "Speeding(Times)": "Violations",
        "Date": "Date"
    })

    # Convert date
    mg4["Date"] = pd.to_datetime(mg4["Date"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Select required columns only
    mg4 = mg4[["Date", "CAR REG", "Mileage", "Violations"]]

    # Add vehicle type
    mg4["Vehicle_Type"] = "MG4"


    # -----------------------------
    # 3. COMBINE DATASETS
    # -----------------------------

    df = pd.concat([leaf, mg4], ignore_index=True)

    # Save final clean file
    df.to_excel("fleet_master_data.xlsx", index=False)

    return df


df = load_and_clean_data()
print(df.head())