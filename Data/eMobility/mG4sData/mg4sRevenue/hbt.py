import pandas as pd

# List of vehicles
vrns = [
    "KDT 162R", "KDT 087R", "KDT 084R",
    "KDT 160R", "KDT 150R", "KDT 149R", "KDT 136R"
]

# Create date range
dates = pd.date_range(start="2026-01-04", end="2026-05-31")

# Create dataset
data = []

for date in dates:
    for vrn in vrns:
        data.append({
            "Date": date.strftime("%d/%m/%Y"),
            "VRN": vrn,
            "Revenue Invoiced": 4700.00
        })

df = pd.DataFrame(data)

# Save to Excel
df.to_excel("HBT.xlsx", index=False)

print(df.head())