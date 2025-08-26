import pandas as pd
from pathlib import Path

# If you have a CSV (e.g., as1-bank.csv), point to it; otherwise we make a tiny demo table
csv = Path("as1-bank.csv")
if csv.exists():
    df = pd.read_csv(csv, low_memory=False)
else:
    df = pd.DataFrame({"day":[1,2,3,4,5,6,7], "sales":[10,20,15,25,30,18,22]})

df.to_parquet("data.parquet", index=False)
print("Wrote data.parquet | rows:", len(df))
print(df.head())
