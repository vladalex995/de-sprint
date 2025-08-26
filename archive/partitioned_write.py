from pathlib import Path
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import duckdb

# 0) Read the Parquet you already created earlier
src = Path("data.parquet")
assert src.exists(), f"Missing {src}! Run pipeline.py first."
df = pd.read_parquet(src)

# 1) Ensure there is a date column to partition by
if "date" not in df.columns:
    df["date"] = pd.date_range("2024-01-01", periods=len(df), freq="D")

df["year"]  = df["date"].dt.year.astype(int)
df["month"] = df["date"].dt.month.astype(int)
df["day"]   = df["date"].dt.day.astype(int)

# 2) Write a partitioned dataset
outdir = Path("parquet_part")
if outdir.exists():
    shutil.rmtree(outdir)  # start clean so we know what gets written

table = pa.Table.from_pandas(df, preserve_index=False)
ds.write_dataset(
    table,
    base_dir=str(outdir),
    format="parquet",
    partitioning=["year", "month", "day"],
)

print("Wrote partitioned dataset to:", outdir.resolve())

# 3) Show the files that were created (so you can see the structure)
files = sorted(outdir.rglob("*.parquet"))
for f in files:
    print(" -", f)

# 4) Verify with DuckDB (use ** so the glob recurses into year=/month=/day=/)
con = duckdb.connect()
print(
    con.execute("""
        SELECT year, month, day, COUNT(*) AS rows
        FROM parquet_scan('parquet_part/**/*.parquet')
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """).df()
)
