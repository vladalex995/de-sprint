from pathlib import Path
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq

BASE = Path(__file__).resolve().parent
RAW_ROOT = BASE / "data" / "raw" / "db" / "orders"
BRONZE_ROOT = BASE / "data" / "bronze" / "orders"

def csv_to_partitioned_parquet(day: str) -> Path:
    y, m, d = day.split("-")
    src = RAW_ROOT / day / "orders.csv"
    out_dir = BRONZE_ROOT / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "orders.parquet"
    table = pa.Table.from_pandas(pd.read_csv(src), preserve_index=False)
    pq.write_table(table, out)
    print(f"Wrote BRONZE: {out}")
    return out

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    a = p.parse_args()
    csv_to_partitioned_parquet(a.date)
