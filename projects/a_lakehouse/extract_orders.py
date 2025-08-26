from pathlib import Path
from datetime import date
import argparse, duckdb, pandas as pd

BASE = Path(__file__).resolve().parent
OPS_DB = BASE / "ops" / "retail.duckdb"
RAW = BASE / "data" / "raw" / "db" / "orders"

def extract_for_day(day: str) -> Path:
    out_dir = RAW / day; out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "orders.csv"
    con = duckdb.connect(str(OPS_DB))
    sql = f"""
      SELECT o.order_id, CAST(o.order_date AS DATE) AS order_date,
             o.customer_id, c.name AS customer_name, c.email AS customer_email,
             o.product_id, p.product_name, p.category,
             o.qty, o.unit_price, o.order_total, o.status
      FROM orders o
      JOIN customers c USING (customer_id)
      JOIN products  p USING (product_id)
      WHERE CAST(o.order_date AS DATE) = DATE '{day}';
    """
    df = con.execute(sql).df(); con.close()
    df.to_csv(out_path, index=False)
    print(f"Wrote RAW: {out_path} rows={len(df)}")
    return out_path

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=str(date.today()))
    args = ap.parse_args()
    extract_for_day(args.date)
