# projects/a_lakehouse/health_check.py
from pathlib import Path
import argparse, duckdb

BASE = Path(__file__).resolve().parent
WAREHOUSE_DB = BASE / "warehouse" / "warehouse.duckdb"

def check_day(day: str) -> None:
    con = duckdb.connect(str(WAREHOUSE_DB))
    # inline the date literal
    sql = f"SELECT COUNT(*) FROM fact_orders WHERE order_date = DATE '{day}';"
    rows = con.execute(sql).fetchone()[0]
    con.close()
    if rows == 0:
        raise SystemExit(f"FAIL: no rows for {day}")
    print(f"OK: {rows} rows for {day}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = p.parse_args()
    check_day(args.date)
