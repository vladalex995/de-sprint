from pathlib import Path
import duckdb

BASE = Path(__file__).resolve().parent
BRONZE = BASE / "data" / "bronze" / "orders"
WAREHOUSE_DIR = BASE / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = WAREHOUSE_DIR / "warehouse.duckdb"

PARQUET_GLOB = str(BRONZE / "year=*" / "month=*" / "day=*" / "orders.parquet")

con = duckdb.connect(str(DB_PATH))

# Embed the path literal; do NOT use parameters here.
con.execute(f"""
CREATE OR REPLACE VIEW bronze_orders AS
SELECT * FROM read_parquet('{PARQUET_GLOB}');
""")

con.execute("""
CREATE OR REPLACE TABLE dim_customer AS
SELECT
  customer_id,
  any_value(customer_name)  AS customer_name,
  any_value(customer_email) AS customer_email
FROM bronze_orders
GROUP BY 1;
""")

con.execute("""
CREATE OR REPLACE TABLE dim_product AS
SELECT
  product_id,
  any_value(product_name) AS product_name,
  any_value(category)     AS category
FROM bronze_orders
GROUP BY 1;
""")

con.execute("""
CREATE OR REPLACE TABLE fact_orders AS
SELECT
  order_id,
  CAST(order_date AS DATE) AS order_date,
  customer_id,
  product_id,
  qty,
  unit_price,
  order_total,
  status
FROM bronze_orders;
""")

for t in ("dim_customer", "dim_product", "fact_orders"):
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {n}")

con.close()
print(f"[OK] Wrote warehouse at {DB_PATH}")
