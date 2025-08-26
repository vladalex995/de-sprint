from pathlib import Path
from datetime import date, timedelta
import numpy as np, pandas as pd, duckdb

BASE = Path(__file__).resolve().parent
OPS = BASE / "ops"; OPS.mkdir(exist_ok=True)
DB = OPS / "retail.duckdb"
con = duckdb.connect(str(DB))

# tiny lookup tables
n_cust = 200
customers = pd.DataFrame({
    "customer_id": range(1, n_cust+1),
    "name": [f"Customer {i}" for i in range(1, n_cust+1)],
    "email": [f"user{i}@example.com" for i in range(1, n_cust+1)],
})
products = pd.DataFrame([
    (1,"T-Shirt","Apparel",19.0),(2,"Hoodie","Apparel",39.0),
    (3,"Mug","Home",12.5),(4,"Sticker","Accessories",3.0),(5,"Cap","Apparel",15.0)
], columns=["product_id","product_name","category","price"])

# 30 days of synthetic orders
days = 30
start = date.today() - timedelta(days=days-1)
rng = np.random.default_rng(42)
rows, oid = [], 1
for i in range(days):
    d = start + timedelta(days=i)
    for _ in range(int(rng.integers(20,60))):
        pid = int(rng.integers(1,6))
        qty = int(rng.integers(1,4))
        price = float(products.loc[products.product_id==pid,"price"].iloc[0])
        rows.append({
            "order_id": oid, "order_date": pd.to_datetime(d),
            "customer_id": int(rng.integers(1, n_cust+1)),
            "product_id": pid, "qty": qty, "unit_price": price,
            "order_total": round(price*qty,2), "status": "paid",
        })
        oid += 1
orders = pd.DataFrame(rows)

for t in ("customers","products","orders"):
    con.execute(f"DROP TABLE IF EXISTS {t}")
con.register("c", customers); con.execute("CREATE TABLE customers AS SELECT * FROM c")
con.register("p", products);  con.execute("CREATE TABLE products  AS SELECT * FROM p")
con.register("o", orders);    con.execute("CREATE TABLE orders    AS SELECT * FROM o")
con.close()
print(f"Seeded {DB}")
