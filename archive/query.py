# Day 2 — Query Parquet with DuckDB (robust version)
import duckdb
import pandas as pd

con = duckdb.connect()

# 0) Peek at the data
print("\nHEAD (first 5 rows):")
print(con.execute("SELECT * FROM 'data.parquet' LIMIT 5").df())

# 1) Count rows (works for any table)
print("\nROW COUNT:")
print(con.execute("SELECT COUNT(*) AS rows FROM 'data.parquet'").df())

# 2) Discover columns so we can write a useful query dynamically
cols = [c.lower() for c in con.execute("SELECT * FROM 'data.parquet' LIMIT 0").df().columns]
print("\nCOLUMNS:", cols)

# Helpers: pick a 'day/date' column and a numeric metric
day_col = next((c for c in ["day","date","dt"] if c in cols), None)
num_col = next((c for c in ["sales","amount","value","revenue","example_metric"] if c in cols), None)

# 3) Grouped aggregation (only if we found suitable columns)
if day_col and num_col:
    q_group = f"""
      SELECT {day_col} AS day_key, SUM(CAST({num_col} AS DOUBLE)) AS total_metric
      FROM 'data.parquet'
      GROUP BY 1
      ORDER BY total_metric DESC
      LIMIT 10
    """
    print("\nTOP 10 by metric:")
    print(con.execute(q_group).df())

    # 4) Window function (moving sum over time)
    # If 'date' exists we sort by it; otherwise by 'day'
    order_col = "date" if "date" in cols else day_col
    q_window = f"""
      SELECT
        {order_col} AS t,
        CAST({num_col} AS DOUBLE) AS metric,
        SUM(CAST({num_col} AS DOUBLE))
          OVER (ORDER BY {order_col}
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS metric_ma3
      FROM 'data.parquet'
      ORDER BY {order_col}
      LIMIT 30
    """
    print("\nWINDOW (3-row moving sum):")
    print(con.execute(q_window).df())

    # 5) Save a CSV result you can open
    df_out = con.execute(q_group).df()
    df_out.to_csv("top_days.csv", index=False)
    print("\nWrote: top_days.csv")
else:
    print("\nNOTE:")
    print("I couldn’t find obvious columns like day/date + sales/amount/value/revenue.")
    print("The file is still queryable — try:")
    print("  SELECT * FROM 'data.parquet' LIMIT 5;")
    print("  SELECT some_numeric_col, COUNT(*) FROM 'data.parquet' GROUP BY 1;")
