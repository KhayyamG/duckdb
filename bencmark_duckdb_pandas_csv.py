#!/usr/bin/env python
import sys
import time
from pathlib import Path
import duckdb
import pandas as pd

DEFAULT_CSV = Path(__file__).parent / "data" / "customers-2000000.csv"
TOP_N = 5

csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
csv_path = csv_path.expanduser().resolve()

if not csv_path.exists():
    raise SystemExit(f"CSV not found: {csv_path}")

print(f"Using CSV: {csv_path}")
print(f"Top {TOP_N} companies by row count")

start = time.perf_counter()
duck_rows = duckdb.execute(
    "SELECT company, COUNT(*) AS rows FROM read_csv_auto(?, HEADER=TRUE) "
    "GROUP BY company ORDER BY rows DESC LIMIT ?",
    [str(csv_path), TOP_N],
).fetchall()
duck_time = time.perf_counter() - start

print("DuckDB")
for company, rows in duck_rows:
    print(f"  {company}: {rows}")
print(f"  processing time: {duck_time:.3f}s")

p_start = time.perf_counter()
df = pd.read_csv(str(csv_path), usecols=["Company"])
counts = df["Company"].value_counts().head(TOP_N)
pandas_rows = list(zip(counts.index, counts.values))
p_time = time.perf_counter() - p_start

print("Pandas")
for company, rows in pandas_rows:
    print(f"  {company}: {rows}")
print(f"  processing time: {p_time:.3f}s")

if duck_time <= p_time:
    print(f"DuckDB was {p_time / duck_time:.2f}x faster.")
else:
    print(f"Pandas was {duck_time / p_time:.2f}x faster.")
