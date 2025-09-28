#!/usr/bin/env python
"""List top 5 companies by row count using DuckDB and Spark."""

import sys
import time
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession, functions as F

DEFAULT_CSV = Path(__file__).parent / "data" / "customers-2000000.csv"
TOP_N = 5

csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
csv_path = csv_path.expanduser().resolve()

if not csv_path.exists():
    raise SystemExit(f"CSV not found: {csv_path}")

print(f"Using CSV: {csv_path}")
print(f"Top {TOP_N} companies by row count")

# DuckDB aggregation
start = time.perf_counter()
duck_rows = duckdb.execute(
    (
        "SELECT company, COUNT(*) AS rows "
        "FROM read_csv_auto(?, HEADER=TRUE) "
        "GROUP BY company ORDER BY rows DESC LIMIT ?"
    ),
    [str(csv_path), TOP_N],
).fetchall()
duck_time = time.perf_counter() - start

print("DuckDB")
for company, rows in duck_rows:
    print(f"  {company}: {rows}")
print(f"  processing time: {duck_time:.3f}s")

# Spark aggregation with separate setup/processing timings
setup_start = time.perf_counter()
spark = SparkSession.builder.master("local[*]").appName("TopCompanies").getOrCreate()
spark_setup_time = time.perf_counter() - setup_start

proc_start = time.perf_counter()
spark_df = spark.read.option("header", "true").csv(str(csv_path))
spark_rows = (
    spark_df.groupBy("company")
    .agg(F.count("*").alias("rows"))
    .orderBy(F.desc("rows"))
    .limit(TOP_N)
    .collect()
)
spark_proc_time = time.perf_counter() - proc_start
spark_total_time = spark_setup_time + spark_proc_time
spark.stop()

print("Spark")
for row in spark_rows:
    print(f"  {row['company']}: {row['rows']}")
print(f"  setup time: {spark_setup_time:.3f}s")
print(f"  processing time: {spark_proc_time:.3f}s")
print(f"  total time: {spark_total_time:.3f}s")

if duck_time > 0 and spark_proc_time > 0:
    if duck_time <= spark_proc_time:
        print(f"DuckDB was {spark_proc_time / duck_time:.2f}x faster (processing only).")
    else:
        print(f"Spark was {duck_time / spark_proc_time:.2f}x faster (processing only).")
