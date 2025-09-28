#!/usr/bin/env python
"""Top 5 companies by row count from a Parquet file using DuckDB and Spark."""

import time
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession, functions as F

TOP_N = 5
DEFAULT_PARQUET = Path(__file__).resolve().parent / "data" / "customers.parquet"

parquet_path = DEFAULT_PARQUET.expanduser().resolve()

if not parquet_path.exists():
    raise SystemExit(f"Parquet not found: {parquet_path}")

print(f"Using Parquet: {parquet_path}")
print(f"Top {TOP_N} companies by row count")

# DuckDB aggregation with separate setup/processing timings
duck_setup_start = time.perf_counter()
duck_conn = duckdb.connect()
duck_setup_time = time.perf_counter() - duck_setup_start

duck_proc_start = time.perf_counter()
duck_rows = duck_conn.execute(
    (
        'SELECT company, COUNT(*) AS rows '
        'FROM read_parquet(?) '
        'GROUP BY company ORDER BY rows DESC LIMIT ?'
    ),
    [str(parquet_path), TOP_N],
).fetchall()
duck_proc_time = time.perf_counter() - duck_proc_start
duck_total_time = duck_setup_time + duck_proc_time
duck_conn.close()

print('DuckDB')
for company, rows in duck_rows:
    print(f'  {company}: {rows}')
print(f'  setup time: {duck_setup_time:.3f}s')
print(f'  processing time: {duck_proc_time:.3f}s')
print(f'  total time: {duck_total_time:.3f}s')

# Spark aggregation with separate setup/processing timings
setup_start = time.perf_counter()
spark = SparkSession.builder.master("local[*]").appName("TopCompaniesParquet").getOrCreate()
spark_setup_time = time.perf_counter() - setup_start

proc_start = time.perf_counter()
spark_df = spark.read.parquet(str(parquet_path))
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

if duck_proc_time > 0 and spark_proc_time > 0:
    if duck_proc_time <= spark_proc_time:
        print(f'DuckDB was {spark_proc_time / duck_proc_time:.2f}x faster (processing only).')
    else:
        print(f'Spark was {duck_proc_time / spark_proc_time:.2f}x faster (processing only).')
