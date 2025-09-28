#!/usr/bin/env python
import time
from pathlib import Path
from collections import defaultdict

import duckdb
from pyspark.sql import SparkSession, functions as F

TOP_N = 5
DEFAULT_PARQUET = Path(__file__).resolve().parent / "data" / "customers.parquet"
parquet_path = DEFAULT_PARQUET.expanduser().resolve()

if not parquet_path.exists():
    raise SystemExit(f"Parquet not found: {parquet_path}")

print(f"Using Parquet: {parquet_path}")
print(f"Top {TOP_N} companies by row count")

# ---------- DuckDB (referans) ----------
start = time.perf_counter()
duck_rows = duckdb.execute(
    "SELECT company, COUNT(*) AS rows FROM read_parquet(?) "
    "GROUP BY company ORDER BY rows DESC LIMIT ?",
    [str(parquet_path), TOP_N],
).fetchall()
duck_time = time.perf_counter() - start

print("DuckDB")
for company, rows in duck_rows:
    print(f"  {company}: {rows}")
print(f"  processing time: {duck_time:.3f}s")

# ---------- Spark (hız odaklı) ----------
setup_start = time.perf_counter()
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TopCompaniesParquetFast")
    # Yerel ortam için daha az shuffle partition
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Sort-based aggregate tercih (bellek taşması riskini azaltır)
    .config("spark.sql.execution.useHashAggregateExec", "false")
    .config("spark.sql.execution.useObjectHashAggregate", "false")
    # Parquet vektörize okuyucu + kolon budama
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    # Kryo serializer
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark_setup_time = time.perf_counter() - setup_start

proc_start = time.perf_counter()

# 1) Sadece gerekli kolonu oku (I/O ve deserialization azalır)
companies = spark.read.parquet(str(parquet_path)).select("company")

# 2) İki aşamalı sayım: mapPartitions ile yerel sayım -> az veri shuffle
def local_counter(iter_rows):
    acc = defaultdict(int)
    for row in iter_rows:
        # row[0] = company
        acc[row[0]] += 1
    # yalnızca küçük (company, count) çiftleri döner
    return acc.items()

# RDD yoluyla azaltılmış shuffle hacmiyle global sayım
counts = (companies.rdd
          .mapPartitions(local_counter)
          .reduceByKey(lambda a, b: a + b))

# Top-N: key/value swap edip value'a göre sıralı takeOrdered
topn = (counts
        .map(lambda kv: (kv[1], kv[0]))             # (count, company)
        .takeOrdered(TOP_N, key=lambda x: -x[0]))   # en büyükten küçüğe

spark_proc_time = time.perf_counter() - proc_start
spark_total_time = spark_setup_time + spark_proc_time

print("Spark (two-phase count, low-shuffle)")
for cnt, comp in topn:
    print(f"  {comp}: {cnt}")
print(f"  setup time: {spark_setup_time:.3f}s")
print(f"  processing time: {spark_proc_time:.3f}s")
print(f"  total time: {spark_total_time:.3f}s")

spark.stop()

# Hız karşılaştırma (processing-only)
if duck_time > 0 and spark_proc_time > 0:
    if duck_time <= spark_proc_time:
        print(f"DuckDB was {spark_proc_time / duck_time:.2f}x faster (processing only).")
    else:
        print(f"Spark was {duck_time / spark_proc_time:.2f}x faster (processing only).")
