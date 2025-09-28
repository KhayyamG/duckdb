# DuckDB Analytics Benchmarks

This folder collects small, focused benchmarks that compare how DuckDB, Apache Spark, and Pandas process wide analytical datasets stored as CSV or Parquet. Each script answers the same question: "Which companies occur most often?" but the implementations highlight different execution models, tuning options, and ergonomics for interactive analytics versus distributed processing.

## Benchmark Scenarios

- `benchmark_duckdb_spark_csv.py` - Reads a large CSV file and compares DuckDB SQL to a local Spark session.
- `benchmark_duckdb_spark_parquet.py` - Runs the same aggregation on Parquet, including setup and processing timing breakdowns.
- `benchmark_faster_parquet.py` - Demonstrates a tuned Spark pipeline (reduced partitions, manual map and reduce) against the DuckDB baseline.
- `bencmark_duckdb_pandas_csv.py` - Evaluates DuckDB versus a Pandas workflow that loads only the required column.
- `benchmark duckdb_pandas_parquet.py` - Streams Parquet fragments with PyArrow to emulate out-of-core Pandas processing.
- `data/csv_to_parquet.py` - Helper to materialize a multi-gigabyte Parquet file from the CSV source for stress testing.

## Data Requirements

Sample data files are located in `data/`:

- `customers-2000000.csv` - Source file (about 333 MB) containing customer and company records.
- `customers.parquet` - Pre-generated Parquet version (about 5.9 GB) used by the Parquet benchmarks.

If you need a different Parquet size, edit `data/csv_to_parquet.py` and run it to duplicate the CSV until the desired target size is reached. All scripts accept a path argument so you can point to your own datasets.

## Environment Setup

1. Create and activate a virtual environment (PowerShell syntax shown):
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
2. Install the required libraries:
   ```powershell
   pip install -r requirements.txt
   pip install pandas pyarrow  # needed for the Pandas and Arrow scenarios
   ```
3. Ensure Java is available so PySpark can start (`java -version`).

> The Spark benchmarks run with `master="local[*]"`, so no external cluster is required. The scripts rely solely on files accessible from the local filesystem.

## Running the Benchmarks

Each benchmark can be executed directly with Python. Replace paths and arguments as needed.

### DuckDB vs Spark on CSV
```powershell
python benchmark_duckdb_spark_csv.py data\customers-2000000.csv
```
- Prints DuckDB processing time, then Spark setup and processing times, followed by the speedup ratio.
- Omitting the argument uses the default CSV in `data/`.

### DuckDB vs Spark on Parquet
```powershell
python benchmark_duckdb_spark_parquet.py
```
- Requires `data/customers.parquet` (or pass a custom path).
- Reports separate setup and processing metrics for both engines.

### Tuned Spark vs DuckDB (Parquet)
```powershell
python benchmark_faster_parquet.py
```
- Spark run uses manual `mapPartitions` counting to reduce shuffle overhead.
- Output shows how configuration and algorithmic choices change Spark's runtime relative to the DuckDB baseline.

### DuckDB vs Pandas on CSV
```powershell
python bencmark_duckdb_pandas_csv.py data\customers-2000000.csv
```
- Pandas only reads the `Company` column to minimize memory footprint.
- Useful for comparing DuckDB SQL to a single-node Pandas workflow.

### DuckDB vs Pandas on Parquet (Chunked)
```powershell
python "benchmark duckdb_pandas_parquet.py" data\customers.parquet 10
```
- Automatically detects the company column name (case-insensitive).
- Pandas path uses a PyArrow dataset scanner to process batches (128000 rows by default), emulating out-of-core execution.
- Second argument sets `TOP_N` (default 5).

## Interpreting Results

- **Setup time** measures session construction (connections, Spark session boot) and highlights engines with heavier initialization costs.
- **Processing time** isolates the aggregation workload. This is the best metric for comparing query efficiency.
- **Speedup ratio** expresses how much faster one engine is than the other for the processing phase.

For consistent comparisons, run each script multiple times and average the results, especially when Spark's JVM warm-up or operating system level disk caching come into play.

## Extending the Experiments

- Swap the aggregation: modify the SQL or DataFrame logic to include additional metrics, filters, or group-by columns.
- Experiment with partitioned datasets: point the scripts to a directory of partitioned Parquet files to observe catalog pruning behavior.
- Vary batch size and vectorization settings in `benchmark duckdb_pandas_parquet.py` to investigate Pandas scalability limits.
- Integrate the scripts into automated benchmarks by capturing stdout and storing results as JSON or CSV for later plotting.

## Troubleshooting

- **Spark fails to start** - Verify that `JAVA_HOME` is set and that your Java version is compatible with Spark 3.5.
- **Out-of-memory errors** - Reduce `TOP_N`, supply a smaller dataset, or tweak `BATCH_SIZE` in the Pandas and Arrow benchmark.
- **Slow local disk throughput** - Store the dataset on an SSD and close other resource-intensive applications before running.

## Project Tree

```
duckdb/
|-- benchmark duckdb_pandas_parquet.py
|-- benchmark_duckdb_spark_csv.py
|-- benchmark_duckdb_spark_parquet.py
|-- benchmark_faster_parquet.py
|-- bencmark_duckdb_pandas_csv.py
|-- data/
|   |-- customers-2000000.csv
|   |-- customers.parquet
|   \-- csv_to_parquet.py
|-- requirements.txt
\-- README.md  <- you are here
```

Happy benchmarking!
