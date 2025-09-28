# CSV Dual Engine Benchmark

This mini project helps you run the same aggregation workload on DuckDB and Apache Spark so that you can compare their execution times on a large CSV file.

## Layout

- `benchmark.py`: CLI script that executes the workload with DuckDB and Spark and prints timing/summary tables.
- `requirements.txt`: Python dependencies (DuckDB + PySpark).
- `README.md`: This guide.
- `data/`: Place your CSV input file here (optional; you can also point to any path).

## Quick start

1. Create and activate a Python virtual environment.
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
2. Install dependencies.
   ```powershell
   pip install -r requirements.txt
   ```
3. Make sure Spark can start. The default configuration uses the local master. If you already have `SPARK_HOME` set, PySpark picks it up automatically.
4. Run the benchmark (replace placeholders with your file/column names).
   ```powershell
   python benchmark.py --csv-path data\your_file.csv --numeric-column amount
   ```

## What the script does

- Loads the CSV with DuckDB and calculates row count plus sum/avg/min/max for the numeric column.
- Builds a local Spark session, performs the same aggregations, and ensures the numeric column is cast to double.
- Measures wall-clock execution time for each engine using `time.perf_counter`.
- Prints a side-by-side comparison and a simple speedup ratio so you can see which engine won.

## Extending the demo

- Add `--group-by-column` if you want to compare group-by aggregations (keep the number of groups manageable for printing).
- Replace the aggregations with more complex SQL/DSL tasks that reflect your workload.
- Redirect the printed JSON to a file and chart the results over multiple runs to track tuning.

## Troubleshooting tips

- For Spark, ensure `JAVA_HOME` points to a compatible JRE/JDK and that you installed `pyspark`.
- If the CSV header has spaces or special characters, wrap the column name in quotes inside the CLI (the script will escape it for DuckDB and Spark).
- When comparing results, consider running multiple repetitions and averaging the timings to reduce noise.
