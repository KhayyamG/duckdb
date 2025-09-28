#!/usr/bin/env python
import sys
import time
import os
from pathlib import Path
import duckdb
import pandas as pd

DEFAULT_PARQUET = Path(__file__).resolve().parent / "data" / "customers.parquet"
TOP_N = 5
BATCH_SIZE = 128_000

def resolve_target(p: Path) -> str:
    if p.is_dir():
        return str(p / "**" / "*.parquet")
    return str(p)

def autodetect_company_col(parquet_target: str) -> str:
    cols = [r[0] for r in duckdb.execute(
        "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [parquet_target]
    ).fetchall()]
    for c in ("Company", "company", "COMPANY"):
        if c in cols:
            return c
    m = {c.lower(): c for c in cols}
    if "company" in m:
        return m["company"]
    raise SystemExit(f"Company column not found. Available columns: {cols}")

def duckdb_topn(parquet_target: str, col: str, top_n: int):
    duckdb.execute(f"PRAGMA threads={os.cpu_count() or 4}")
    q = f"""
        SELECT {col} AS company, COUNT({col}) AS rows
        FROM read_parquet(?)
        WHERE {col} IS NOT NULL
        GROUP BY {col}
        ORDER BY rows DESC
        LIMIT ?
    """
    t0 = time.perf_counter()
    rows = duckdb.execute(q, [parquet_target, top_n]).fetchall()
    return rows, time.perf_counter() - t0

def _make_scanner(dataset, *, columns, batch_size):
    import pyarrow.dataset as ds
    if hasattr(dataset, "scanner"):
        return dataset.scanner(columns=columns, batch_size=batch_size)
    if hasattr(ds, "Scanner") and hasattr(ds.Scanner, "from_dataset"):
        return ds.Scanner.from_dataset(dataset, columns=columns, batch_size=batch_size)
    raise SystemExit("Your PyArrow version does not support this API. Run `pip install -U pyarrow`.")

def pandas_topn_chunked_allcols(parquet_path_or_glob: str, col: str, top_n: int, batch_size: int = BATCH_SIZE):
    import pyarrow.dataset as ds
    dataset = ds.dataset(parquet_path_or_glob, format="parquet", partitioning="hive")
    fragments = list(dataset.get_fragments())
    if not fragments:
        raise SystemExit(f"No Parquet found: {parquet_path_or_glob}")

    t0 = time.perf_counter()
    vc = None

    scanner = _make_scanner(dataset, columns=None, batch_size=batch_size)
    for batch in scanner.to_batches():
        idx = batch.schema.get_field_index(col)
        if idx == -1:
            raise SystemExit(f"Column '{col}' not found in batch.")
        s = batch.column(idx).to_pandas()
        part = s.value_counts(dropna=True)
        vc = part if vc is None else vc.add(part, fill_value=0)

    rows = []
    if vc is not None:
        vc = vc.sort_values(ascending=False)
        rows = list(zip(vc.index[:top_n].tolist(),
                        vc.values[:top_n].astype(int).tolist()))
    return rows, time.perf_counter() - t0

def pandas_topn_direct(parquet_path: str, col: str, top_n: int):
    t0 = time.perf_counter()
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    vc = df[col].value_counts(dropna=True).head(top_n)
    rows = list(zip(vc.index.tolist(), vc.values.tolist()))
    return rows, time.perf_counter() - t0

def main():
    argv = sys.argv[1:]
    parquet_path = Path(argv[0]).expanduser().resolve() if argv else DEFAULT_PARQUET
    top_n = int(argv[1]) if len(argv) >= 2 else TOP_N

    if not parquet_path.exists():
        raise SystemExit(f"Parquet file/directory not found: {parquet_path}")

    parquet_target = resolve_target(parquet_path)
    print(f"Using Parquet: {parquet_target}")
    print(f"Top {top_n} companies by row count\n")

    company_col = autodetect_company_col(parquet_target)

    duck_rows, duck_time = duckdb_topn(parquet_target, company_col, top_n)
    print("DuckDB")
    for c, r in duck_rows:
        print(f"  {c}: {r}")
    print(f"  processing time: {duck_time:.3f}s\n")

    print("Pandas")
    try:
        raise MemoryError
    except Exception:
        pandas_rows, p_time = pandas_topn_chunked_allcols(parquet_target, company_col, top_n)
        mode = "chunked-allcols (pyarrow)"

    for c, r in pandas_rows:
        print(f"  {c}: {r}")
    print(f"  processing time: {p_time:.3f}s  [{mode}]\n")

    if p_time > 0 and duck_time <= p_time:
        print(f"DuckDB was {p_time / duck_time:.2f}x faster.")
    elif p_time > 0:
        print(f"Pandas was {duck_time / p_time:.2f}x faster.")
    else:
        print("Comparison unavailable (Pandas time is zero).")

if __name__ == "__main__":
    main()
