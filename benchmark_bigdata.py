import os, gzip, time, tracemalloc, psutil, shutil, math
from pathlib import Path

# ==== CONFIG ====
# Point these to your files.
RAW_CSV = Path(r"C:\Users\DALMAC\Desktop\TP\Big Data\2019-Oct.csv")
GZIP_CSV = RAW_CSV.with_suffix(RAW_CSV.suffix + ".gz")
PARQUET_DIR = RAW_CSV.with_suffix("").with_name(RAW_CSV.stem + "_parquet")

# Column(s) to aggregate for a sanity check (edit to match your data)
# If unknown, set NUM_COLS = [] and we’ll just count rows.
NUM_COLS = ["some_numeric_col"]   # example: ["fare_amount"] for taxi data

CHUNKSIZE = 1_000_000  # tune to your RAM (start 100k–1M)

# ==== UTILS ====
def fmt_bytes(n):
    if n is None: return "n/a"
    units = ['B','KB','MB','GB','TB']
    if n == 0: return "0 B"
    i = int(math.floor(math.log(n, 1024)))
    return f"{n / (1024**i):.2f} {units[i]}"

def measure(func, *args, **kwargs):
    proc = psutil.Process(os.getpid())
    rss_before = proc.memory_info().rss
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = proc.memory_info().rss
    return {
        "elapsed_s": elapsed,
        "peak_mem_bytes": peak,
        "rss_delta_bytes": max(0, rss_after - rss_before),
        "result": result,
    }

def file_size(p: Path):
    return p.stat().st_size if p.exists() else None

# ==== METHOD A: Pandas with chunks ====
def pandas_chunked_count_and_sum(csv_path, chunksize, num_cols):
    import pandas as pd
    total_rows = 0
    sums = {c: 0.0 for c in num_cols}
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=True):
        total_rows += len(chunk)
        for c in num_cols:
            if c in chunk.columns:
                sums[c] += chunk[c].dropna().astype(float).sum()
    return {"rows": total_rows, "sums": sums}

# ==== METHOD B: Dask DataFrame ====
def dask_count_and_sum(csv_path, num_cols):
    import dask.dataframe as dd
    # blocksize=None lets Dask auto-partition; tweak if needed
    df = dd.read_csv(str(csv_path), blocksize=None, dtype="object")  # safe default dtypes
    rows = int(df.shape[0].compute())
    sums = {}
    for c in num_cols:
        if c in df.columns:
            # convert to float safely
            sums[c] = float(dd.to_numeric(df[c], errors="coerce").sum().compute())
    return {"rows": rows, "sums": sums}

# ==== METHOD C: Compressed CSV (gzip) + chunks ====
def ensure_gzip(src: Path, dst: Path):
    if dst.exists() and file_size(dst) and file_size(dst) > 0:
        return
    with open(src, "rb") as f_in, gzip.open(dst, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)

def pandas_gzip_chunked_count_and_sum(gzip_csv, chunksize, num_cols):
    import pandas as pd
    total_rows = 0
    sums = {c: 0.0 for c in num_cols}
    for chunk in pd.read_csv(gzip_csv, compression="gzip", chunksize=chunksize, low_memory=True):
        total_rows += len(chunk)
        for c in num_cols:
            if c in chunk.columns:
                sums[c] += chunk[c].dropna().astype(float).sum()
    return {"rows": total_rows, "sums": sums}

# ==== Optional: write Parquet to compare storage & future speed ====
def write_parquet_from_csv(csv_path, out_dir, sample_rows=5_000_000):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Infer a rough schema on a sample
    it = pd.read_csv(csv_path, chunksize=sample_rows, low_memory=True)
    sample = next(it)
    table = pa.Table.from_pandas(sample, preserve_index=False)
    pq.write_table(table, out_dir / "part0.parquet", compression="snappy")

    # Stream rest in chunks (append as row groups)
    for i, chunk in enumerate(it, start=1):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        pq.write_table(table, out_dir / f"part{i}.parquet", compression="snappy")

    # Return total size
    total = sum(p.stat().st_size for p in out_dir.glob("*.parquet"))
    return total

# ==== RUN ====
if __name__ == "__main__":
    print("\n=== STORAGE (before) ===")
    print("Raw CSV:", RAW_CSV, fmt_bytes(file_size(RAW_CSV)))

    # A) Pandas chunked
    mA = measure(pandas_chunked_count_and_sum, RAW_CSV, CHUNKSIZE, NUM_COLS)
    print("\n[Method A] Pandas chunks")
    print("Elapsed:", f"{mA['elapsed_s']:.2f}s", " Peak mem:", fmt_bytes(mA["peak_mem_bytes"]))
    print("Rows:", mA["result"]["rows"], " Sums:", mA["result"]["sums"])

    # Prepare gzip once
    print("\nCreating/Checking gzip…")
    ensure_gzip(RAW_CSV, GZIP_CSV)
    print("Gzip CSV:", GZIP_CSV, fmt_bytes(file_size(GZIP_CSV)))

    # C) Pandas gzip + chunks
    mC = measure(pandas_gzip_chunked_count_and_sum, GZIP_CSV, CHUNKSIZE, NUM_COLS)
    print("\n[Method C] Pandas gzip + chunks")
    print("Elapsed:", f"{mC['elapsed_s']:.2f}s", " Peak mem:", fmt_bytes(mC["peak_mem_bytes"]))
    print("Rows:", mC["result"]["rows"], " Sums:", mC["result"]["sums"])

    # B) Dask
    try:
        mB = measure(dask_count_and_sum, RAW_CSV, NUM_COLS)
        print("\n[Method B] Dask DataFrame")
        print("Elapsed:", f"{mB['elapsed_s']:.2f}s", " Peak mem:", fmt_bytes(mB["peak_mem_bytes"]))
        print("Rows:", mB["result"]["rows"], " Sums:", mB["result"]["sums"])
    except Exception as e:
        print("\n[Method B] Dask failed:", e)

    # Optional: Parquet conversion for storage & future speed
    try:
        print("\nWriting Parquet (snappy) for storage comparison…")
        parquet_bytes = measure(write_parquet_from_csv, RAW_CSV, PARQUET_DIR)[ "result" ]
        print("Parquet dir:", PARQUET_DIR, fmt_bytes(parquet_bytes))
    except Exception as e:
        print("Parquet step skipped/failed:", e)

    print("\n=== STORAGE (after) ===")
    print("Raw CSV size:  ", fmt_bytes(file_size(RAW_CSV)))
    print("Gzip CSV size: ", fmt_bytes(file_size(GZIP_CSV)))
    psize = sum(p.stat().st_size for p in Path(PARQUET_DIR).glob("*.parquet")) if Path(PARQUET_DIR).exists() else None
    print("Parquet size:  ", fmt_bytes(psize))
