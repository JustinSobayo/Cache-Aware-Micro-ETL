# Cache-Aware Micro-ETL: Findings & How-To

## Quick usage
- Generate data (format auto): `python -m src.main generate --rows 1_000_000 --format parquet`
- Run a variant (auto-generates input if missing): `python -m src.main run --variant d`
- Sweep batch sizes: `python -m src.main sweep --size-kb 16 --size-kb 64 --size-kb 256`
- Profile a run with cProfile: `python -m src.main run --variant b --profile`

## Variants (A–G)
- A Row-based Pure Python (CSV): pointer chasing, object overhead.
- B NumPy Batched (Parquet): vectorized, contiguous arrays.
- C Pandas Batched (Parquet): productive DataFrame ops, vectorized backend.
- D Polars Columnar (Parquet): Rust/Arrow, multi-threaded, cache-friendly.
- E DuckDB SQL (Parquet): vectorized in-process SQL engine.
- F Semi-Structured JSONL: highlights cost of nested/row-wise parsing.
- G Out-of-Core Streaming (CSV): chunked processing for data > RAM.

## What to measure
- Wall-clock runtime per variant and working-set size (see `output/sweep_results.csv`).
- Throughput (rows/s) versus working-set size; expect steep gains from B–E.
- Cache-friendliness: columnar/vectorized (D/E) should dominate A/F; G trades throughput for scalability.

## Result log (fill in after running)
- Sweep output CSV: `output/sweep_results.csv`
- Profile artifacts: `output/profile_<variant>.prof`
- Notable observations:
  - TODO: Record fastest variant per size.
  - TODO: Note when JSONL (F) becomes bottleneck.
  - TODO: Note out-of-core penalty (G) versus in-memory variants.

## Next steps
- Capture hardware specs (CPU caches, cores) alongside results.
- Plot throughput vs. working-set size using matplotlib for visuals.
- Extend profiling to pyinstrument or scalene if deeper insights needed.

