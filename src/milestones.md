# Project Milestones: Cache-Aware Micro-ETL

This document tracks the milestones and progress of the Cache-Aware Micro-ETL project.

---

## Milestone 0: Environment & CLI Skeleton
- [x] Repository structure created (`/src`, `/docs`, `/data`).
- [x] Virtual environment and dependencies configured (`numpy`, `pandas`, `pyarrow`, `polars`, `duckdb`).
- [x] Configuration management implemented (`src/config.py`).
- [x] CLI entry point established (`src/main.py`).
- [x] Basic profiling utilities added (`src/profiling_utils.py`).

## Milestone 1: Synthetic Dataset Generator
**Current Focus**
- [x] `DataGenerator` class implemented with NumPy vectorization.
- [/] Binary file serialization using `struct` (In Progress).
- [ ] Parquet/Arrow output support.
- [ ] CLI command `microetl generate`.

## 📅 Future Milestones

### Milestone 2: Variant A - Baseline Pure Python
- Naive row-by-row iteration.
- Use Python dicts for aggregation.
- Line-by-line CSV output.

### Milestone 3: Variant B - Batched NumPy/Pandas
- Loading data in large blocks.
- Vectorized filtering and groupby operations.
- Comparison of throughput with Variant A.

### Milestone 4: Variant C - Columnar / Arrow / Polars / DuckDB
- Contiguous memory layout using Arrow/Polars.
- SQL-based aggregations using DuckDB.
- Modern high-performance storage (Parquet).

### Milestone 5: Variant D - Batch-Size Sweep
- Analyzing performance across different working set sizes (16KB to 4MB).
- Visualizing the interaction with L1/L2/L3 caches.

### Milestone 6: Profiling & Interpretation
- Using `cProfile`, `pyinstrument`, or `scalene` to find bottlenecks.
- Interpreting results in the context of memory hierarchy and locality.

### Milestone 7: Final Report & Narrative
- Summary of findings and "performance story."
- Resume-ready bullet points.
