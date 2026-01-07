from pathlib import Path
import csv

import polars as pl

from src.profiling_utils import timer


def _write_output(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_type", "count", "sum", "mean"])
        writer.writeheader()
        writer.writerows(rows)


@timer
def run(input_path: Path, output_path: Path | None = None) -> list[dict]:
    """
    Variant D: Polars columnar, multi-threaded aggregation on Parquet input.
    """
    df = (
        pl.scan_parquet(input_path)
        .group_by("event_types")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("values").sum().alias("sum"),
                pl.col("values").mean().alias("mean"),
            ]
        )
        .rename({"event_types": "event_type"})
        .sort("event_type")
        .collect()
    )
    rows = df.to_dicts()

    if output_path:
        _write_output(rows, output_path)

    return rows

