import csv
from pathlib import Path

import duckdb

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
    Variant E: DuckDB SQL aggregation on Parquet input.
    """
    query = f"""
        SELECT
            event_types AS event_type,
            COUNT(*)   AS count,
            SUM(values) AS sum,
            AVG(values) AS mean
        FROM parquet_scan('{input_path.as_posix()}')
        GROUP BY 1
        ORDER BY 1
    """
    rows = duckdb.query(query).to_df().to_dict(orient="records")

    if output_path:
        _write_output(rows, output_path)

    return rows

