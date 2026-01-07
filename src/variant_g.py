import csv
from pathlib import Path

import pandas as pd

from src.profiling_utils import timer

CHUNK_ROWS = 200_000


def _write_output(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_type", "count", "sum", "mean"])
        writer.writeheader()
        writer.writerows(rows)


@timer
def run(input_path: Path, output_path: Path | None = None) -> list[dict]:
    """
    Variant G: Out-of-core streaming using chunked CSV reads to handle oversized data.
    """
    totals: dict[int, dict[str, float]] = {}
    for chunk in pd.read_csv(input_path, chunksize=CHUNK_ROWS):
        grouped = chunk.groupby("event_types")["values"].agg(["count", "sum"])
        for event_type, row in grouped.iterrows():
            group = totals.setdefault(int(event_type), {"count": 0, "sum": 0.0})
            group["count"] += int(row["count"])
            group["sum"] += float(row["sum"])

    rows = [
        {
            "event_type": et,
            "count": stats["count"],
            "sum": stats["sum"],
            "mean": stats["sum"] / stats["count"] if stats["count"] else 0.0,
        }
        for et, stats in sorted(totals.items())
    ]

    if output_path:
        _write_output(rows, output_path)

    return rows

