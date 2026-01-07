import csv
from pathlib import Path
from typing import Iterable

from src.profiling_utils import timer


def _write_output(rows: Iterable[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_type", "count", "sum", "mean"])
        writer.writeheader()
        writer.writerows(rows)


@timer
def run(input_path: Path, output_path: Path | None = None) -> list[dict]:
    """
    Variant A: pure Python row-based processing using CSV input.
    Aggregates count/sum/mean of `values` grouped by `event_types`.
    """
    totals: dict[int, dict[str, float]] = {}

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_type = int(row["event_types"])
            val = float(row["values"])
            group = totals.setdefault(event_type, {"count": 0, "sum": 0.0})
            group["count"] += 1
            group["sum"] += val

    results = [
        {
            "event_type": et,
            "count": stats["count"],
            "sum": stats["sum"],
            "mean": stats["sum"] / stats["count"] if stats["count"] else 0.0,
        }
        for et, stats in sorted(totals.items())
    ]

    if output_path:
        _write_output(results, output_path)

    return results

