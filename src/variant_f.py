import csv
import json
from pathlib import Path

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
    Variant F: Semi-structured JSONL parsing (row-wise), demonstrates overhead.
    """
    totals: dict[int, dict[str, float]] = {}
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            event_type = int(record["event_types"])
            val = float(record["values"])
            group = totals.setdefault(event_type, {"count": 0, "sum": 0.0})
            group["count"] += 1
            group["sum"] += val

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

