import csv
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

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
    Variant B: NumPy batched aggregation from Parquet input.
    """
    table = pq.read_table(input_path)
    event_types = table["event_types"].to_numpy()
    values = table["values"].to_numpy()

    unique_types = np.unique(event_types)
    results: list[dict] = []
    for et in unique_types:
        mask = event_types == et
        count = int(mask.sum())
        if count == 0:
            continue
        sum_val = float(values[mask].sum())
        results.append(
            {
                "event_type": int(et),
                "count": count,
                "sum": sum_val,
                "mean": sum_val / count,
            }
        )

    results.sort(key=lambda r: r["event_type"])

    if output_path:
        _write_output(results, output_path)

    return results

