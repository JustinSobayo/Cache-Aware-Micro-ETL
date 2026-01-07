import csv
from pathlib import Path

import pandas as pd

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
    Variant C: Pandas batched DataFrame operations from Parquet input.
    """
    df = pd.read_parquet(input_path)
    agg = (
        df.groupby("event_types")["values"]
        .agg(["count", "sum", "mean"])
        .reset_index()
        .rename(columns={"event_types": "event_type"})
        .sort_values("event_type")
    )
    rows = agg.to_dict(orient="records")

    if output_path:
        _write_output(rows, output_path)

    return rows

