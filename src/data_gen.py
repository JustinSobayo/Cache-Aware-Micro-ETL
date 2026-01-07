import json
import struct
from pathlib import Path
from typing import Iterable, Literal

import numpy as np
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from src.config import settings

SupportedFormat = Literal["binary", "parquet", "arrow", "csv", "jsonl"]


class DataGenerator:
    """
    Synthetic dataset generator for cache-aware benchmarking.
    """

    def __init__(self, seed: int = settings.SEED):
        self.rng = np.random.default_rng(seed)

    def generate_batch(self, n_rows: int) -> dict[str, np.ndarray]:
        """Generate a batch of synthetic data."""
        event_id = np.arange(n_rows, dtype=np.uint64)
        timestamp = np.sort(
            self.rng.integers(low=0, high=10**9, size=n_rows, dtype=np.uint64)
        )
        user_ids = self.rng.integers(low=100, high=9999, size=n_rows, dtype=np.uint16)
        event_types = self.rng.integers(low=0, high=4, size=n_rows, dtype=np.uint8)
        values = self.rng.standard_normal(size=n_rows).astype(np.float64)
        metadata_samples: list[str] = [f'{{"info": "test_{i}"}}' for i in range(1000)]
        metadata_list: list[str] = (metadata_samples * (n_rows // 1000 + 1))[:n_rows]
        metadata = np.array(metadata_list, dtype=object)
        return {
            "event_id": event_id,
            "timestamp": timestamp,
            "user_ids": user_ids,
            "event_types": event_types,
            "values": values,
            "metadata": metadata,
        }

    def _as_table(self, data: dict[str, np.ndarray]) -> pa.Table:
        """Convert the numpy dict to a PyArrow Table."""
        return pa.table(data)

    def save_as_binary(self, data: dict[str, np.ndarray], path: Path) -> Path:
        """
        Save data to a compact binary format.

        Layout per row (little endian):
        - event_id:   uint64
        - timestamp:  uint64
        - user_id:    uint16
        - event_type: uint8
        - value:      float64
        - metadata:   uint16 length prefix + UTF-8 bytes
        """

        n_rows = len(next(iter(data.values())))
        with path.open("wb") as f:
            f.write(b"CETL1")
            f.write(struct.pack("<Q", n_rows))
            for i in range(n_rows):
                meta_bytes = str(data["metadata"][i]).encode("utf-8")
                if len(meta_bytes) > 65535:
                    raise ValueError("Metadata string too long for uint16 length prefix")
                row = struct.pack(
                    "<QQHBdH",
                    int(data["event_id"][i]),
                    int(data["timestamp"][i]),
                    int(data["user_ids"][i]),
                    int(data["event_types"][i]),
                    float(data["values"][i]),
                    len(meta_bytes),
                )
                f.write(row)
                f.write(meta_bytes)
        return path

    def save_as_parquet(self, data: dict[str, np.ndarray], path: Path) -> Path:
        table = self._as_table(data)
        pq.write_table(table, path)
        return path

    def save_as_arrow(self, data: dict[str, np.ndarray], path: Path) -> Path:
        table = self._as_table(data)
        with pa.OSFile(path, "wb") as sink:
            with pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
        return path

    def save_as_csv(self, data: dict[str, np.ndarray], path: Path) -> Path:
        table = self._as_table(data)
        pa_csv.write_csv(table, path)
        return path

    def save_as_jsonl(self, data: dict[str, np.ndarray], path: Path) -> Path:
        n_rows = len(next(iter(data.values())))
        with path.open("w", encoding="utf-8") as f:
            for i in range(n_rows):
                record = {k: self._json_safe(v[i]) for k, v in data.items()}
                f.write(json.dumps(record) + "\n")
        return path

    def generate_and_save(
        self,
        n_rows: int,
        fmt: SupportedFormat,
        output_path: Path | None = None,
    ) -> Path:
        """Generate a dataset and persist it in the requested format."""
        data = self.generate_batch(n_rows)
        fmt = fmt.lower()
        target = output_path or self._default_path(fmt)
        target.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "binary":
            return self.save_as_binary(data, target)
        if fmt == "parquet":
            return self.save_as_parquet(data, target)
        if fmt == "arrow":
            return self.save_as_arrow(data, target)
        if fmt == "csv":
            return self.save_as_csv(data, target)
        if fmt == "jsonl":
            return self.save_as_jsonl(data, target)
        raise ValueError(f"Unsupported format: {fmt}")

    def _default_path(self, fmt: str) -> Path:
        ext_map = {
            "binary": "bin",
            "parquet": "parquet",
            "arrow": "arrow",
            "csv": "csv",
            "jsonl": "jsonl",
        }
        ext = ext_map.get(fmt, fmt)
        return settings.DATA_DIR / f"synthetic.{ext}"

    @staticmethod
    def _json_safe(value) -> object:
        """Convert numpy scalars to native Python types for JSON serialization."""
        if isinstance(value, np.generic):
            return value.item()
        return value


if __name__ == "__main__":
    gen = DataGenerator()
    path = gen.generate_and_save(1000, fmt="parquet")
    print(f"Generated sample data at {path}")
