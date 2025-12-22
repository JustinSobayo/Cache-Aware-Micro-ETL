import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import struct
import json
from pathlib import Path
from src.config import settings
from src.profiling_utils import timer

class DataGenerator:
    """
    Synthetic dataset generator for cache-aware benchmarking.
    """
    def __init__(self, seed: int = settings.SEED):
        self.rng = np.random.default_rng(seed)
    def generate_batch(self, n_rows: int) -> dict[str, np.array]:
        """Generate a batch of synthetic data."""
        event_id = np.arange(n_rows, dtype=np.uint64)
        timestamp = np.sort(self.rng.integers(low=0, high=10**9, size = n_rows, dtype = np.uint64))
        user_ids = self.rng.integers(low = 100, high = 9999, size = n_rows, dtype = np.uint16)
        event_types = self.rng.integers(low = 0, high = 4, size = n_rows, dtype = np.uint8)
        values = self.rng.standard_normal(size = n_rows).astype(np.float64)
        metadata_samples: list[str] = [f'{{"info": "test_{i}"}}' for i in range(1000)]
        metadata: list[str] = (metadata_samples * (n_rows // 1000 + 1))[:n_rows]
        metadata = np.array(metadata, dtype = object)
        return {
            "event_id": event_id,
            "timestamp": timestamp,
            "user_ids": user_ids,
            "event_types": event_types,
            "values": values,
            "metadata": metadata,
        }



    def generate_sample(self):
        # Placeholder for the logic
        pass

if __name__ == "__main__":
    # This allows us to test the generator directly
    gen = DataGenerator()
    print("Generator initialized and ready.")
