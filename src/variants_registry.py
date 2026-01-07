from pathlib import Path
from typing import Callable, Optional

from src import variant_a, variant_b, variant_c, variant_d, variant_e, variant_f, variant_g

VariantHandler = Callable[[Path, Optional[Path]], list[dict]]

VARIANT_REGISTRY: dict[str, dict] = {
    "a": {
        "name": "Row-based Pure Python",
        "handler": variant_a.run,
        "default_format": "csv",
        "allowed_formats": {"csv"},
    },
    "b": {
        "name": "NumPy Batched",
        "handler": variant_b.run,
        "default_format": "parquet",
        "allowed_formats": {"parquet"},
    },
    "c": {
        "name": "Pandas Batched",
        "handler": variant_c.run,
        "default_format": "parquet",
        "allowed_formats": {"parquet"},
    },
    "d": {
        "name": "Polars Columnar",
        "handler": variant_d.run,
        "default_format": "parquet",
        "allowed_formats": {"parquet"},
    },
    "e": {
        "name": "DuckDB SQL",
        "handler": variant_e.run,
        "default_format": "parquet",
        "allowed_formats": {"parquet"},
    },
    "f": {
        "name": "Semi-Structured JSONL",
        "handler": variant_f.run,
        "default_format": "jsonl",
        "allowed_formats": {"jsonl"},
    },
    "g": {
        "name": "Out-of-Core Streaming",
        "handler": variant_g.run,
        "default_format": "csv",
        "allowed_formats": {"csv"},
    },
}

EXTENSIONS = {
    "binary": "bin",
    "parquet": "parquet",
    "arrow": "arrow",
    "csv": "csv",
    "jsonl": "jsonl",
}
 
