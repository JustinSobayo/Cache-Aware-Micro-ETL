import csv
from pathlib import Path
from typing import Iterable, List

from rich.console import Console

from src.config import settings
from src.data_gen import DataGenerator
from src.profiling_utils import measure_seconds
from src.variants_registry import EXTENSIONS, VARIANT_REGISTRY

console = Console()

DEFAULT_SIZES_KB = [16, 64, 256, 1024, 4096]
EST_BYTES_PER_ROW = 48  # rough estimate for sizing rows to working set


def _rows_for_kb(size_kb: int) -> int:
    return max(1, int((size_kb * 1024) / EST_BYTES_PER_ROW))


def sweep(
    variants: Iterable[str] = ("a", "b", "c", "d", "e", "f", "g"),
    sizes_kb: Iterable[int] = DEFAULT_SIZES_KB,
    seed: int = settings.SEED,
) -> List[dict]:
    """
    Run a batch-size sweep across variants and working set sizes.
    """
    results: List[dict] = []
    generator = DataGenerator(seed=seed)

    for variant_key in variants:
        if variant_key not in VARIANT_REGISTRY:
            console.print(f"[red]Skipping unknown variant {variant_key}[/red]")
            continue
        info = VARIANT_REGISTRY[variant_key]
        fmt = info["default_format"]
        handler = info["handler"]
        for size_kb in sizes_kb:
            rows = _rows_for_kb(size_kb)
            dataset_path = settings.DATA_DIR / f"sweep_{variant_key}_{size_kb}kb.{EXTENSIONS[fmt]}"

            if not dataset_path.exists():
                console.print(
                    f"[yellow]Generating[/yellow] {rows:,} rows ({size_kb}KB target) "
                    f"for variant {variant_key.upper()} -> {dataset_path}"
                )
                generator.generate_and_save(rows, fmt=fmt, output_path=dataset_path)

            console.print(
                f"[bold green]Running variant {variant_key.upper()}[/bold green] "
                f"size={size_kb}KB rows={rows:,}"
            )
            _, duration = measure_seconds(handler, dataset_path, None)
            results.append(
                {
                    "variant": variant_key,
                    "variant_name": info["name"],
                    "size_kb": size_kb,
                    "rows": rows,
                    "seconds": duration,
                    "throughput_rows_per_s": rows / duration if duration > 0 else 0,
                }
            )
    return results


def write_results_csv(results: List[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not results:
        return
    fieldnames = list(results[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

