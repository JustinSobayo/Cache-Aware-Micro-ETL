import cProfile
import functools
import time
from pathlib import Path
from typing import Any, Callable, Tuple

from rich.console import Console

console = Console()


def timer(func: Callable) -> Callable:
    """Decorator that reports the execution time."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        duration = end_time - start_time
        console.print(
            f"[bold blue]PROFILER:[/bold blue] {func.__name__} executed in {duration:.4f} seconds"
        )
        return result

    return wrapper


def measure_seconds(func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """Measure wall-clock seconds for a callable, returning (result, seconds)."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    duration = time.perf_counter() - start_time
    return result, duration


def run_with_cprofile(func: Callable, output_path: Path, *args, **kwargs) -> Any:
    """
    Execute `func` under cProfile and write stats to `output_path`.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    profiler = cProfile.Profile()
    result = profiler.runcall(func, *args, **kwargs)
    profiler.dump_stats(output_path)
    console.print(f"[bold blue]cProfile[/bold blue] stats written to {output_path}")
    return result