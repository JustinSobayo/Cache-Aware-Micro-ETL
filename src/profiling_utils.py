import time
import functools
from typing import Callable, Any
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
        console.print(f"[bold blue]PROFILER:[/bold blue] {func.__name__} executed in {duration:.4f} seconds")
        return result
    return wrapper