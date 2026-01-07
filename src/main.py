from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console

from src.config import settings
from src import bench
from src.data_gen import DataGenerator
from src.profiling_utils import run_with_cprofile
from src.variants_registry import EXTENSIONS, VARIANT_REGISTRY, VariantHandler

# Initialize the Typer app and Rich console
app = typer.Typer(
    name="micro-etl",
    help="Cache-Aware Micro-ETL Benchmark: Hardware-to-Algorithm Mapping",
    add_completion=False,
)
console = Console()


@app.command()
def info():
    """
    Display project configuration and environment info.
    """
    console.print("\n[bold cyan]Cache-Aware Micro-ETL Config[/bold cyan]")
    console.print("-" * 40)
    console.print(f"[bold white]Base Directory:[/bold white]   {settings.BASE_DIR}")
    console.print(f"[bold white]Data Directory:[/bold white]   {settings.DATA_DIR}")
    console.print(f"[bold white]Output Directory:[/bold white] {settings.OUTPUT_DIR}")
    console.print(f"[bold white]Default Rows:[/bold white]     {settings.DEFAULT_ROWS:,}")
    console.print("-" * 40 + "\n")


@app.command()
def generate(
    rows: int = typer.Option(
        settings.DEFAULT_ROWS,
        "--rows",
        "-r",
        help="Number of rows to generate.",
    ),
    fmt: str = typer.Option(
        "parquet",
        "--format",
        "-f",
        help="Output format: binary, parquet, arrow, csv, jsonl.",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional output path; defaults to data/synthetic.<ext>.",
    ),
    seed: int = typer.Option(
        settings.SEED,
        "--seed",
        help="RNG seed for reproducible datasets.",
    ),
):
    """
    Generate synthetic datasets for benchmarking.
    """
    fmt = fmt.lower()
    generator = DataGenerator(seed=seed)
    console.print(
        f"[bold green]Generating[/bold green] {rows:,} rows as [cyan]{fmt}[/cyan]..."
    )
    try:
        path = generator.generate_and_save(rows, fmt=fmt, output_path=output)
    except ValueError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1)

    console.print(f"[bold green]Success:[/bold green] wrote {rows:,} rows to {path}")


@app.command()
def run(
    variant: str = typer.Option(
        ...,
        "--variant",
        "-v",
        help="Variant to run: a, b, c, d, e, f, g.",
    ),
    input_path: Optional[Path] = typer.Option(
        None,
        "--input",
        "-i",
        help="Optional path to input dataset; defaults per variant.",
    ),
    rows: int = typer.Option(
        settings.DEFAULT_ROWS,
        "--rows",
        help="Row count for auto-generation if input is missing.",
    ),
    fmt: Optional[str] = typer.Option(
        None,
        "--format",
        "-f",
        help="Override format for auto-generation (must match variant).",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional path to write aggregated results as CSV.",
    ),
    seed: int = typer.Option(
        settings.SEED,
        "--seed",
        help="RNG seed when auto-generating datasets.",
    ),
    auto_generate: bool = typer.Option(
        True,
        "--auto-generate/--no-auto-generate",
        help="Auto-generate data if input is missing.",
    ),
    profile: bool = typer.Option(
        False,
        "--profile",
        help="Run under cProfile and write stats to OUTPUT/profile_*.prof.",
    ),
    profile_output: Optional[Path] = typer.Option(
        None,
        "--profile-output",
        help="Optional path for cProfile stats file.",
    ),
):
    """
    Run a specific ETL pipeline variant (A–G).
    """
    variant_key = variant.lower()
    if variant_key not in VARIANT_REGISTRY:
        console.print(f"[red]Unknown variant: {variant}[/red]")
        raise typer.Exit(code=1)

    variant_info = VARIANT_REGISTRY[variant_key]
    selected_format = (fmt.lower() if fmt else variant_info["default_format"])
    if selected_format not in variant_info["allowed_formats"]:
        console.print(
            f"[red]Format '{selected_format}' not supported for variant {variant_key.upper()}[/red]"
        )
        raise typer.Exit(code=1)

    default_input = settings.DATA_DIR / f"synthetic.{EXTENSIONS[selected_format]}"
    dataset_path = input_path or default_input

    if not dataset_path.exists():
        if not auto_generate:
            console.print(
                f"[red]Input file missing:[/red] {dataset_path}. "
                "Enable --auto-generate or supply --input."
            )
            raise typer.Exit(code=1)
        console.print(
            f"[yellow]Auto-generating[/yellow] {rows:,} rows as {selected_format} at {dataset_path}"
        )
        generator = DataGenerator(seed=seed)
        generator.generate_and_save(rows, fmt=selected_format, output_path=dataset_path)

    console.print(
        f"[bold green]Running variant {variant_key.upper()}[/bold green] "
        f"({variant_info['name']}) on {dataset_path}"
    )
    handler: VariantHandler = variant_info["handler"]
    if profile:
        stats_path = profile_output or settings.OUTPUT_DIR / f"profile_{variant_key}.prof"
        results = run_with_cprofile(handler, stats_path, dataset_path, output)
    else:
        results = handler(dataset_path, output)

    console.print("[bold green]Aggregation complete[/bold green]")
    for row in results:
        console.print(
            f"event_type={row['event_type']} count={row['count']} "
            f"sum={row['sum']:.4f} mean={row['mean']:.4f}"
        )


@app.command()
def sweep(
    variant: List[str] = typer.Option(
        [],
        "--variant",
        "-v",
        help="Variants to include (repeatable). Defaults to all.",
    ),
    size_kb: List[int] = typer.Option(
        [16, 64, 256, 1024, 4096],
        "--size-kb",
        "-s",
        help="Working-set sizes (KB) to sweep (repeatable).",
    ),
    seed: int = typer.Option(
        settings.SEED,
        "--seed",
        help="Seed for synthetic data generation.",
    ),
    output: Path = typer.Option(
        settings.OUTPUT_DIR / "sweep_results.csv",
        "--output",
        "-o",
        help="Path to write sweep results CSV.",
    ),
):
    """
    Run a batch-size sweep across variants and working set sizes.
    """
    variants = [v.lower() for v in variant] or list(VARIANT_REGISTRY.keys())
    results = bench.sweep(variants=variants, sizes_kb=size_kb, seed=seed)
    if results:
        bench.write_results_csv(results, output)
        console.print(
            f"[bold green]Sweep complete[/bold green]. Results written to {output}"
        )
    else:
        console.print("[yellow]No results produced.[/yellow]")


if __name__ == "__main__":
    app()
