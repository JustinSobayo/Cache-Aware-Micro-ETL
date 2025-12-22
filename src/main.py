import typer
from rich.console import Console
from src.config import settings

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
    console.print("\n[bold cyan]🚀 Cache-Aware Micro-ETL Config[/bold cyan]")
    console.print("-" * 40)
    console.print(f"[bold white]Base Directory:[/bold white]   {settings.BASE_DIR}")
    console.print(f"[bold white]Data Directory:[/bold white]   {settings.DATA_DIR}")
    console.print(f"[bold white]Output Directory:[/bold white] {settings.OUTPUT_DIR}")
    console.print(f"[bold white]Default Rows:[/bold white]     {settings.DEFAULT_ROWS:,}")
    console.print("-" * 40 + "\n")

@app.command()
def generate():
    """
    🧪 (Placeholder) Generate synthetic datasets for benchmarking.
    """
    console.print("[yellow]Coming soon in Milestone 1: Synthetic Dataset Generator[/yellow]")

@app.command()
def run():
    """
    ▶︎ (Placeholder) Run a specific ETL pipeline variant.
    """
    console.print("[yellow]Coming soon in Milestone 2: Pipeline Variants A-G[/yellow]")

if __name__ == "__main__":
    app()
