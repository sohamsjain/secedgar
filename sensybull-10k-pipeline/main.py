"""Main pipeline orchestration for the 10-K analysis pipeline.

Usage:
    python main.py backfill   - Process all watchlist companies once
    python main.py watch      - Continuously poll for new filings
"""

import asyncio
import json
import logging
import sys

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
from rich.text import Text

from analysis.groq_client import GroqAnalyzer
from config import config
from edgar.client import EDGARClient
from edgar.parser import FilingParser
from edgar.watcher import FilingWatcher
from storage.db import SQLiteStorage

console = Console()

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
)
logger = logging.getLogger(__name__)


def print_investment_brief(brief: dict, ticker: str):
    """Print a rich-formatted investment brief panel to the console."""
    signal = brief.get("overall_signal", "unknown")
    signal_colors = {"positive": "green", "neutral": "yellow", "negative": "red"}
    signal_color = signal_colors.get(signal, "white")

    # Build the panel content
    lines = []

    # Header
    company = brief.get("company", ticker)
    fiscal_year = brief.get("fiscal_year", "N/A")
    confidence = brief.get("confidence_score", "N/A")
    lines.append(f"[bold]{company}[/bold] ({ticker})")
    lines.append(f"Fiscal Year: {fiscal_year}")
    lines.append(f"Confidence: {confidence}/10")
    lines.append("")

    # Overall Signal
    signal_text = Text(f"  {signal.upper()}  ", style=f"bold white on {signal_color}")
    lines.append(f"Overall Signal: [{signal_color}]{signal.upper()}[/{signal_color}]")
    lines.append("")

    # Bull Case
    lines.append("[bold green]Bull Case[/bold green]")
    for i, point in enumerate(brief.get("bull_case", []), 1):
        lines.append(f"  {i}. {point}")
    lines.append("")

    # Bear Case
    lines.append("[bold red]Bear Case[/bold red]")
    for i, point in enumerate(brief.get("bear_case", []), 1):
        lines.append(f"  {i}. {point}")
    lines.append("")

    # Top Risks (from risk analysis if available)
    lines.append("[bold yellow]Key Metrics to Watch[/bold yellow]")
    for metric in brief.get("key_metrics_to_watch", []):
        lines.append(f"  - {metric}")

    panel = Panel(
        "\n".join(lines),
        title=f"[bold]Investment Brief: {ticker}[/bold]",
        border_style=signal_color,
        padding=(1, 2),
    )
    console.print(panel)


async def run_pipeline(
    ticker: str,
    cik: str,
    edgar_client: EDGARClient,
    parser: FilingParser,
    analyzer: GroqAnalyzer,
    db: SQLiteStorage,
) -> dict | None:
    """Run the full analysis pipeline for a single company.

    Fetches the latest 10-K, parses sections, runs Groq analysis concurrently,
    generates an investment brief, and saves everything to the database.
    """
    console.rule(f"[bold blue]Processing {ticker}[/bold blue]")

    try:
        # Step 1: Fetch latest 10-K accession
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Fetching latest 10-K for {ticker}...", total=None)
            accession = await edgar_client.get_latest_10k_accession(cik)

        if not accession:
            console.print(f"[yellow]No 10-K filing found for {ticker}[/yellow]")
            return None

        # Check if already processed
        if db.filing_exists(accession):
            console.print(f"[dim]Filing {accession} already processed, skipping[/dim]")
            existing = db.get_brief(ticker)
            if existing:
                return existing["brief_json"]
            return None

        console.print(f"  Accession: [cyan]{accession}[/cyan]")

        # Step 2: Fetch XBRL facts and filing document URL concurrently
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Fetching filing data for {ticker}...", total=None)
            xbrl_facts, doc_url = await asyncio.gather(
                edgar_client.get_xbrl_facts(cik),
                edgar_client.get_filing_document_url(cik, accession),
            )

        if not doc_url:
            console.print(f"[red]Could not find primary document for {ticker}[/red]")
            return None

        # Step 3: Fetch and parse the filing HTML
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Downloading filing for {ticker}...", total=None)
            html = await edgar_client.fetch_filing_html(doc_url)

        metadata = parser.extract_metadata(html)
        clean_text = parser.clean_html(html)
        sections = parser.extract_sections(clean_text)
        validated = parser.validate_sections(sections)
        quality_score = validated.pop("quality_score", 0)

        console.print(f"  Quality Score: [{'green' if quality_score >= 60 else 'yellow'}]{quality_score}/100[/]")
        console.print(f"  Sections found: {', '.join(s for s in validated if s != 'quality_score')}")

        # Step 4: Save filing and sections to DB
        fiscal_year = metadata.get("period_of_report", "")[:4] or ""
        filing_id = db.save_filing(
            ticker=ticker,
            cik=cik,
            accession=accession,
            filed_date=metadata.get("period_of_report", ""),
            fiscal_year=fiscal_year,
        )

        for section_name, section_text in validated.items():
            word_count = len(section_text.split())
            db.save_section(filing_id, section_name, section_text, word_count, quality_score)

        # Step 5: Run Groq analysis concurrently on key sections
        sections_to_analyze = ["business", "risk_factors", "mda"]
        analysis_tasks = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(
                f"Analyzing {ticker} sections with Groq...",
                total=len(sections_to_analyze),
            )

            for section_name in sections_to_analyze:
                if section_name in validated and validated[section_name]:
                    analysis_tasks.append(
                        analyzer.analyze_section(section_name, validated[section_name], ticker)
                    )
                else:
                    logger.warning("Skipping analysis for missing section: %s", section_name)

            analyses_results = await asyncio.gather(*analysis_tasks)

        # Collect results
        analyses = {}
        for result in analyses_results:
            section_name = result.get("_section", "unknown")
            analyses[section_name] = result

            # Save analysis to DB
            model = result.get("_model", config.groq_model)
            db.save_analysis(filing_id, section_name, model, result)

            status = "[red]FAILED[/red]" if "error" in result else "[green]OK[/green]"
            console.print(f"  {section_name}: {status}")

        # Step 6: Generate investment brief
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Generating investment brief for {ticker}...", total=None)
            brief = await analyzer.generate_investment_brief(analyses, ticker, xbrl_facts)

        if "error" not in brief:
            db.save_brief(filing_id, ticker, brief)
            console.print()
            print_investment_brief(brief, ticker)
        else:
            console.print(f"[red]Failed to generate investment brief: {brief.get('error')}[/red]")
            db.save_brief(filing_id, ticker, brief)

        return brief

    except Exception as e:
        console.print(f"[bold red]Error processing {ticker}: {e}[/bold red]")
        logger.exception("Pipeline error for %s", ticker)
        return None


async def run_backfill():
    """Process all watchlist companies once."""
    console.print(
        Panel(
            "[bold]Sensybull 10-K Analysis Pipeline[/bold]\n"
            f"Mode: Backfill | Model: {config.groq_model}\n"
            f"Watchlist: {', '.join(config.watchlist.keys())}",
            border_style="blue",
        )
    )

    errors = config.validate()
    if errors:
        for err in errors:
            console.print(f"[red]Config error: {err}[/red]")
        console.print("[yellow]Set variables in .env file and retry.[/yellow]")
        return

    edgar_client = EDGARClient()
    parser_inst = FilingParser()
    analyzer = GroqAnalyzer()
    db = SQLiteStorage()

    results = {}
    for ticker, cik in config.watchlist.items():
        try:
            brief = await run_pipeline(ticker, cik, edgar_client, parser_inst, analyzer, db)
            results[ticker] = brief
        except Exception as e:
            console.print(f"[bold red]Failed to process {ticker}: {e}[/bold red]")
            results[ticker] = None

    # Summary table
    console.print()
    console.rule("[bold]Summary[/bold]")
    table = Table(title="Processing Results")
    table.add_column("Ticker", style="cyan", justify="center")
    table.add_column("Status", justify="center")
    table.add_column("Signal", justify="center")
    table.add_column("Confidence", justify="center")

    for ticker, brief in results.items():
        if brief and "error" not in brief:
            signal = brief.get("overall_signal", "unknown")
            signal_colors = {"positive": "green", "neutral": "yellow", "negative": "red"}
            color = signal_colors.get(signal, "white")
            table.add_row(
                ticker,
                "[green]OK[/green]",
                f"[{color}]{signal.upper()}[/{color}]",
                str(brief.get("confidence_score", "N/A")),
            )
        else:
            table.add_row(ticker, "[red]FAILED[/red]", "-", "-")

    console.print(table)
    console.print(f"\n[dim]Total Groq tokens used: {analyzer.total_tokens_used}[/dim]")
    db.close()


async def run_watcher():
    """Continuously poll for new filings and process them."""
    console.print(
        Panel(
            "[bold]Sensybull 10-K Analysis Pipeline[/bold]\n"
            f"Mode: Watch | Model: {config.groq_model}\n"
            f"Poll interval: {config.poll_interval}s\n"
            f"Watchlist: {', '.join(config.watchlist.keys())}",
            border_style="green",
        )
    )

    errors = config.validate()
    if errors:
        for err in errors:
            console.print(f"[red]Config error: {err}[/red]")
        console.print("[yellow]Set variables in .env file and retry.[/yellow]")
        return

    edgar_client = EDGARClient()
    parser_inst = FilingParser()
    analyzer = GroqAnalyzer()
    db = SQLiteStorage()

    queue = asyncio.Queue()
    watcher = FilingWatcher(queue, db)

    async def process_events():
        """Process new filing events from the queue."""
        while True:
            event = await queue.get()
            ticker = event["ticker"]
            cik = event["cik"]
            console.print(f"\n[bold green]New filing detected: {ticker}[/bold green]")
            try:
                await run_pipeline(ticker, cik, edgar_client, parser_inst, analyzer, db)
            except Exception as e:
                console.print(f"[bold red]Failed to process {ticker}: {e}[/bold red]")
            queue.task_done()

    # Run watcher and processor concurrently
    console.print("[dim]Watching for new 10-K filings... (Ctrl+C to stop)[/dim]")
    try:
        await asyncio.gather(
            watcher.run(),
            process_events(),
        )
    except KeyboardInterrupt:
        watcher.stop()
        console.print("\n[yellow]Watcher stopped.[/yellow]")
    finally:
        db.close()


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        console.print("[bold]Sensybull 10-K Analysis Pipeline[/bold]")
        console.print()
        console.print("Usage:")
        console.print("  python main.py backfill   Process all watchlist companies now")
        console.print("  python main.py watch      Continuously poll for new filings")
        console.print()
        console.print(f"Watchlist: {', '.join(config.watchlist.keys())}")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "backfill":
        asyncio.run(run_backfill())
    elif mode == "watch":
        asyncio.run(run_watcher())
    else:
        console.print(f"[red]Unknown mode: {mode}[/red]")
        console.print("Use 'backfill' or 'watch'")
        sys.exit(1)


if __name__ == "__main__":
    main()
