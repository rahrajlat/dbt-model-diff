"""Rich formatter for interactive CLI output."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def render(result: dict[str, Any]) -> None:
    """
    Render a comprehensive diff result using Rich formatted tables and panels.
    
    This function displays the results of a dbt model comparison between base and head
    versions, including metadata, row counts, schema differences, column profiles, and
    row-level changes.
    
    Args:
        result (dict[str, Any]): A dictionary containing the diff analysis results with
            the following expected structure:
            - meta (dict): Metadata about the comparison including:
                - model (str): Name of the dbt model
                - base (str): Base version identifier
                - head (str): Head version identifier
                - mode (str): Comparison mode
                - keys (list): Primary key columns
                - diff_schema (str): Schema used for diff
                - tables (dict): Table names for base and head
            - rowcounts (dict): Row count metrics with 'base' and 'head' counts
            - schema_diff (dict): Schema differences containing:
                - only_in_head (list): Columns present only in head
                - only_in_base (list): Columns present only in base
            - column_profile (dict): Column-level statistics with null percentages,
              distinct counts, and uniqueness percentages for common columns
            - row_diff (dict): Row-level differences containing:
                - added (int): Number of added rows
                - removed (int): Number of removed rows
                - changed (int): Number of changed rows
                - sample_keys (list): Sample of changed row keys
    
    Returns:
        None
    
    Displays:
        - Header panel with model metadata
        - Summary table with base and head rowcounts
        - Schema differences (columns only in HEAD/BASE)
        - Column profile table with statistics for common columns
        - Row-level diff summary table
        - Sample of changed keys (if available)
    """
    """Render a diff result using Rich."""
    meta = result.get("meta", {})
    model = meta.get("model")
    base = meta.get("base")
    head = meta.get("head")
    mode = meta.get("mode")
    keys = meta.get("keys") or []
    diff_schema = meta.get("diff_schema")
    tables = meta.get("tables") or {}

    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"[bold]{model}[/bold]",
                    f"mode={mode}",
                    f"base={base}  head={head}",
                    f"keys={', '.join(keys) if keys else '(none)'}",
                    f"diff_schema={diff_schema}",
                    f"tables: {tables.get('base')} / {tables.get('head')}",
                ]
            ),
            title="dbt-model-diff",
        )
    )

    rc = result.get("rowcounts", {})
    summary = Table(title="Summary")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Base rowcount", str(rc.get("base", 0)))
    summary.add_row("Head rowcount", str(rc.get("head", 0)))
    console.print(summary)

    sd = result.get("schema_diff", {})
    only_head = sd.get("only_in_head") or []
    only_base = sd.get("only_in_base") or []
    if only_head:
        console.print("[yellow]Columns only in HEAD:[/yellow] " + ", ".join(only_head))
    if only_base:
        console.print("[yellow]Columns only in BASE:[/yellow] " + ", ".join(only_base))

    prof = result.get("column_profile") or {}
    if prof:
        prof_t = Table(title=f"Column profile ({len(prof)} common columns)")
        prof_t.add_column("Column")
        prof_t.add_column("Base null %", justify="right")
        prof_t.add_column("Head null %", justify="right")
        prof_t.add_column("Base distinct", justify="right")
        prof_t.add_column("Head distinct", justify="right")
        prof_t.add_column("Base uniq %", justify="right")
        prof_t.add_column("Head uniq %", justify="right")

        for col, v in prof.items():
            b = v.get("base", {})
            h = v.get("head", {})
            prof_t.add_row(
                col,
                f"{b.get('null_pct', 0.0):.1f}",
                f"{h.get('null_pct', 0.0):.1f}",
                str(b.get("distinct", 0)),
                str(h.get("distinct", 0)),
                f"{b.get('uniq_pct', 0.0):.1f}",
                f"{h.get('uniq_pct', 0.0):.1f}",
            )
        console.print(prof_t)

    rd = result.get("row_diff")
    if rd:
        diff = Table(title="Row-level diff")
        diff.add_column("Metric")
        diff.add_column("Value", justify="right")
        diff.add_row("Added rows", str(rd.get("added", 0)))
        diff.add_row("Removed rows", str(rd.get("removed", 0)))
        diff.add_row("Changed rows", str(rd.get("changed", 0)))
        console.print(diff)

        sample = rd.get("sample_keys") or []
        if sample:
            samp = Table(title=f"Sample changed keys (limit {len(sample)})")
            for k in keys:
                samp.add_column(k)
            for row in sample:
                samp.add_row(*[str(x) for x in row])
            console.print(samp)
