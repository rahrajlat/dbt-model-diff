"""CLI entrypoint for dbt-model-diff."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from dbt_model_diff.adapters.postgres import PostgresAdapter
from dbt_model_diff.adapters.redshift import RedshiftAdapter
from dbt_model_diff.core.dbt_profiles import load_conn_info_and_type
from dbt_model_diff.core.diff_flow import run_diff
from dbt_model_diff.formatters.json_fmt import render as render_json
from dbt_model_diff.formatters.markdown_fmt import render as render_markdown
from dbt_model_diff.formatters.rich_fmt import render as render_rich

app = typer.Typer(add_completion=False)


def _get_adapter(adapter_type: str):
    """Return adapter instance for adapter_type."""
    if adapter_type == "postgres":
        return PostgresAdapter()
    if adapter_type == "redshift":
        return RedshiftAdapter()
    raise typer.BadParameter(f"Unsupported adapter type: {adapter_type}")


@app.command("diff")
def diff_cmd(
    model: str = typer.Argument(...,
                                help="dbt model name (e.g. dim_customers)"),
    keys: Optional[str] = typer.Option(
        None,
        "--keys",
        help="Comma-separated key columns. If omitted, runs STATS ONLY.",
    ),
    base: str = typer.Option("main", help="Base git ref/branch"),
    head: str = typer.Option("HEAD", help="Head git ref/branch"),
    project_dir: Path = typer.Option(
        Path("."),
        help="dbt project directory (must contain dbt_project.yml)",
    ),
    profiles_dir: Path = typer.Option(
        Path("."),
        help="dbt profiles directory (must contain profiles.yml)",
    ),
    profile: Optional[str] = typer.Option(
        None, help="dbt profile name (optional)"),
    target: Optional[str] = typer.Option(
        None, help="dbt target name (optional)"),
    where: Optional[str] = typer.Option(
        None, help="Optional SQL predicate applied to both sides"),
    sample: int = typer.Option(
        20, help="Sample changed keys to include (FULL DIFF only)"),
    keep_schemas: bool = typer.Option(
        False, help="Keep diff schema/tables after run"),
    col_stats: bool = typer.Option(
        True,
        "--col-stats/--no-col-stats",
        help="Print/emit null% + distinct + uniqueness% per common column",
    ),
    fmt: str = typer.Option(
        "rich",
        "--format",
        help="Output format: rich | json | markdown",
    ),
):
    """
Compare dbt model output between two git refs and render results.

Executes a comprehensive diff analysis on a specified dbt model between two git references.
This command loads database connection information, runs the diff operation, and outputs
results in the specified format.

Args:
    model: The name of the dbt model to compare (e.g., dim_customers).
    keys: Comma-separated list of key column names for row-level comparison. 
          If omitted, only statistical comparison is performed.
    base: Base git reference or branch name (default: "main").
    head: Head git reference or branch name (default: "HEAD").
    project_dir: Path to the dbt project directory containing dbt_project.yml (default: current directory).
    profiles_dir: Path to the dbt profiles directory containing profiles.yml (default: current directory).
    profile: Optional dbt profile name to use from profiles.yml.
    target: Optional dbt target name to use within the profile.
    where: Optional SQL WHERE clause to filter rows on both sides of the comparison.
    sample: Number of changed keys to sample for detailed row-level diff output (default: 20).
    keep_schemas: If True, preserves diff schema and temporary tables after execution (default: False).
    col_stats: If True, include per-column statistics (null%, distinct count, uniqueness%) 
               in output (default: True).
    fmt: Output format for results (default: "rich"). Options: "rich", "json", "markdown".

Raises:
    typer.BadParameter: If the specified format is not one of: rich, json, markdown.
    Exception: If database connection cannot be established or diff operation fails.

Returns:
    None. Outputs results to stdout in the specified format.
"""
    fmt = fmt.lower().strip()
    if fmt not in {"rich", "json", "markdown"}:
        raise typer.BadParameter(
            "--format must be one of: rich, json, markdown")

    project_dir = project_dir.expanduser().resolve()
    profiles_dir = profiles_dir.expanduser().resolve()

    key_cols = [k.strip()
                for k in keys.split(",") if k.strip()] if keys else []

    conn_info, adapter_type = load_conn_info_and_type(
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
    )
    adapter = _get_adapter(adapter_type)

    result = run_diff(
        adapter=adapter,
        conn_info=conn_info,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        model=model,
        base_ref=base,
        head_ref=head,
        key_cols=key_cols,
        where=where,
        sample=sample,
        keep_schemas=keep_schemas,
        col_stats=col_stats,
        target=target,
        verbose=(fmt == "rich"),
    )

    if fmt == "rich":
        render_rich(result)
        return

    if fmt == "json":
        sys.stdout.write(render_json(result) + "\n")
        return

    sys.stdout.write(render_markdown(result) + "\n")


def main() -> None:
    """Console script entrypoint."""
    app()


if __name__ == "__main__":
    main()
