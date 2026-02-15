"""Markdown formatter suitable for PR comments."""

from __future__ import annotations

from typing import Any


def _md_table(rows: list[list[str]]) -> str:
    header = "| " + " | ".join(rows[0]) + " |"
    sep = "|" + "|".join(["---"] * len(rows[0])) + "|"
    body = "\n".join(["| " + " | ".join(r) + " |" for r in rows[1:]])
    return "\n".join([header, sep, body])


def render(result: dict[str, Any]) -> str:
    """
    Render a diff result as Markdown.
    
    Generates a formatted Markdown report comparing two dbt model versions (base and head).
    The report includes metadata, row count metrics, schema differences, column profiles,
    and row-level changes.
    
    Args:
        result (dict[str, Any]): A dictionary containing diff analysis results with the following structure:
            - meta (dict): Metadata about the comparison
                - model (str): Name of the dbt model being compared
                - base (str): Base version identifier
                - head (str): Head version identifier
                - mode (str): Comparison mode
                - keys (list): List of key columns used for comparison
            - rowcounts (dict): Row count statistics
                - base (int): Number of rows in base version
                - head (int): Number of rows in head version
            - schema_diff (dict): Schema-level differences
                - only_in_head (list): Columns present only in head version
                - only_in_base (list): Columns present only in base version
            - column_profile (dict): Statistics for common columns
                - {column_name} (dict): Profile data for each column
                    - base (dict): null_pct, distinct, uniq_pct
                    - head (dict): null_pct, distinct, uniq_pct
            - row_diff (dict): Row-level change statistics
                - added (int): Number of rows added
                - removed (int): Number of rows removed
                - changed (int): Number of rows changed
                - sample_keys (list): Sample of changed row keys
    
    Returns:
        str: Formatted Markdown string containing the complete diff report with sections
             for metadata, row counts, schema differences, column profiles, and row-level changes.
    """
    """Render a diff result as Markdown."""
    meta = result.get("meta", {})
    model = meta.get("model")
    base = meta.get("base")
    head = meta.get("head")
    mode = meta.get("mode")
    keys = meta.get("keys") or []

    lines: list[str] = []
    lines.append(f"## dbt-model-diff: `{model}`")
    lines.append(f"**Base:** `{base}`  |  **Head:** `{head}`")
    lines.append(f"**Mode:** `{mode}`" + (f"  |  **Keys:** `{', '.join(keys)}`" if keys else ""))
    lines.append("")

    rc = result.get("rowcounts", {})
    lines.append(_md_table([["Metric", "Value"], ["Base rowcount", str(rc.get("base", "-"))], ["Head rowcount", str(rc.get("head", "-"))]]))
    lines.append("")

    sd = result.get("schema_diff", {})
    only_head = sd.get("only_in_head") or []
    only_base = sd.get("only_in_base") or []

    if only_head or only_base:
        lines.append("### Schema differences")
        if only_head:
            lines.append(f"- Columns only in **HEAD**: `{', '.join(only_head)}`")
        if only_base:
            lines.append(f"- Columns only in **BASE**: `{', '.join(only_base)}`")
        lines.append("")

    prof = result.get("column_profile") or {}
    if prof:
        lines.append("### Column profile (common columns)")
        rows = [["Column", "Base null %", "Head null %", "Base distinct", "Head distinct", "Base uniq %", "Head uniq %"]]
        for col, v in prof.items():
            b = v.get("base", {})
            h = v.get("head", {})
            rows.append(
                [
                    col,
                    f"{b.get('null_pct', 0.0):.1f}",
                    f"{h.get('null_pct', 0.0):.1f}",
                    str(b.get("distinct", 0)),
                    str(h.get("distinct", 0)),
                    f"{b.get('uniq_pct', 0.0):.1f}",
                    f"{h.get('uniq_pct', 0.0):.1f}",
                ]
            )
        lines.append(_md_table(rows))
        lines.append("")

    rd = result.get("row_diff")
    if rd:
        lines.append("### Row-level diff")
        lines.append(f"- Added: **{rd.get('added', 0)}**")
        lines.append(f"- Removed: **{rd.get('removed', 0)}**")
        lines.append(f"- Changed: **{rd.get('changed', 0)}**")
        sample = rd.get("sample_keys") or []
        if sample:
            lines.append("")

            keys = meta.get("keys") or []
            hdr = keys if keys else ["key"]
            rows = [hdr] + [[str(x) for x in row] for row in sample]
            lines.append("#### Sample changed keys")
            lines.append(_md_table(rows))
        lines.append("")

    return "\n".join(lines).strip()
