"""End-to-end diff flow (Option 2): build -> copy -> build -> copy -> compare.

This module computes a structured result dict. Rendering is handled by formatters.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional

from dbt_model_diff.adapters.base import WarehouseAdapter
from dbt_model_diff.core.dbt_runner import dbt_build
from dbt_model_diff.core.manifest import get_model_node, parse_relation_name_pg
from dbt_model_diff.core.subprocess_utils import run
from dbt_model_diff.core.types import WarehouseConnInfo
from dbt_model_diff.core.util import pct, sanitize_ident


def run_diff(
    adapter: WarehouseAdapter,
    conn_info: WarehouseConnInfo,
    project_dir: Path,
    profiles_dir: Path,
    model: str,
    base_ref: str,
    head_ref: str,
    key_cols: list[str],
    where: Optional[str],
    sample: int,
    keep_schemas: bool,
    col_stats: bool,
    target: Optional[str],
    verbose: bool = True,
) -> dict[str, Any]:
    """Run the full Option 2 diff flow and return a structured result dict.
    
    Compares dbt model outputs between two git references by building and copying
    snapshots of each version, then analyzing schema and row-level differences.
    
    Args:
        adapter: Warehouse adapter for database operations.
        conn_info: Connection info for the warehouse.
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        model: Name of the dbt model to compare.
        base_ref: Git reference (branch/commit) for the base version.
        head_ref: Git reference (branch/commit) for the head version.
        key_cols: List of column names to use as primary keys for row-level diff.
        where: Optional SQL WHERE clause to filter rows.
        sample: Maximum number of changed rows to sample.
        keep_schemas: If True, preserve diff schema after completion.
        col_stats: If True, compute column-level statistics.
        target: Optional dbt target name.
        verbose: If True, print progress messages.
    
    Returns:
        A dict containing metadata, row counts, schema differences, column profiles,
        and row-level diff results (added/removed/changed counts and sample keys).
    """

    repo_root = Path(
        run(["git", "-C", str(project_dir), "rev-parse", "--show-toplevel"]).strip()
    ).resolve()
    project_rel = project_dir.relative_to(repo_root)

    run_id = sanitize_ident(f"{model}_{base_ref}_{head_ref}")
    diff_schema = f"dbt_model_diff__{run_id}"
    base_table = f"{sanitize_ident(model)}__base"
    head_table = f"{sanitize_ident(model)}__head"

    conn = adapter.connect(conn_info)
    conn.autocommit = True

    tmp_dir = Path(tempfile.mkdtemp(prefix="dbt-model-diff-"))
    wt_base = tmp_dir / "base"
    wt_head = tmp_dir / "head"

    result: dict[str, Any] = {
        "meta": {
            "model": model,
            "base": base_ref,
            "head": head_ref,
            "mode": "FULL_DIFF" if key_cols else "STATS_ONLY",
            "keys": key_cols,
            "diff_schema": diff_schema,
            "tables": {"base": base_table, "head": head_table},
        },
        "rowcounts": {},
        "schema_diff": {"only_in_base": [], "only_in_head": [], "common": []},
        "column_profile": {},
        "row_diff": None,
    }

    try:
        adapter.ensure_schema(conn, diff_schema)

        # Create worktrees
        run(["git", "-C", str(repo_root), "worktree", "add", "--force", str(wt_base), base_ref])
        run(["git", "-C", str(repo_root), "worktree", "add", "--force", str(wt_head), head_ref])

        wt_base_project = wt_base / project_rel
        wt_head_project = wt_head / project_rel

        # BASE build + immediate copy
        if verbose:
            print(f"dbt build (base: {base_ref})")
        dbt_build(wt_base_project, profiles_dir, model, target)
        base_node = get_model_node(wt_base_project, model)
        base_relname = base_node.get("relation_name")
        base_src_schema, base_src_ident = parse_relation_name_pg(base_relname)
        adapter.ctas_copy(conn, base_src_schema, base_src_ident, diff_schema, base_table)

        # HEAD build + immediate copy
        if verbose:
            print(f"dbt build (head: {head_ref})")
        dbt_build(wt_head_project, profiles_dir, model, target)
        head_node = get_model_node(wt_head_project, model)
        head_relname = head_node.get("relation_name")
        head_src_schema, head_src_ident = parse_relation_name_pg(head_relname)
        adapter.ctas_copy(conn, head_src_schema, head_src_ident, diff_schema, head_table)

        # Compare snapshots
        q = adapter.quote_ident
        base_rel = f"{q(diff_schema)}.{q(base_table)}"
        head_rel = f"{q(diff_schema)}.{q(head_table)}"

        predicate = f" where {where} " if where else ""
        base_sub = f"(select * from {base_rel}{predicate})"
        head_sub = f"(select * from {head_rel}{predicate})"

        base_count = adapter.rowcount(conn, base_sub)
        head_count = adapter.rowcount(conn, head_sub)
        result["rowcounts"] = {"base": base_count, "head": head_count}

        head_cols = adapter.list_columns(conn, diff_schema, head_table)
        base_cols = adapter.list_columns(conn, diff_schema, base_table)
        head_set = set(head_cols)
        base_set = set(base_cols)

        common_cols = [c for c in head_cols if c in base_set]
        only_in_head = [c for c in head_cols if c not in base_set]
        only_in_base = [c for c in base_cols if c not in head_set]

        result["schema_diff"] = {
            "only_in_base": only_in_base,
            "only_in_head": only_in_head,
            "common": common_cols,
        }

        if col_stats and common_cols:
            base_prof = adapter.column_profile(conn, diff_schema, base_table, common_cols)
            head_prof = adapter.column_profile(conn, diff_schema, head_table, common_cols)

            col_out: dict[str, Any] = {}
            for col in common_cols:
                b = base_prof.get(col, {"nulls": 0, "distinct": 0})
                h = head_prof.get(col, {"nulls": 0, "distinct": 0})

                col_out[col] = {
                    "base": {
                        "nulls": int(b["nulls"]),
                        "distinct": int(b["distinct"]),
                        "null_pct": pct(int(b["nulls"]), base_count),
                        "uniq_pct": pct(int(b["distinct"]), base_count),
                    },
                    "head": {
                        "nulls": int(h["nulls"]),
                        "distinct": int(h["distinct"]),
                        "null_pct": pct(int(h["nulls"]), head_count),
                        "uniq_pct": pct(int(h["distinct"]), head_count),
                    },
                }
            result["column_profile"] = col_out

        if not key_cols:
            return result

        # Row-level diff
        non_key_cols = [c for c in common_cols if c not in set(key_cols)]
        base_hash = adapter.build_row_hash_expr(non_key_cols)
        head_hash = adapter.build_row_hash_expr(non_key_cols)

        join_on = " and ".join([f"b.{q(k)} = h.{q(k)}" for k in key_cols])
        using_clause = ", ".join([q(k) for k in key_cols])
        first_key = key_cols[0]

        added = adapter.scalar(
            conn,
            f"""
            select count(*)
            from {head_sub} h
            left join {base_sub} b on {join_on}
            where b.{q(first_key)} is null
            """,
        )
        removed = adapter.scalar(
            conn,
            f"""
            select count(*)
            from {base_sub} b
            left join {head_sub} h on {join_on}
            where h.{q(first_key)} is null
            """,
        )
        changed = adapter.scalar(
            conn,
            f"""
            with base_h as (
              select {', '.join([q(k) for k in key_cols])},
                     {base_hash} as row_hash
              from {base_sub} b
            ),
            head_h as (
              select {', '.join([q(k) for k in key_cols])},
                     {head_hash} as row_hash
              from {head_sub} h
            )
            select count(*)
            from base_h b
            join head_h h using ({using_clause})
            where b.row_hash <> h.row_hash
            """,
        )

        sample_keys: list[list[Any]] = []
        if changed and sample > 0:
            rows = adapter.rows(
                conn,
                f"""
                with base_h as (
                  select {', '.join([q(k) for k in key_cols])},
                         {base_hash} as row_hash
                  from {base_sub} b
                ),
                head_h as (
                  select {', '.join([q(k) for k in key_cols])},
                         {head_hash} as row_hash
                  from {head_sub} h
                )
                select {', '.join([f'b.{q(k)}' for k in key_cols])}
                from base_h b
                join head_h h using ({using_clause})
                where b.row_hash <> h.row_hash
                limit {int(sample)}
                """,
            )
            sample_keys = [list(r) for r in rows]

        result["row_diff"] = {
            "added": int(added),
            "removed": int(removed),
            "changed": int(changed),
            "sample_keys": sample_keys,
        }
        return result

    finally:
        # Cleanup worktrees + temp
        try:
            run(["git", "-C", str(repo_root), "worktree", "remove", "--force", str(wt_base)])
        except Exception:
            pass
        try:
            run(["git", "-C", str(repo_root), "worktree", "remove", "--force", str(wt_head)])
        except Exception:
            pass
        shutil.rmtree(tmp_dir, ignore_errors=True)

        if not keep_schemas:
            try:
                adapter.drop_schema(conn, diff_schema)
            except Exception:
                pass

        conn.close()
