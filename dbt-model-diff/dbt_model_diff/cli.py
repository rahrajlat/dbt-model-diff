from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple

import psycopg2
import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------


def run(cmd: list[str], cwd: Path | None = None) -> str:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"Command failed:\n  {' '.join(cmd)}\n\nSTDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}"
        )
    return p.stdout


def sanitize_ident(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s)[:60].lower()


def quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def git_worktree_add(repo_root: Path, ref: str, dest: Path) -> None:
    run(["git", "-C", str(repo_root), "worktree", "add", "--force", str(dest), ref])


def git_worktree_remove(repo_root: Path, dest: Path) -> None:
    try:
        run(["git", "-C", str(repo_root), "worktree", "remove", "--force", str(dest)])
    except Exception:
        pass


# ---------------------------------------------------------------------
# dbt + postgres helpers
# ---------------------------------------------------------------------

@dataclass
class PgTarget:
    host: str
    user: str
    password: str
    port: int
    dbname: str


def load_pg_target(profiles_dir: Path, profile: str | None, target: str | None) -> PgTarget:
    profiles_path = profiles_dir / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(f"profiles.yml not found at {profiles_path}")

    data = yaml.safe_load(profiles_path.read_text())

    if not profile:
        if "DBT_PROFILE" in os.environ:
            profile = os.environ["DBT_PROFILE"]
        elif isinstance(data, dict) and len(data.keys()) == 1:
            profile = list(data.keys())[0]
        else:
            raise ValueError("Multiple dbt profiles found. Provide --profile.")

    prof = data[profile]
    outputs = prof.get("outputs", {})

    if not target:
        target = prof.get("target")

    out = outputs.get(target)
    if not out:
        raise ValueError(
            f"Target '{target}' not found in profile '{profile}'.")

    if out.get("type") != "postgres":
        raise ValueError(
            f"Postgres-only for now. Found type='{out.get('type')}'.")

    return PgTarget(
        host=out["host"],
        user=out["user"],
        password=out.get("password", ""),
        port=int(out.get("port", 5432)),
        dbname=out.get("dbname") or out.get("database"),
    )


def pg_connect(t: PgTarget):
    return psycopg2.connect(
        host=t.host,
        port=t.port,
        dbname=t.dbname,
        user=t.user,
        password=t.password,
    )


def dbt_build(project_dir: Path, profiles_dir: Path, model: str, target: str | None):
    if not (project_dir / "dbt_project.yml").exists():
        raise RuntimeError(f"dbt_project.yml not found in {project_dir}")

    cmd = [
        "dbt", "build",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(profiles_dir),
        "--select", model,
    ]
    if target:
        cmd += ["--target", target]

    run(cmd, cwd=project_dir)


def get_model_node_from_manifest(project_dir: Path, model: str) -> dict:
    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found at {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model" and node.get("name") == model:
            return node
    raise ValueError(f"Model '{model}' not found in manifest.json")


def parse_relation_name_pg(relation_name: str) -> Tuple[str, str]:
    """
    Parse dbt manifest relation_name like:
      "db"."schema"."identifier"  OR  "schema"."identifier"
    Return (schema, identifier).
    """
    quoted = re.findall(r'"([^"]+)"', relation_name or "")
    if len(quoted) >= 2:
        return quoted[-2], quoted[-1]

    parts = [p.strip().strip('"')
             for p in (relation_name or "").split(".") if p.strip()]
    if len(parts) >= 2:
        return parts[-2], parts[-1]

    raise ValueError(f"Could not parse relation_name: {relation_name}")


def list_columns(conn, schema: str, table: str) -> list[str]:
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = %s and table_name = %s
    order by ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]


def build_row_hash_expr(cols: Iterable[str]) -> str:
    parts = [f"coalesce({quote_ident(c)}::text,'<NULL>')" for c in cols]
    if not parts:
        return "md5('')"
    concat = (" || '|' || ").join(parts)
    return f"md5({concat})"


def run_scalar(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def run_rows(conn, sql: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def ensure_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {quote_ident(schema)};")


def drop_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"drop schema if exists {quote_ident(schema)} cascade;")


def ctas_copy(conn, src_schema: str, src_ident: str, dst_schema: str, dst_ident: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"drop table if exists {quote_ident(dst_schema)}.{quote_ident(dst_ident)};")
        cur.execute(
            f"create table {quote_ident(dst_schema)}.{quote_ident(dst_ident)} as "
            f"select * from {quote_ident(src_schema)}.{quote_ident(src_ident)};"
        )


def pct(n: int, d: int) -> float:
    return 0.0 if d == 0 else (n / d) * 100.0


def fetch_column_profile(conn, schema: str, table: str, cols: list[str]) -> dict[str, dict[str, int]]:
    """
    One query per table:
    - null count
    - distinct count
    """
    if not cols:
        return {}

    select_parts: list[str] = []
    for c in cols:
        qc = quote_ident(c)
        select_parts.append(
            f"sum(({qc} is null)::int) as {quote_ident(c + '__nulls')}")
        select_parts.append(
            f"count(distinct {qc}) as {quote_ident(c + '__distinct')}")

    sql = f"select {', '.join(select_parts)} from {quote_ident(schema)}.{quote_ident(table)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    out: dict[str, dict[str, int]] = {}
    idx = 0
    for c in cols:
        nulls = int(row[idx])
        distincts = int(row[idx + 1])
        out[c] = {"nulls": nulls, "distinct": distincts}
        idx += 2
    return out


# ---------------------------------------------------------------------
# CLI (MODEL-first)
# ---------------------------------------------------------------------

@app.command()
def main(
    model: str = typer.Argument(...,
                                help="dbt model name (e.g. dim_customers)"),
    keys: str = typer.Option(..., "--keys",
                             help="Comma-separated primary/unique key columns"),
    base: str = typer.Option("main", help="Base git ref/branch"),
    head: str = typer.Option("HEAD", help="Head git ref/branch"),
    project_dir: Path = typer.Option(
        Path("."), help="Path to dbt project (folder containing dbt_project.yml)"),
    profiles_dir: Path = typer.Option(
        Path("."), help="Path to dbt profiles dir (folder containing profiles.yml)"),
    profile: str | None = typer.Option(
        None, help="dbt profile name (optional)"),
    target: str | None = typer.Option(None, help="dbt target name (optional)"),
    where: str | None = typer.Option(
        None, help="Optional SQL predicate applied to both sides"),
    sample: int = typer.Option(20, help="Sample changed keys to print"),
    keep_schemas: bool = typer.Option(
        False, help="Do not drop diff schema after run"),
    col_stats: bool = typer.Option(
        True, "--col-stats/--no-col-stats", help="Print null% + distinct/uniqueness% per column"),
    max_cols: int = typer.Option(
        50, "--max-cols", help="Limit number of columns profiled (ordered by HEAD column order)"),
):
    kcols = [k.strip() for k in keys.split(",") if k.strip()]
    if not kcols:
        raise typer.BadParameter("Provide at least one key via --keys")

    project_dir = project_dir.expanduser().resolve()
    profiles_dir = profiles_dir.expanduser().resolve()

    if not (project_dir / "dbt_project.yml").exists():
        raise RuntimeError(
            f"--project-dir must contain dbt_project.yml. Not found in: {project_dir}")

    repo_root = Path(run(["git", "-C", str(project_dir),
                     "rev-parse", "--show-toplevel"]).strip()).resolve()
    project_rel = project_dir.relative_to(repo_root)

    run_id = sanitize_ident(f"{model}_{base}_{head}")
    diff_schema = f"dbt_model_diff__{run_id}"
    base_table = f"{sanitize_ident(model)}__base"
    head_table = f"{sanitize_ident(model)}__head"

    console.print(
        Panel.fit(
            f"[bold]{model}[/bold]\n"
            f"base={base}  head={head}\n"
            f"keys={', '.join(kcols)}\n"
            f"diff_schema={diff_schema}\n"
            f"tables:\n  {base_table}\n  {head_table}",
            title="dbt-model-diff (data) — Postgres v1 (Option 2, fixed + col stats)",
        )
    )

    tmp = Path(tempfile.mkdtemp(prefix="dbt-model-diff-"))
    wt_base = tmp / "base"
    wt_head = tmp / "head"

    pg = load_pg_target(profiles_dir, profile, target)
    conn = pg_connect(pg)
    conn.autocommit = True

    try:
        ensure_schema(conn, diff_schema)

        # worktrees
        git_worktree_add(repo_root, base, wt_base)
        git_worktree_add(repo_root, head, wt_head)

        wt_base_project = wt_base / project_rel
        wt_head_project = wt_head / project_rel

        # -------------------------
        # BASE: build then copy NOW
        # -------------------------
        console.print(f"[cyan]dbt build (base: {base})[/cyan]")
        dbt_build(wt_base_project, profiles_dir, model, target)

        base_node = get_model_node_from_manifest(wt_base_project, model)
        base_relname = base_node.get("relation_name")
        console.print(f"[dim]base relation_name: {base_relname}[/dim]")

        base_src_schema, base_src_ident = parse_relation_name_pg(base_relname)
        console.print(
            f"[cyan]Copying base relation → {diff_schema}.{base_table}[/cyan]")
        ctas_copy(conn, base_src_schema, base_src_ident,
                  diff_schema, base_table)

        # -------------------------
        # HEAD: build then copy NOW
        # -------------------------
        console.print(f"[cyan]dbt build (head: {head})[/cyan]")
        dbt_build(wt_head_project, profiles_dir, model, target)

        head_node = get_model_node_from_manifest(wt_head_project, model)
        head_relname = head_node.get("relation_name")
        console.print(f"[dim]head relation_name: {head_relname}[/dim]")

        head_src_schema, head_src_ident = parse_relation_name_pg(head_relname)
        console.print(
            f"[cyan]Copying head relation → {diff_schema}.{head_table}[/cyan]")
        ctas_copy(conn, head_src_schema, head_src_ident,
                  diff_schema, head_table)

        # -------------------------
        # DIFF the controlled tables
        # -------------------------
        base_rel = f"{quote_ident(diff_schema)}.{quote_ident(base_table)}"
        head_rel = f"{quote_ident(diff_schema)}.{quote_ident(head_table)}"

        predicate = f" where {where} " if where else ""
        base_sub = f"(select * from {base_rel}{predicate})"
        head_sub = f"(select * from {head_rel}{predicate})"

        # columns
        head_cols = list_columns(conn, diff_schema, head_table)
        base_cols = list_columns(conn, diff_schema, base_table)
        head_set, base_set = set(head_cols), set(base_cols)

        common_cols = [c for c in head_cols if c in base_set]
        only_in_head = [c for c in head_cols if c not in base_set]
        only_in_base = [c for c in base_cols if c not in head_set]

        non_key_cols = [c for c in common_cols if c not in set(kcols)]

        # rowcounts
        base_count = run_scalar(conn, f"select count(*) from {base_sub} b")
        head_count = run_scalar(conn, f"select count(*) from {head_sub} h")

        # added/removed
        join_on = " and ".join(
            [f"b.{quote_ident(k)} = h.{quote_ident(k)}" for k in kcols])
        first_key = kcols[0]

        added = run_scalar(
            conn,
            f"""
            select count(*)
            from {head_sub} h
            left join {base_sub} b on {join_on}
            where b.{quote_ident(first_key)} is null
            """,
        )

        removed = run_scalar(
            conn,
            f"""
            select count(*)
            from {base_sub} b
            left join {head_sub} h on {join_on}
            where h.{quote_ident(first_key)} is null
            """,
        )

        # changed via hash (common non-key cols only)
        base_hash = build_row_hash_expr(non_key_cols)
        head_hash = build_row_hash_expr(non_key_cols)
        using_clause = ", ".join([quote_ident(k) for k in kcols])

        changed = run_scalar(
            conn,
            f"""
            with base_h as (
              select {", ".join([quote_ident(k) for k in kcols])},
                     {base_hash} as row_hash
              from {base_sub} b
            ),
            head_h as (
              select {", ".join([quote_ident(k) for k in kcols])},
                     {head_hash} as row_hash
              from {head_sub} h
            )
            select count(*)
            from base_h b
            join head_h h using ({using_clause})
            where b.row_hash <> h.row_hash
            """,
        )

        tbl = Table(title="Summary")
        tbl.add_column("Metric")
        tbl.add_column("Value", justify="right")
        tbl.add_row("Base rowcount", str(base_count))
        tbl.add_row("Head rowcount", str(head_count))
        tbl.add_row("Added rows", str(added))
        tbl.add_row("Removed rows", str(removed))
        tbl.add_row("Changed rows", str(changed))
        console.print(tbl)

        if only_in_head:
            console.print(
                "[yellow]Columns only in HEAD:[/yellow] " + ", ".join(only_in_head))
        if only_in_base:
            console.print(
                "[yellow]Columns only in BASE:[/yellow] " + ", ".join(only_in_base))

        # -------------------------
        # Column profiling (nulls%, distinct, uniqueness%)
        # -------------------------
        if col_stats:
            prof_cols = common_cols[: max_cols if max_cols >
                                    0 else len(common_cols)]

            base_prof = fetch_column_profile(
                conn, diff_schema, base_table, prof_cols)
            head_prof = fetch_column_profile(
                conn, diff_schema, head_table, prof_cols)

            prof_tbl = Table(
                title=f"Column profile (first {len(prof_cols)} common columns)")
            prof_tbl.add_column("Column")
            prof_tbl.add_column("Base null %", justify="right")
            prof_tbl.add_column("Head null %", justify="right")
            prof_tbl.add_column("Base distinct", justify="right")
            prof_tbl.add_column("Head distinct", justify="right")
            prof_tbl.add_column("Base uniq %", justify="right")
            prof_tbl.add_column("Head uniq %", justify="right")

            for c in prof_cols:
                b = base_prof.get(c, {"nulls": 0, "distinct": 0})
                h = head_prof.get(c, {"nulls": 0, "distinct": 0})

                b_null_pct = pct(b["nulls"], base_count)
                h_null_pct = pct(h["nulls"], head_count)

                b_uniq_pct = pct(b["distinct"], base_count)
                h_uniq_pct = pct(h["distinct"], head_count)

                prof_tbl.add_row(
                    c,
                    f"{b_null_pct:.1f}",
                    f"{h_null_pct:.1f}",
                    str(b["distinct"]),
                    str(h["distinct"]),
                    f"{b_uniq_pct:.1f}",
                    f"{h_uniq_pct:.1f}",
                )

            console.print(prof_tbl)

        # sample changed keys
        if changed and sample > 0:
            rows = run_rows(
                conn,
                f"""
                with base_h as (
                  select {", ".join([quote_ident(k) for k in kcols])},
                         {base_hash} as row_hash
                  from {base_sub} b
                ),
                head_h as (
                  select {", ".join([quote_ident(k) for k in kcols])},
                         {head_hash} as row_hash
                  from {head_sub} h
                )
                select {", ".join([f"b.{quote_ident(k)}" for k in kcols])}
                from base_h b
                join head_h h using ({using_clause})
                where b.row_hash <> h.row_hash
                limit {int(sample)}
                """,
            )

            samp = Table(title=f"Sample changed keys (limit {sample})")
            for k in kcols:
                samp.add_column(k)
            for r in rows:
                samp.add_row(*[str(x) for x in r])
            console.print(samp)

    finally:
        # cleanup worktrees
        try:
            git_worktree_remove(repo_root, wt_base)
            git_worktree_remove(repo_root, wt_head)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        # cleanup diff schema
        if not keep_schemas:
            try:
                drop_schema(conn, diff_schema)
            except Exception:
                pass

        conn.close()


if __name__ == "__main__":
    app()
