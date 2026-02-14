from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import psycopg2
import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()


# ----------------------------
# helpers
# ----------------------------

def run(cmd: list[str], cwd: Path | None = None) -> str:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None,
                       capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            f"Command failed:\n  {' '.join(cmd)}\n\nSTDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}"
        )
    return p.stdout


def sanitize_ident(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s)[:60].lower()


def quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def git_worktree_add(repo: Path, ref: str, dest: Path) -> None:
    run(["git", "-C", str(repo), "worktree", "add", "--force", str(dest), ref])


def git_worktree_remove(repo: Path, dest: Path) -> None:
    try:
        run(["git", "-C", str(repo), "worktree", "remove", "--force", str(dest)])
    except Exception:
        pass


@dataclass
class PgTarget:
    host: str
    user: str
    password: str
    port: int
    dbname: str


def load_pg_target(profiles_dir: Path, profile: str | None, target: str | None) -> PgTarget:
    profiles_path = profiles_dir / "profiles.yml"
    data = yaml.safe_load(profiles_path.read_text())

    if not profile:
        if "DBT_PROFILE" in os.environ:
            profile = os.environ["DBT_PROFILE"]
        elif len(data.keys()) == 1:
            profile = list(data.keys())[0]
        else:
            raise ValueError("Multiple dbt profiles found. Provide --profile.")

    prof = data[profile]
    outputs = prof.get("outputs", {})
    if not outputs:
        raise ValueError(f"No outputs found for profile '{profile}'.")

    if not target:
        target = prof.get("target")
    if not target:
        raise ValueError(
            f"No target specified and profile '{profile}' has no default target.")

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


def dbt_build(project_dir: Path, profiles_dir: Path, model: str, forced_schema: str, target: str | None) -> None:
    cmd = [
        "dbt", "build",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(profiles_dir),
        "--select", model,
        "--vars", json.dumps({"dbt_diff_schema": forced_schema}),
    ]
    if target:
        cmd += ["--target", target]
    run(cmd, cwd=project_dir)


def get_relation_identifier_from_manifest(project_dir: Path, model: str) -> str:
    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. Run dbt once to generate it.")
    manifest = json.loads(manifest_path.read_text())

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model" and node.get("name") == model:
            return node.get("alias") or node.get("name")

    raise ValueError(f"Model '{model}' not found in manifest.json")


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


# ----------------------------
# CLI (MODEL-first)
# ----------------------------

@app.command()
def main(
    model: str = typer.Argument(..., help="dbt model name (e.g. stg_orders)"),
    keys: str = typer.Option(..., "--keys",
                             help="Comma-separated primary/unique key columns"),
    base: str = typer.Option("main", help="Base git ref/branch"),
    head: str = typer.Option("HEAD", help="Head git ref/branch"),
    project_dir: Path = typer.Option(
        Path("."), help="Path to dbt project (git repo)"),
    profiles_dir: Path = typer.Option(
        Path("~/.dbt").expanduser(), help="Path to dbt profiles dir"),
    profile: str | None = typer.Option(None, help="dbt profile name"),
    target: str | None = typer.Option(None, help="dbt target name"),
    where: str | None = typer.Option(
        None, help="Optional SQL predicate applied to both sides"),
    sample: int = typer.Option(20, help="Sample changed keys to print"),
    keep_schemas: bool = typer.Option(
        False, help="Do not drop diff schemas after run"),
):
    kcols = [k.strip() for k in keys.split(",") if k.strip()]
    if not kcols:
        raise typer.BadParameter("Provide at least one key column via --keys")

    project_dir = project_dir.resolve()
    repo_dir = project_dir  # v1 assumption: dbt project is the git repo root

    run_id = sanitize_ident(f"{model}_{base}_{head}")
    base_schema = f"dbt_diff__base__{run_id}"
    head_schema = f"dbt_diff__head__{run_id}"

    console.print(
        Panel.fit(
            f"[bold]{model}[/bold]\nbase={base}  head={head}\nkeys={', '.join(kcols)}\n"
            f"schemas:\n  {base_schema}\n  {head_schema}",
            title="dbt-model-diff (data) — Postgres v1",
        )
    )

    tmp = Path(tempfile.mkdtemp(prefix="dbt-model-diff-"))
    wt_base = tmp / "base"
    wt_head = tmp / "head"

    try:
        # worktrees
        git_worktree_add(repo_dir, base, wt_base)
        git_worktree_add(repo_dir, head, wt_head)

        # dbt builds
        console.print(f"[cyan]dbt build (base) → {base_schema}[/cyan]")
        dbt_build(wt_base, profiles_dir, model, base_schema, target)

        console.print(f"[cyan]dbt build (head) → {head_schema}[/cyan]")
        dbt_build(wt_head, profiles_dir, model, head_schema, target)

        # relation identifier (alias/name)
        identifier = get_relation_identifier_from_manifest(wt_head, model)

        # connect
        pg = load_pg_target(profiles_dir, profile, target)
        conn = pg_connect(pg)
        conn.autocommit = True

        try:
            base_rel = f"{quote_ident(base_schema)}.{quote_ident(identifier)}"
            head_rel = f"{quote_ident(head_schema)}.{quote_ident(identifier)}"

            predicate = f" where {where} " if where else ""
            base_sub = f"(select * from {base_rel}{predicate})"
            head_sub = f"(select * from {head_rel}{predicate})"

            # columns (intersection, ordered by head)
            head_cols = list_columns(conn, head_schema, identifier)
            base_cols_set = set(list_columns(conn, base_schema, identifier))
            common_cols = [c for c in head_cols if c in base_cols_set]

            missing_in_base = [c for c in head_cols if c not in base_cols_set]
            # (optional) base-only columns
            # missing_in_head = [c for c in base_cols_set if c not in set(head_cols)]

            non_key_cols = [c for c in common_cols if c not in set(kcols)]

            # counts
            base_count = run_scalar(conn, f"select count(*) from {base_sub} b")
            head_count = run_scalar(conn, f"select count(*) from {head_sub} h")

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

            # print summary
            tbl = Table(title="Summary")
            tbl.add_column("Metric")
            tbl.add_column("Value", justify="right")
            tbl.add_row("Base rowcount", str(base_count))
            tbl.add_row("Head rowcount", str(head_count))
            tbl.add_row("Added rows", str(added))
            tbl.add_row("Removed rows", str(removed))
            tbl.add_row("Changed rows", str(changed))
            console.print(tbl)

            if missing_in_base:
                console.print(
                    "[yellow]Columns present in head only (not included in hash diff):[/yellow]")
                console.print("  " + ", ".join(missing_in_base))

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
            conn.close()

        # drop schemas
        if not keep_schemas:
            pg = load_pg_target(profiles_dir, profile, target)
            conn = pg_connect(pg)
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"drop schema if exists {quote_ident(base_schema)} cascade;")
                    cur.execute(
                        f"drop schema if exists {quote_ident(head_schema)} cascade;")
            finally:
                conn.close()

    finally:
        git_worktree_remove(repo_dir, wt_base)
        git_worktree_remove(repo_dir, wt_head)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    app()
