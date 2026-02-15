from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest


PG_CONTAINER = "dbt_model_diff_test_pg"
PG_PORT = 55432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "postgres"


def _have_cmd(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) != 0


def _run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> str:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"Command failed:\n  {' '.join(cmd)}\n\nSTDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}"
        )
    return p.stdout


def _try_run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> tuple[bool, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        return False, (p.stdout + "\n" + p.stderr)
    return True, p.stdout


def _docker_rm(name: str) -> None:
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)


def _start_postgres() -> None:
    _docker_rm(PG_CONTAINER)
    _run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            PG_CONTAINER,
            "-e",
            f"POSTGRES_PASSWORD={PG_PASSWORD}",
            "-p",
            f"{PG_PORT}:5432",
            "postgres:15",
        ]
    )


def _wait_for_postgres(timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        p = subprocess.run(
            ["docker", "exec", PG_CONTAINER, "pg_isready", "-U", PG_USER],
            capture_output=True,
            text=True,
        )
        if p.returncode == 0:
            return
        time.sleep(0.5)
    raise RuntimeError("Postgres did not become ready in time")


def _psql(sql: str) -> None:
    _run(
        [
            "docker",
            "exec",
            "-e",
            f"PGPASSWORD={PG_PASSWORD}",
            PG_CONTAINER,
            "psql",
            "-U",
            PG_USER,
            "-d",
            PG_DB,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ]
    )


def _init_source_data() -> None:
    _psql("create schema if not exists raw;")
    _psql("drop table if exists raw.customers;")
    _psql(
        """
        create table raw.customers (
          id integer primary key,
          name varchar(50)
        );
        """
    )
    _psql(
        """
        insert into raw.customers (id, name) values
          (1, 'Alice'),
          (2, 'Bob'),
          (3, 'Chandra'),
          (4, 'Deepak');
        """
    )


def _write_fixture_project(repo: Path) -> None:
    (repo / "models").mkdir(parents=True, exist_ok=True)

    (repo / "dbt_project.yml").write_text(
        """name: "mini_project"
version: "1.0.0"
profile: "mini_project"
config-version: 2

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

models:
  mini_project:
    +materialized: table
""",
        encoding="utf-8",
    )

    (repo / "models" / "schema.yml").write_text(
        """version: 2

sources:
  - name: raw
    schema: raw
    tables:
      - name: customers
""",
        encoding="utf-8",
    )

    # Base version: filters out id=4
    (repo / "models" / "dim_customers.sql").write_text(
        """select
  id as customer_id,
  name
from {{ source('raw', 'customers') }}
where id <= 3
order by id
""",
        encoding="utf-8",
    )

    (repo / "profiles.yml").write_text(
        f"""mini_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1
      user: {PG_USER}
      password: {PG_PASSWORD}
      port: {PG_PORT}
      dbname: {PG_DB}
      schema: core
""",
        encoding="utf-8",
    )


def _git(cmd: list[str], repo: Path) -> str:
    return _run(["git", "-C", str(repo), *cmd])


def _build_cli_candidates(repo: Path) -> list[list[str]]:
    """Build candidate command lines for both CLI shapes.

    New shape:
      dbt-model-diff diff <model> ...
    Old shape:
      dbt-model-diff <model> ...
    """
    base_args = [
        "--keys",
        "customer_id",
        "--base",
        "main",
        "--head",
        "feature/include-4",
        "--profiles-dir",
        str(repo),
        "--project-dir",
        str(repo),
        "--format",
        "json",
    ]

    cands: list[list[str]] = []

    exe = "dbt-model-diff" if _have_cmd("dbt-model-diff") else None
    py = os.environ.get("PYTHON", "python")

    if exe:
        cands.append([exe, "diff", "dim_customers", *base_args])
        cands.append([exe, "dim_customers", *base_args])  # backward compat
    cands.append([py, "-m", "dbt_model_diff.cli", "diff", "dim_customers", *base_args])
    cands.append([py, "-m", "dbt_model_diff.cli", "dim_customers", *base_args])  # backward compat

    return cands


@pytest.mark.integration
def test_postgres_e2e_diff(tmp_path: Path):
    if not _have_cmd("docker"):
        pytest.skip("docker not installed")
    if not _have_cmd("git"):
        pytest.skip("git not installed")
    if not _have_cmd("dbt"):
        pytest.skip("dbt not installed (install dbt-postgres)")
    if not _port_is_free(PG_PORT):
        pytest.skip(f"port {PG_PORT} is busy; free it or change PG_PORT in test")

    _start_postgres()
    try:
        _wait_for_postgres()
        _init_source_data()

        repo = tmp_path / "repo"
        repo.mkdir(parents=True, exist_ok=True)
        _write_fixture_project(repo)

        _git(["init"], repo)
        _git(["config", "user.email", "test@example.com"], repo)
        _git(["config", "user.name", "Test"], repo)

        _git(["add", "."], repo)
        _git(["commit", "-m", "base"], repo)
        _git(["branch", "-M", "main"], repo)

        _git(["checkout", "-b", "feature/include-4"], repo)
        (repo / "models" / "dim_customers.sql").write_text(
            """select
  id as customer_id,
  name
from {{ source('raw', 'customers') }}
where id <= 4
order by id
""",
            encoding="utf-8",
        )
        _git(["add", "models/dim_customers.sql"], repo)
        _git(["commit", "-m", "include id=4"], repo)
        _git(["checkout", "main"], repo)

        last_err = ""
        out = None
        for cmd in _build_cli_candidates(repo):
            ok, txt = _try_run(cmd, cwd=repo)
            if ok:
                out = txt
                break
            last_err = txt

        if out is None:
            raise RuntimeError(
                "Could not run dbt-model-diff with any known CLI shape. Last error:\n" + last_err
            )

        result = json.loads(out)

        assert result["rowcounts"]["base"] == 3
        assert result["rowcounts"]["head"] == 4

        rd = result["row_diff"]
        assert rd["added"] == 1
        assert rd["removed"] == 0
        assert rd["changed"] == 0

    finally:
        _docker_rm(PG_CONTAINER)
