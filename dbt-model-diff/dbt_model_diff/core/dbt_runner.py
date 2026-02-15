"""dbt CLI runner."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from dbt_model_diff.core.subprocess_utils import run


def dbt_build(project_dir: Path, profiles_dir: Path, model: str, target: Optional[str]) -> None:
    """Run `dbt build --select <model>`.

    Args:
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to the dbt profiles directory.
        model: Name of the model to build.
        target: Optional target name to use in the dbt profile.

    Raises:
        RuntimeError: If dbt_project.yml is not found in the project directory.
    """
    if not (project_dir / "dbt_project.yml").exists():
        raise RuntimeError(f"dbt_project.yml not found in: {project_dir}")

    cmd = [
        "dbt",
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--select",
        model,
    ]
    if target:
        cmd += ["--target", target]

    run(cmd, cwd=project_dir)
