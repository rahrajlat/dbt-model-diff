"""Load warehouse connection details from dbt profiles.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

import yaml

from dbt_model_diff.core.types import WarehouseConnInfo


def load_conn_info_and_type(
    profiles_dir: Path,
    profile: Optional[str],
    target: Optional[str],
) -> Tuple[WarehouseConnInfo, str]:
    """Load connection info and adapter type from dbt profiles.yml.

    Args:
        profiles_dir: Directory containing profiles.yml file.
        profile: Name of the dbt profile to use. If None, will attempt to resolve
                from DBT_PROFILE environment variable or use the only available profile.
        target: Name of the target within the profile. If None, uses the profile's
               default target.

    Returns:
        Tuple containing WarehouseConnInfo object with connection details and
        the adapter type string (e.g., 'postgres', 'redshift').

    Raises:
        FileNotFoundError: If profiles.yml not found in profiles_dir.
        ValueError: If profiles.yml is invalid, profile/target not found,
                   adapter type is unsupported, or required connection fields are missing.
    """
    profiles_path = profiles_dir / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(f"profiles.yml not found at: {profiles_path}")

    data = yaml.safe_load(profiles_path.read_text())
    if not isinstance(data, dict) or not data:
        raise ValueError("profiles.yml is empty or invalid")

    if not profile:
        if "DBT_PROFILE" in os.environ:
            profile = os.environ["DBT_PROFILE"]
        elif len(data.keys()) == 1:
            profile = list(data.keys())[0]
        else:
            raise ValueError("Multiple profiles found. Provide --profile.")

    prof = data.get(profile)
    if not isinstance(prof, dict):
        raise ValueError(f"Profile '{profile}' not found")

    outputs = prof.get("outputs", {})
    if not isinstance(outputs, dict) or not outputs:
        raise ValueError(f"No outputs found for profile '{profile}'")

    if not target:
        target = prof.get("target")
    if not target:
        raise ValueError(
            f"No target specified and profile '{profile}' has no default target")

    out = outputs.get(target)
    if not isinstance(out, dict):
        raise ValueError(f"Target '{target}' not found in profile '{profile}'")

    adapter_type = out.get("type")
    if adapter_type not in {"postgres", "redshift"}:
        raise ValueError(
            f"Unsupported profile type '{adapter_type}'. Expected postgres/redshift."
        )

    info = WarehouseConnInfo(
        type=adapter_type,
        host=out["host"],
        user=out["user"],
        password=out.get("password", ""),
        port=int(out.get("port", 5432)),
        dbname=out.get("dbname") or out.get("database"),
    )
    return info, adapter_type
