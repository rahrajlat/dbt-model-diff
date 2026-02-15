"""Read dbt manifest.json and extract model relation information."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Tuple


def get_model_node(project_dir: Path, model: str) -> dict:
    """
    Retrieve a dbt model node from the manifest.json file.
    
    This function reads the manifest.json file from a dbt project's target directory
    and searches for a model node matching the specified model name.
    
    Args:
        project_dir (Path): The root directory path of the dbt project.
        model (str): The name of the dbt model to retrieve.
    
    Returns:
        dict: The manifest node dictionary containing all metadata and configuration
              for the specified model.
    
    Raises:
        FileNotFoundError: If manifest.json is not found at the expected location
                          (project_dir/target/manifest.json).
        ValueError: If the manifest.json is invalid (nodes key missing or not a dict),
                   or if the specified model is not found in the manifest.
    
    Example:
        >>> from pathlib import Path
        >>> node = get_model_node(Path("/path/to/dbt/project"), "my_model")
        >>> print(node.get("name"))
        my_model
    """
    """Return the manifest node for a model by `name`."""
    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found at: {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    nodes = manifest.get("nodes", {})
    if not isinstance(nodes, dict):
        raise ValueError("Invalid manifest.json: nodes missing")

    for node in nodes.values():
        if node.get("resource_type") == "model" and node.get("name") == model:
            return node

    raise ValueError(f"Model '{model}' not found in manifest.json")


def parse_relation_name_pg(relation_name: str) -> Tuple[str, str]:
    """Parse Postgres/Redshift-style relation_name into (schema, identifier)."""
    quoted = re.findall(r'"([^"]+)"', relation_name or "")
    if len(quoted) >= 2:
        return quoted[-2], quoted[-1]

    parts = [p.strip().strip('"') for p in (relation_name or "").split(".") if p.strip()]
    if len(parts) >= 2:
        return parts[-2], parts[-1]

    raise ValueError(f"Could not parse relation_name: {relation_name}")
