"""JSON formatter."""

from __future__ import annotations

import json
from typing import Any


def render(result: dict[str, Any]) -> str:
    """
    Render a diff result as pretty JSON.
    
    Converts a dictionary containing diff results into a formatted JSON string
    with consistent indentation and sorted keys for readability.
    
    Args:
        result: A dictionary containing the diff results to be rendered.
                Keys and values can be of any type.
    
    Returns:
        A string containing the JSON representation of the input dictionary,
        formatted with 2-space indentation and alphabetically sorted keys.
    
    Note:
        Non-serializable objects are converted to strings using str().
    """
    """Render a diff result as pretty JSON."""
    return json.dumps(result, indent=2, sort_keys=True, default=str)
