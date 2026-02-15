"""Small shared helpers."""

from __future__ import annotations

import re


def sanitize_ident(value: str, max_len: int = 60) -> str:
    """
    Sanitize a string into a safe identifier fragment.
    
    Converts the input string into a valid identifier by replacing any
    non-alphanumeric characters (excluding underscores) with underscores,
    converting to lowercase, and truncating to the maximum length.
    
    Args:
        value (str): The string to sanitize.
        max_len (int, optional): Maximum length of the resulting identifier.
            Defaults to 60.
    
    Returns:
        str: A sanitized identifier string containing only lowercase letters,
            numbers, and underscores, truncated to max_len characters.
    
    Examples:
        >>> sanitize_ident("My-Model_Name")
        'my_model_name'
        >>> sanitize_ident("Table@Name#2024!", max_len=10)
        'table_name'
    """
    """Sanitize a string into a safe-ish identifier fragment."""
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).lower()[:max_len]


def pct(n: int, d: int) -> float:
    """Compute percent with divide-by-zero safety."""
    return 0.0 if d == 0 else (n / d) * 100.0
