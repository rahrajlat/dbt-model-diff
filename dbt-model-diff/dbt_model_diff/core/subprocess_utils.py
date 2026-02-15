"""Subprocess helper."""

from __future__ import annotations

import subprocess
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None) -> str:
    """
    Execute a shell command and return its standard output.
    
    Runs the specified command in a subprocess and captures its output. If the command
    fails (non-zero exit code), raises a RuntimeError with detailed error information.
    
    Args:
        cmd: List of command and arguments to execute (e.g., ['git', 'status']).
        cwd: Optional working directory path where the command should be executed.
             If None, uses the current working directory. Defaults to None.
    
    Returns:
        str: The standard output (stdout) of the executed command.
    
    Raises:
        RuntimeError: If the command exits with a non-zero return code. The exception
                      message includes the failed command, stdout, and stderr output.
    
    Example:
        >>> output = run(['git', 'log', '--oneline'], cwd=Path('/path/to/repo'))
        >>> print(output)
    """
    """Run a command and return stdout, raising RuntimeError on failure."""
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed:\n  {' '.join(cmd)}\n\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout
