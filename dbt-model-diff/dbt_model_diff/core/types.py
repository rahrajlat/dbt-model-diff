"""Core types used across adapters and flow."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WarehouseConnInfo:
    """Minimal connection info resolved from dbt profiles."""

    type: str
    host: str
    user: str
    password: str
    port: int
    dbname: str
