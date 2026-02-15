"""Adapter protocol for warehouse-specific SQL and connections."""

from __future__ import annotations

from typing import Any, Iterable, Protocol

from dbt_model_diff.core.types import WarehouseConnInfo


class WarehouseAdapter(Protocol):
    """Warehouse adapter protocol."""

    name: str

    def connect(self, info: WarehouseConnInfo):
        ...

    def quote_ident(self, ident: str) -> str:
        ...

    def ensure_schema(self, conn, schema: str) -> None:
        ...

    def drop_schema(self, conn, schema: str) -> None:
        ...

    def ctas_copy(self, conn, src_schema: str, src_table: str, dst_schema: str, dst_table: str) -> None:
        ...

    def list_columns(self, conn, schema: str, table: str) -> list[str]:
        ...

    def rowcount(self, conn, relation_sql: str) -> int:
        ...

    def column_profile(self, conn, schema: str, table: str, cols: list[str]) -> dict[str, dict[str, int]]:
        ...

    def build_row_hash_expr(self, cols: Iterable[str]) -> str:
        ...

    def scalar(self, conn, sql: str) -> int:
        ...

    def rows(self, conn, sql: str) -> list[tuple[Any, ...]]:
        ...
