"""Redshift adapter implementation.

Redshift is Postgres-like but metadata tables differ. This adapter:
- Uses psycopg2 to connect
- Uses double-quote identifier quoting
- Uses CTAS for copying snapshots
- Lists columns via SVV_COLUMNS (recommended in Redshift docs) citeturn0search13
- Uses md5() for hashing (supported in Redshift) citeturn0search0
"""

from __future__ import annotations

from typing import Any, Iterable

import psycopg2

from dbt_model_diff.core.types import WarehouseConnInfo


class RedshiftAdapter:
    """Amazon Redshift adapter."""

    name = "redshift"

    def connect(self, info: WarehouseConnInfo):
        # Redshift uses the same libpq/psycopg2 connection parameters.
        return psycopg2.connect(
            host=info.host,
            port=info.port,
            dbname=info.dbname,
            user=info.user,
            password=info.password,
        )

    def quote_ident(self, ident: str) -> str:
        # Use simple quoting compatible with Redshift quoted identifiers.
        return '"' + ident.replace('"', '""') + '"'

    def ensure_schema(self, conn, schema: str) -> None:
        with conn.cursor() as cur:
            cur.execute(f"create schema if not exists {self.quote_ident(schema)};")

    def drop_schema(self, conn, schema: str) -> None:
        with conn.cursor() as cur:
            cur.execute(f"drop schema if exists {self.quote_ident(schema)} cascade;")

    def ctas_copy(
        self,
        conn,
        src_schema: str,
        src_table: str,
        dst_schema: str,
        dst_table: str,
    ) -> None:
        q = self.quote_ident
        with conn.cursor() as cur:
            # Redshift supports DROP TABLE IF EXISTS and CTAS. citeturn0search3turn0search11turn0search7
            cur.execute(f"drop table if exists {q(dst_schema)}.{q(dst_table)};")
            cur.execute(
                f"create table {q(dst_schema)}.{q(dst_table)} as "
                f"select * from {q(src_schema)}.{q(src_table)};"
            )

    def list_columns(self, conn, schema: str, table: str) -> list[str]:
        # SVV_COLUMNS includes local/external and is generally safer in Redshift. citeturn0search13
        sql = """
            select column_name
            from svv_columns
            where table_schema = %s and table_name = %s and data_type !='boolean'
            order by ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return [r[0] for r in cur.fetchall()]

    def rowcount(self, conn, relation_sql: str) -> int:
        return self.scalar(conn, f"select count(*) from {relation_sql} t")

    def scalar(self, conn, sql: str) -> int:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def rows(self, conn, sql: str) -> list[tuple[Any, ...]]:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def column_profile(self, conn, schema: str, table: str, cols: list[str]) -> dict[str, dict[str, int]]:
        if not cols:
            return {}

        q = self.quote_ident
        parts: list[str] = []
        for c in cols:
            qc = q(c)
            # Redshift supports boolean expressions in CASE; avoid Postgres-only ::int cast.
            parts.append(f"sum(case when {qc} is null then 1 else 0 end) as {q(c + '__nulls')}")
            parts.append(f"count(distinct {qc}) as {q(c + '__distinct')}")
        sql = f"select {', '.join(parts)} from {q(schema)}.{q(table)}"

        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

        out: dict[str, dict[str, int]] = {}
        idx = 0
        for c in cols:
            out[c] = {"nulls": int(row[idx]), "distinct": int(row[idx + 1])}
            idx += 2
        return out

    def build_row_hash_expr(self, cols: Iterable[str]) -> str:
        cols = list(cols)
        if not cols:
            return "md5('')"

        # Use md5() which exists in Redshift. citeturn0search0
        parts = [f"coalesce({self.quote_ident(c)}::varchar,'<NULL>')" for c in cols]
        concat = " || '|' || ".join(parts)
        return f"md5({concat})"
