#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create a test schema that parallel test workers may be creating at the same time.
"""


def create_schema(conn, schema: str) -> None:
    """
    Run `CREATE SCHEMA IF NOT EXISTS` on `conn`. PostgreSQL's `IF NOT EXISTS` is not atomic: when
    two sessions create the same schema at once, the second fails with a unique violation on
    `pg_namespace_nspname_index`. The schema exists either way, so that error is success here.
    """
    from meerschaum.connectors.sql._native import IntegrityError
    try:
        conn.exec(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    except IntegrityError as e:
        if 'pg_namespace_nspname_index' not in str(e):
            raise
