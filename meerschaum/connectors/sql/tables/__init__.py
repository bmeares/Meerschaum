#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define SQLAlchemy tables
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Union, InstanceConnector, List
from meerschaum.utils.warnings import error, warn

### store a tables dict for each connector
connector_tables = {}

_sequence_flavors = {'duckdb', 'oracle'}
_skip_index_names_flavors = {'mssql',}

def get_tables(
        mrsm_instance: Optional[Union[str, InstanceConnector]] = None,
        create: bool = True,
        debug: Optional[bool] = None
    ) -> Union[Dict[str, 'sqlalchemy.Table'], bool]:
    """
    Create tables on the database and return the `sqlalchemy` tables.

    Parameters
    ----------
    mrsm_instance: Optional[Union[str, InstanceConnector]], default None
        The connector on which the tables reside.

    create: bool, default True:
        If `True`, create the tables if they don't exist.

    debug: Optional[bool], default None:
        Verbosity Toggle.

    Returns
    -------
    A dictionary of `sqlalchemy.Table` objects if the connector is a `SQLConnector`.
    Otherwise just a bool for `APIConnector` objects.

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors
    from meerschaum import get_connector

    sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import(
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql',
        lazy = False
    )
    if not sqlalchemy:
        error(f"Failed to import sqlalchemy. Is sqlalchemy installed?")

    if mrsm_instance is None:
        conn = get_connector(debug=debug)
    elif isinstance(mrsm_instance, str):
        conn = parse_instance_keys(mrsm_instance, debug=debug)
    else: ### NOTE: mrsm_instance MUST BE a SQL Connector for this to work!
        conn = mrsm_instance

    ### kind of a hack. Create the tables remotely
    from meerschaum.connectors.api import APIConnector
    if isinstance(conn, APIConnector):
        if create:
            return conn.create_metadata(debug=debug)
        return {}

    ### Skip if the connector is not a SQL connector.
    if getattr(conn, 'type', None) != 'sql':
        return {}

    if conn not in connector_tables:
        if debug:
            dprint(f"Creating tables for connector '{conn}'.")

        id_type = sqlalchemy.Integer
        if conn.flavor in json_flavors:
            params_type = sqlalchemy.types.JSON
        else:
            params_type = sqlalchemy.types.Text
        id_names = ('user_id', 'plugin_id', 'pipe_id')
        sequences = {
            k: sqlalchemy.Sequence(k + '_seq')
            for k in id_names 
        }
        id_col_args = { k: [k, id_type] for k in id_names }
        id_col_kw = { k: {'primary_key': True} for k in id_names }
        index_names = conn.flavor not in _skip_index_names_flavors

        if conn.flavor in _sequence_flavors:
            for k, args in id_col_args.items():
                args.append(sequences[k])
            for k, kw in id_col_kw.items():
                kw.update({'server_default': sequences[k].next_value()})

        _tables = {
            'users': sqlalchemy.Table(
                'mrsm_users',
                conn.metadata,
                sqlalchemy.Column(
                    *id_col_args['user_id'],
                    **id_col_kw['user_id'],
                ),
                sqlalchemy.Column(
                    'username',
                    sqlalchemy.String(256),
                    index = index_names,
                    nullable = False,
                ),
                sqlalchemy.Column('password_hash', sqlalchemy.String(1024)),
                sqlalchemy.Column('email', sqlalchemy.String(256)),
                sqlalchemy.Column('user_type', sqlalchemy.String(256)),
                sqlalchemy.Column('attributes', params_type),
                extend_existing = True,
            ),
            'plugins': sqlalchemy.Table(
                *([
                    'mrsm_plugins',
                    conn.metadata,
                    sqlalchemy.Column(
                        *id_col_args['plugin_id'],
                        **id_col_kw['plugin_id'],
                    ),
                    sqlalchemy.Column(
                        'plugin_name', sqlalchemy.String(256), index=index_names, nullable=False,
                    ),
                    sqlalchemy.Column('user_id', sqlalchemy.Integer, nullable=False),
                    sqlalchemy.Column('version', sqlalchemy.String(256)),
                    sqlalchemy.Column('attributes', params_type),
                ] + ([
                    sqlalchemy.ForeignKeyConstraint(['user_id'], ['mrsm_users.user_id']),
                ] if conn.flavor != 'duckdb' else [])),
                extend_existing = True,
            ),
            'temp_tables': sqlalchemy.Table(
                'mrsm_temp_tables',
                conn.metadata,
                sqlalchemy.Column(
                    'date_created',
                    sqlalchemy.DateTime,
                    index = True,
                    nullable = False,
                ),
                sqlalchemy.Column(
                    'table',
                    sqlalchemy.String(256),
                    index = index_names,
                    nullable = False,
                ),
                sqlalchemy.Column(
                    'ready_to_drop',
                    sqlalchemy.DateTime,
                    index = False,
                    nullable = True,
                ),
                extend_existing = True,
            ),
        }

        _tables['pipes'] = sqlalchemy.Table(
            "mrsm_pipes",
            conn.metadata,
            sqlalchemy.Column(
                *id_col_args['pipe_id'],
                **id_col_kw['pipe_id'],
            ),
            sqlalchemy.Column(
                "connector_keys",
                sqlalchemy.String(256),
                index = index_names,
                nullable = False,
            ),
            sqlalchemy.Column(
                "metric_key",
                sqlalchemy.String(256),
                index = index_names,
                nullable = False,
            ),
            sqlalchemy.Column(
                "location_key",
                sqlalchemy.String(256),
                index = index_names,
                nullable = True,
            ),
            sqlalchemy.Column("parameters", params_type),
            extend_existing = True,
        )

        ### store the table dict for reuse (per connector)
        connector_tables[conn] = _tables
        if create:
            create_schemas(
                conn,
                schemas = [conn.internal_schema],
                debug = debug,
            )
            create_tables(conn, tables=_tables)

    return connector_tables[conn]


def create_tables(
        conn: 'meerschaum.connectors.SQLConnector',
        tables: Optional[Dict[str, 'sqlalchemy.Table']] = None,
    ) -> bool:
    """
    Create the tables on the database.
    """
    from meerschaum.utils.sql import get_rename_table_queries, table_exists
    _tables = tables if tables is not None else get_tables(conn)

    rename_queries = []
    for table_key, table in _tables.items():
        if table_exists(
            table_key,
            conn,
            schema = conn.instance_schema,
        ):
            rename_queries.extend(get_rename_table_queries(
                table_key,
                table.name,
                schema = conn.instance_schema,
                flavor = conn.flavor,
            ))
    if rename_queries:
        conn.exec_queries(rename_queries)

    try:
        conn.metadata.create_all(bind=conn.engine)
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(str(e))
        return False
    return True


def create_schemas(
        conn: 'meerschaum.connectors.SQLConnector',
        schemas: List[str],
        debug: bool = False,
    ) -> bool:
    """
    Create the internal Meerschaum schema on the database.
    """
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import sql_item_name, NO_SCHEMA_FLAVORS, SKIP_IF_EXISTS_FLAVORS
    if conn.flavor in NO_SCHEMA_FLAVORS:
        return True

    sqlalchemy_schema = attempt_import('sqlalchemy.schema')
    successes = {}
    skip_if_not_exists = conn.flavor in SKIP_IF_EXISTS_FLAVORS
    if_not_exists_str = ("IF NOT EXISTS " if not skip_if_not_exists else "")
    with conn.engine.connect() as connection:
        for schema in schemas:
            if not schema:
                continue
            schema_name = sql_item_name(schema, conn.flavor)
            schema_exists = conn.engine.dialect.has_schema(connection, schema)
            if schema_exists:
                continue

            create_schema_query = f"CREATE SCHEMA {if_not_exists_str}{schema_name}"
            try:
                result = conn.exec(create_schema_query, debug=debug)
                successes[schema] = (result is not None)
            except Exception as e:
                warn(f"Failed to create internal schema '{schema}':\n{e}")
    return all(successes.values())
