#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define SQLAlchemy tables
"""

from __future__ import annotations

import pickle
import threading
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Union, InstanceConnector, List
from meerschaum.utils.warnings import error, warn, dprint

### store a tables dict for each connector
connector_tables = {}
_tables_locks = {}

_sequence_flavors = {'duckdb', 'oracle'}
_skip_index_names_flavors = {'mssql',}

def get_tables(
    mrsm_instance: Optional[Union[str, InstanceConnector]] = None,
    create: Optional[bool] = None,
    refresh: bool = False,
    debug: bool = False,
) -> Union[Dict[str, 'sqlalchemy.Table'], bool]:
    """
    Create tables on the database and return the `sqlalchemy` tables.

    Parameters
    ----------
    mrsm_instance: Optional[Union[str, InstanceConnector]], default None
        The connector on which the tables reside.

    create: Optional[bool], default None
        If `True`, create the tables if they don't exist.

    refresh: bool, default False
        If `True`, invalidate and rebuild any cache.

    debug: bool, default False
        Verbosity Toggle.

    Returns
    -------
    A dictionary of `sqlalchemy.Table` objects if the connector is a `SQLConnector`.
    Otherwise just a bool for `APIConnector` objects.

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors
    from meerschaum import get_connector

    sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import(
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql',
        lazy=False,
    )
    if not sqlalchemy:
        error("Failed to import sqlalchemy. Is sqlalchemy installed?")

    if mrsm_instance is None:
        conn = get_connector(debug=debug)
    elif isinstance(mrsm_instance, str):
        conn = parse_instance_keys(mrsm_instance, debug=debug)
    else: ### NOTE: mrsm_instance MUST BE a SQL Connector for this to work!
        conn = mrsm_instance

    cache_expired = refresh or (
        (
            _check_create_cache(conn, debug=debug)
            if conn.flavor not in ('sqlite', 'duckdb', 'geopackage')
            else True
        )
        if conn.type == 'sql'
        else False
    )
    create = create or cache_expired

    ### Skip if the connector is not a SQL connector.
    if getattr(conn, 'type', None) != 'sql':
        return {}

    conn_key = str(conn)

    if refresh:
        _ = connector_tables.pop(conn_key, None)

    if conn_key in connector_tables:
        return connector_tables[conn_key]

    fasteners = attempt_import('fasteners')
    pickle_path = conn.get_metadata_cache_path(kind='pkl')
    lock_path = pickle_path.with_suffix('.lock')
    lock = fasteners.InterProcessLock(lock_path)

    with lock:
        if not cache_expired and pickle_path.exists():
            try:
                with open(pickle_path, 'rb') as f:
                    metadata = pickle.load(f)
                metadata.bind = conn.engine
                tables = {tbl.name.replace('mrsm_', ''): tbl for tbl in metadata.tables.values()}
                connector_tables[conn_key] = tables
                return tables
            except Exception as e:
                warn(f"Failed to load metadata from cache, rebuilding: {e}")

        if conn_key not in _tables_locks:
            _tables_locks[conn_key] = threading.Lock()

    with _tables_locks[conn_key]:
        if conn_key not in connector_tables:
            if debug:
                dprint(f"Building in-memory instance tables for '{conn}'.")

            id_type = sqlalchemy.Integer
            if conn.flavor in json_flavors:
                from sqlalchemy.dialects.postgresql import JSONB
                params_type = JSONB
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

            pipes_parameters_col = sqlalchemy.Column("parameters", params_type)
            pipes_table_args = [
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
                pipes_parameters_col,
            ]
            if conn.flavor in json_flavors:
                pipes_table_args.append(
                    sqlalchemy.Index(
                        'ix_mrsm_pipes_parameters_tags',
                        pipes_parameters_col['tags'],
                        postgresql_using='gin'
                    )
                )
            _tables['pipes'] = sqlalchemy.Table(
                *pipes_table_args,
                extend_existing = True,
            )

            ### store the table dict for reuse (per connector)
            connector_tables[conn_key] = _tables

            if debug:
                dprint(f"Built in-memory tables for '{conn}'.")

            if create:
                if debug:
                    dprint(f"Creating tables for connector '{conn}'.")

                create_schemas(
                    conn,
                    schemas = [conn.internal_schema],
                    debug = debug,
                )
                create_tables(conn, tables=_tables)

                _write_create_cache(mrsm.get_connector(str(mrsm_instance)), debug=debug)

        if conn.flavor not in ('sqlite', 'duckdb', 'geopackage'):
            with open(pickle_path, 'wb') as f:
                pickle.dump(conn.metadata, f)

    connector_tables[conn_key] = _tables
    return connector_tables[conn_key]


def create_tables(
    conn: mrsm.connectors.SQLConnector,
    tables: Optional[Dict[str, 'sqlalchemy.Table']] = None,
) -> bool:
    """
    Create the tables on the database.
    """
    _tables = tables if tables is not None else get_tables(conn)

    try:
        conn.metadata.create_all(bind=conn.engine)
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(str(e))
        return False
    return True


def create_schemas(
    conn: mrsm.connectors.SQLConnector,
    schemas: List[str],
    debug: bool = False,
) -> bool:
    """
    Create the internal Meerschaum schema on the database.
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import sql_item_name, NO_SCHEMA_FLAVORS, SKIP_IF_EXISTS_FLAVORS
    if conn.flavor in NO_SCHEMA_FLAVORS:
        return True

    _ = attempt_import('sqlalchemy.schema', lazy=False)
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


def _check_create_cache(connector: mrsm.connectors.SQLConnector, debug: bool = False) -> bool:
    """
    Return `True` if the metadata cache is missing or expired.
    """
    import json
    from datetime import datetime, timedelta
    from meerschaum.utils.dtypes import get_current_timestamp

    if connector.type != 'sql':
        return False

    path = connector.get_metadata_cache_path()
    if not path.exists():
        if debug:
            dprint(f"Metadata cache doesn't exist for '{connector}'.")
        return True

    try:
        with open(path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except Exception:
        return True

    created_str = metadata.get('created', None)
    if not created_str:
        return True

    now = get_current_timestamp()
    created = datetime.fromisoformat(created_str)

    delta = now - created
    threshold_minutes = (
        mrsm.get_config('system', 'connectors', 'sql', 'instance', 'create_metadata_cache_minutes')
    )
    threshold_delta = timedelta(minutes=threshold_minutes)
    if delta >= threshold_delta:
        if debug:
            dprint(f"Metadata cache expired for '{connector}'.")
        return True

    if debug:
        dprint(f"Using cached metadata for '{connector}'.")

    return False


def _write_create_cache(connector: mrsm.connectors.SQLConnector, debug: bool = False):
    """
    Write the current timestamp to the cache file.
    """
    if connector.type != 'sql':
        return

    import json
    from meerschaum.utils.dtypes import get_current_timestamp, json_serialize_value

    if debug:
        dprint(f"Writing metadata cache for '{connector}'.")

    path = connector.get_metadata_cache_path()
    now = get_current_timestamp()
    with open(path, 'w+', encoding='utf-8') as f:
        json.dump({'created': now}, f, default=json_serialize_value)
