#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define SQLAlchemy tables
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Union, InstanceConnector

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
    from meerschaum.utils.warnings import error
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
            'users' : sqlalchemy.Table(
                'users',
                conn.metadata,
                sqlalchemy.Column(
                    *id_col_args['user_id'],
                    **id_col_kw['user_id'],
                ),
                sqlalchemy.Column(
                    'username', sqlalchemy.String(256), index=index_names, nullable=False,
                ),
                sqlalchemy.Column('password_hash', sqlalchemy.String(1024)),
                sqlalchemy.Column('email', sqlalchemy.String(256)),
                sqlalchemy.Column('user_type', sqlalchemy.String(256)),
                sqlalchemy.Column('attributes', params_type),
                extend_existing = True,
            ),
            'plugins' : sqlalchemy.Table(
                *([
                    'plugins',
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
                    sqlalchemy.ForeignKeyConstraint(['user_id'], ['users.user_id']),
                ] if conn.flavor != 'duckdb' else [])),
                extend_existing = True,
            ),
        }

        _tables['pipes'] = sqlalchemy.Table(
            "pipes",
            conn.metadata,
            sqlalchemy.Column(
                *id_col_args['pipe_id'],
                **id_col_kw['pipe_id'],
            ),
            sqlalchemy.Column(
                "connector_keys", sqlalchemy.String(256), index=index_names, nullable=False
            ),
            sqlalchemy.Column(
                "metric_key", sqlalchemy.String(256), index=index_names, nullable=False
            ),
            sqlalchemy.Column("location_key", sqlalchemy.String(256), index=index_names),
            sqlalchemy.Column("parameters", params_type),
            extend_existing = True,
        )

        ### store the table dict for reuse (per connector)
        connector_tables[conn] = _tables
        if create:
            create_tables(conn, tables=_tables)

    return connector_tables[conn]


def create_tables(
        conn: 'meerschaum.connectors.SQLConnector',
        tables: Optional[Dict[str, 'sqlalchemy.Table']],
    ) -> bool:
    """
    Create the tables on the database.
    """
    from meerschaum.utils.warnings import warn
    _tables = tables if tables is not None else get_tables(conn)
    try:
        conn.metadata.create_all(bind=conn.engine)
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(str(e))
        return False
    return True


