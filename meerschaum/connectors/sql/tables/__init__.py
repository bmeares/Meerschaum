#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define SQLAlchemy tables
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Union

### store a tables dict for each connector
connector_tables = dict()

_sequence_flavors = {'duckdb'}
_skip_index_names_flavors = {'mssql'}

def get_tables(
        mrsm_instance : Optional[Union[str, meerschaum.connectors.Connector]] = None,
        create : bool = True,
        debug : Optional[bool] = None
    ) -> Dict[str, sqlalchemy.Table]:
    """
    Substantiate and create sqlalchemy tables
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.packages import attempt_import
    from meerschaum import get_connector

    sqlalchemy, sqlalchemy_dialects_postgresql, sqlalchemy_utils = attempt_import(
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy_utils',
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
        return conn.create_metadata(debug=debug)

    global connector_tables
    if conn not in connector_tables:
        if debug:
            dprint(f"Creating tables for connector '{conn}'.")

        id_type = sqlalchemy.Integer
        params_type = (
            sqlalchemy_utils.types.json.JSONType if conn.flavor not in ('duckdb')
            else sqlalchemy.String
        )

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
                sqlalchemy.Column('username', sqlalchemy.String(256), index=index_names, nullable=False),
                sqlalchemy.Column('password_hash', sqlalchemy.String(1024)),
                sqlalchemy.Column('email', sqlalchemy.String(256)),
                sqlalchemy.Column('user_type', sqlalchemy.String(256)),
                sqlalchemy.Column('attributes', params_type),
                extend_existing = True,
            ),
            'plugins' : sqlalchemy.Table(
                'plugins',
                conn.metadata,
                sqlalchemy.Column(
                    *id_col_args['plugin_id'],
                    **id_col_kw['plugin_id'],
                ),
                sqlalchemy.Column('plugin_name', sqlalchemy.String(256), index=index_names, nullable=False),
                sqlalchemy.Column('user_id', sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column('version', sqlalchemy.String(256)),
                sqlalchemy.Column('attributes', params_type),
                sqlalchemy.ForeignKeyConstraint(['user_id'], ['users.user_id']),
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
            sqlalchemy.Column("metric_key", sqlalchemy.String(256), index=index_names, nullable=False),
            sqlalchemy.Column("location_key", sqlalchemy.String(256), index=index_names),
            sqlalchemy.Column("parameters", params_type),
            #  sqlalchemy.UniqueConstraint(
                #  'connector_keys', 'metric_key', 'location_key', name='pipe_index'
            #  ),
            extend_existing = True,
        )

        try:
            conn.metadata.create_all(bind=conn.engine)
        except Exception as e:
            #  import traceback
            #  traceback.print_exception(type(e), e, e.__traceback__)
            warn(str(e))

        ### store the table dict for reuse (per connector)
        connector_tables[conn] = _tables

    return connector_tables[conn]
