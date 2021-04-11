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

def get_tables(
        mrsm_instance : Optional[Union[str, meerschaum.connectors.Connector]] = None,
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

    sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import(
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql'
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
            dprint(f"Creating tables for connector '{conn}'")

        params_type = sqlalchemy.String
        ### leverage PostgreSQL JSON data type
        if conn.flavor in ('postgresql', 'timescaledb'):
            params_type = sqlalchemy_dialects_postgresql.JSON

        _tables = {
            'users' : sqlalchemy.Table(
                'users',
                conn.metadata,
                sqlalchemy.Column('user_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('username', sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column('password_hash', sqlalchemy.String),
                sqlalchemy.Column('email', sqlalchemy.String),
                sqlalchemy.Column('user_type', sqlalchemy.String),
                sqlalchemy.Column('attributes', params_type),
                extend_existing = True,
            ),
            'plugins' : sqlalchemy.Table(
                'plugins',
                conn.metadata,
                sqlalchemy.Column('plugin_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('plugin_name', sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column('user_id', sqlalchemy.Integer, nullable=False),
                sqlalchemy.Column('version', sqlalchemy.String),
                sqlalchemy.Column('attributes', params_type),
                extend_existing = True,
            ),
        }

        _tables['pipes'] = sqlalchemy.Table(
            "pipes",
            conn.metadata,
            sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("connector_keys", sqlalchemy.String, index=True, nullable=False),
            sqlalchemy.Column("metric_key", sqlalchemy.String, index=True, nullable=False),
            sqlalchemy.Column("location_key", sqlalchemy.String, index=True),
            sqlalchemy.Column("parameters", params_type),
            sqlalchemy.UniqueConstraint(
                'connector_keys', 'metric_key', 'location_key', name='pipe_index'
            ),
            extend_existing = True,
        )

        try:
            conn.metadata.create_all(bind=conn.engine)
        except Exception as e:
            warn(str(e))

        ### store the table dict for reuse (per connector)
        connector_tables[conn] = _tables

    return connector_tables[conn]
