#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define SQLAlchemy tables
"""


### this needs to be accessed through get_tables,
### otherwise it will attempt to connect to the SQL server
### before FastAPI and break things
#  tables = dict()

### store a tables dict for each connector
connector_tables = dict()

def get_tables(
        mrsm_instance : str = None,
        debug : bool = None
    ):
    """
    Substantiate and create sqlalchemy tables
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import attempt_import, parse_instance_keys
    from meerschaum import get_connector

    sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import('sqlalchemy', 'sqlalchemy.dialects.postgresql')

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
        if debug: dprint(f"Creating tables for connector '{conn}'")
        _tables = {
            'metrics' : sqlalchemy.Table(
                'metrics',
                conn.metadata,
                sqlalchemy.Column('metric_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('connector_keys', sqlalchemy.String, index=True),
                sqlalchemy.Column('metric_key', sqlalchemy.String, index=True),
                sqlalchemy.Column('metric_name', sqlalchemy.String)
            ),
            'locations' : sqlalchemy.Table(
                'locations',
                conn.metadata,
                sqlalchemy.Column('location_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('connector_keys', sqlalchemy.String, index=True),
                sqlalchemy.Column('location_key', sqlalchemy.String, index=True),
                sqlalchemy.Column('location_name', sqlalchemy.String)
            ),
        }

        params_type = sqlalchemy.String
        ### leveage PostgreSQL JSON data type
        if conn.flavor in ('postgresql', 'timescaledb'):
            params_type = sqlalchemy_dialects_postgresql.JSON

        _tables['pipes'] = sqlalchemy.Table(
            "pipes",
            conn.metadata,
            sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("connector_keys", sqlalchemy.String, index=True, nullable=False),
            sqlalchemy.Column("metric_key", sqlalchemy.String, index=True, nullable=False),
            sqlalchemy.Column("location_key", sqlalchemy.String, index=True),
            sqlalchemy.Column("parameters", params_type),
            sqlalchemy.UniqueConstraint('connector_keys', 'metric_key', 'location_key', name='pipe_index')
        )
        try:
            conn.metadata.create_all()
        except Exception as e:
            warn(str(e))

        ### store the table dict for reuse (per connector)
        connector_tables[conn] = _tables
    
    return connector_tables[conn]

