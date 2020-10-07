#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define API SQLAlchemy tables
"""

from meerschaum.api import connector
from meerschaum.utils.misc import attempt_import
sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import('sqlalchemy', 'sqlalchemy.dialects.postgresql')

### this needs to be accessed through get_tables,
### otherwise it will attempt to connect to the SQL server
### before FastAPI and break things
tables = dict()

def get_tables():
    """
    Substantiate and create sqlalchemy tables
    """
    global tables
    if len(tables) == 0:
        tables = {
            'metrics' : sqlalchemy.Table(
                'metrics',
                connector.metadata,
                sqlalchemy.Column('metric_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('connector_keys', sqlalchemy.String, index=True),
                sqlalchemy.Column('metric_key', sqlalchemy.String, index=True),
                sqlalchemy.Column('metric_name', sqlalchemy.String)
            ),
            'locations' : sqlalchemy.Table(
                'locations',
                connector.metadata,
                sqlalchemy.Column('location_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('connector_keys', sqlalchemy.String, index=True),
                sqlalchemy.Column('location_key', sqlalchemy.String, index=True),
                sqlalchemy.Column('location_name', sqlalchemy.String)
            #  'interfaces' : sqlalchemy.Table(
                #  'iterfaces',
                #  connector.metadata,
                #  sqlalchemy.Column('interface_id', sqlalchemy.Integer, primary_key=True),
                #  sqlalchemy.Column('connector_keys')
            ),
        }
        ### leveage PostgreSQL JSON data type
        if connector.flavor in ('postgres', 'timescaledb'):
            tables['pipes'] = sqlalchemy.Table(
                "pipes",
                connector.metadata,
                sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column("connector_keys", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("metric_key", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("location_key", sqlalchemy.String, index=True),
                sqlalchemy.Column("parameters", sqlalchemy_dialects_postgresql.JSON),
                sqlalchemy.UniqueConstraint('connector_keys', 'metric_key', 'location_key', name='pipe_index')
            )
        ### other databases flavors just use text
        else:
            tables['pipes'] = sqlalchemy.Table(
                "pipes",
                connector.metadata,
                sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column("connector_keys", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("metric_key", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("location_key", sqlalchemy.String, index=True),
                sqlalchemy.Column("parameters", sqlalchemy.String),
                sqlalchemy.UniqueConstraint('connector_keys', 'metric_key', 'location_key', name='pipe_index')
            )

        connector.metadata.create_all()
    return tables

