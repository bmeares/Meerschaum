#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define API SQLAlchemy tables
"""

from meerschaum.api import connector
from meerschaum.utils.misc import attempt_import
sqlalchemy = attempt_import('sqlalchemy')

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
            'pipes' : sqlalchemy.Table(
                "pipes",
                connector.metadata,
                sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column("connector_keys", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("metric_key", sqlalchemy.String, index=True, nullable=False),
                sqlalchemy.Column("location_key", sqlalchemy.String, index=True),
                sqlalchemy.UniqueConstraint('connector_keys', 'metric_key', 'location_key', name='pipe_index')
            ),
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

        connector.metadata.create_all()
    return tables

