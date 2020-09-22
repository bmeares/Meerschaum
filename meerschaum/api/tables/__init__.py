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
                          sqlalchemy.Column("location_key", sqlalchemy.String),
                          sqlalchemy.Column("metric_key", sqlalchemy.String),
                          sqlalchemy.Column("connector_keys", sqlalchemy.String)
                      ),
        }

        connector.metadata.create_all()
    return tables

