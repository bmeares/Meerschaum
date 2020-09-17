#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define API SQLAlchemy tables
"""

from meerschaum.api import connector
from meerschaum.utils.misc import attempt_import
sqlalchemy = attempt_import('sqlalchemy')

tables = {
    'pipes' : sqlalchemy.Table(
                  "pipes",
                  connector.metadata,
                  sqlalchemy.Column("pipe_id", sqlalchemy.Integer, primary_key=True),
                  sqlalchemy.Column("building_key", sqlalchemy.String),
                  sqlalchemy.Column("metric", sqlalchemy.String),
              ),
}

connector.metadata.create_all()

