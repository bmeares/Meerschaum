#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create and access locations
"""

#  from meerschaum.api import app, endpoints, database, connector
#  from meerschaum.api.models import Location
from meerschaum.api import app, debug, manager, get_connector, endpoints
from meerschaum.api.tables import get_tables
#  from meerschaum.utils.misc import attempt_import
#  sqlalchemy = attempt_import('sqlalchemy')

#  @app.post(endpoint)
#  async def register_location(location : Location):
    #  """
    #  Register a new Location
    #  """
    #  query = get_tables()['locations'].insert().values(
        #  location_key = location.location_key,
        #  location_name = location.location_name,
    #  )

    #  last_record_id = await database.execute(query)
    #  return {**location.dict(), "location_id": last_record_id}

#  @app.get('/locations')
#  async def get_locations():
    #  query = get_tables()['pipes'].select()
    #  return await database.fetch_all(query)

