#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes
"""

from meerschaum.api import fast_api, endpoints, database, connector
from meerschaum.api.models import Pipe, PipeIn
from meerschaum.api.tables import tables
import sqlalchemy
pipes_endpoint = endpoints['mrsm'] + '/pipes'

@fast_api.post(pipes_endpoint)
async def register_pipe(pipe : PipeIn):
    """
    Register a new Pipe
    """
    query = tables['pipes'].insert().values(
            building_key = pipe.building_key,
            metric = pipe.metric
    )

    last_record_id = await database.execute(query)
    return {**pipe.dict(), "pipe_id": last_record_id}

@fast_api.get(pipes_endpoint)
async def get_pipes():
    query = tables['pipes'].select()
    return await database.fetch_all(query)

