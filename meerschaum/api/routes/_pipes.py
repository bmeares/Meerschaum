#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API
"""

from meerschaum.api import fast_api, endpoints, database, connector
from meerschaum.api.models import MetaPipe
from meerschaum.api.tables import get_tables
from meerschaum.utils.misc import attempt_import
sqlalchemy = attempt_import('sqlalchemy')
pipes_endpoint = endpoints['mrsm'] + '/pipes'

@fast_api.post(pipes_endpoint)
async def register_pipe(pipe : MetaPipe):
    """
    Register a new Pipe
    """
    query = get_tables()['pipes'].insert().values(
        connector_keys = pipe.connector_keys,
        metric_key = pipe.metric_key,
        location_key = pipe.location_key
    )

    last_record_id = await database.execute(query)
    return {**pipe.dict(), "pipe_id": last_record_id}

@fast_api.get(pipes_endpoint)
async def get_pipes():
    query = get_tables()['pipes'].select()
    return await database.fetch_all(query)

