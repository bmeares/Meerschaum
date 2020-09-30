#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API
"""

from meerschaum.api import fastapi, fast_api, endpoints, database, connector, pipes, get_pipes_sql
from meerschaum.api.models import MetaPipe
from meerschaum.api.tables import get_tables
from meerschaum.utils.misc import attempt_import, is_pipe_registered
sqlalchemy = attempt_import('sqlalchemy')
pipes_endpoint = endpoints['mrsm'] + '/pipes'

@fast_api.post(pipes_endpoint)
async def register_pipe(pipe : MetaPipe):
    """
    Register a new Pipe
    """
    global pipes
    pipes = get_pipes_sql(debug=True)
    if is_pipe_registered(pipe, pipes):
        raise fastapi.HTTPException(status_code=409, detail="Pipe already registered")
    query = get_tables()['pipes'].insert().values(
        connector_keys = pipe.connector_keys,
        metric_key = pipe.metric_key,
        location_key = pipe.location_key
    )

    last_record_id = await database.execute(query)
    return {**pipe.dict(), "pipe_id": last_record_id}

@fast_api.get(pipes_endpoint)
async def get_pipes() -> dict:
    return pipes

@fast_api.get(pipes_endpoint + '/{connector_keys}')
async def get_pipes_by_connector(
        connector_keys : str
    ) -> dict:
    return pipes[connector_keys]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}')
async def get_pipes_by_connector_and_metric(
        connector_keys : str,
        metric_key : str
    ):
    return pipes[connector_keys][metric_key]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}')
async def get_pipes_by_connector_and_metric_and_location(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ):
    return pipes[connector_keys][metric_key][location_key]

