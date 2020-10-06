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
        location_key = pipe.location_key,
        parameters = pipe.parameters,
    )
    pipes = get_pipes_sql(debug=True)

    last_record_id = await database.execute(query)
    return {**pipe.dict(), "pipe_id": last_record_id}

@fast_api.patch(pipes_endpoint)
async def edit_pipe(pipe : MetaPipe, patch : bool = False):
    """
    Edit a Pipe's parameters.
    patch : bool : False
        If patch is True, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default)
    """
    from meerschaum.utils.debug import dprint
    global pipes
    pipes = get_pipes_sql(debug=True)
    if not is_pipe_registered(pipe, pipes):
        raise fastapi.HTTPException(status_code=404, detail="Pipe is not registered.")

    if not patch:
        parameters = pipe.parameters
    else:
        from meerschaum.config._patch import apply_patch_to_config
        parameters = apply_patch_to_config(
            pipes[pipe.connector_keys][pipe.metric_key][pipe.location_key].parameters,
            pipe.parameters
        )

    q = f"""
    UPDATE pipes
    SET parameters = '{str(pipe.parameters).replace("'", '"')}'
    WHERE connector_keys = '{pipe.connector_keys}'
        AND metric_key = '{pipe.metric_key}'
        AND location_key """ + ("IS NULL" if pipe.location_key is None else f"= '{pipe.location_key}'")
    return_code = connector.exec(q)
    pipes = get_pipes_sql(debug=True)
    dprint(str(return_code))
    return pipes[pipe.connector_keys][pipe.metric_key][pipe.location_key]

@fast_api.get(pipes_endpoint)
async def get_pipes() -> dict:
    """
    Get all registered Pipes with metadata, excluding parameters.
    """
    return pipes

@fast_api.get(pipes_endpoint + '/{connector_keys}')
async def get_pipes_by_connector(
        connector_keys : str
    ) -> dict:
    """
    Get all registered Pipes by connector_keys with metadata, excluding parameters.
    """
    if connector_keys not in pipes:
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    return pipes[connector_keys]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}')
async def get_pipes_by_connector_and_metric(
        connector_keys : str,
        metric_key : str,
        parent : bool = False
    ):
    """
    Get all registered Pipes by connector_keys and metric_key with metadata, excluding parameters.

    parent : bool (default False)
        Return the parent Pipe (location_key is None)
    """
    if connector_keys not in pipes:
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if parent: return pipes[connector_keys][metric_key][None]
    return pipes[connector_keys][metric_key]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}')
async def get_pipes_by_connector_and_metric_and_location(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ):
    """
    Get a specific Pipe with metadata, excluding parameters.
    """
    if connector_keys not in pipes:
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if location_key not in pipes[connector_keys][metric_key]:
        raise fastapi.HTTPException(status_code=404, detail=f"location_key '{location_key}' not found.")
 
    return pipes[connector_keys][metric_key][location_key]

