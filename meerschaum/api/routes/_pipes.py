#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API
"""

from meerschaum.api import fastapi, fast_api, endpoints, database, connector, pipes, get_pipe, get_pipes_sql
from meerschaum.api.models import MetaPipe
from meerschaum.api.tables import get_tables
from meerschaum.utils.misc import attempt_import, is_pipe_registered, round_time
import datetime
sqlalchemy = attempt_import('sqlalchemy')
pipes_endpoint = endpoints['mrsm'] + '/pipes'

@fast_api.post(pipes_endpoint)
def register_pipe(pipe : MetaPipe):
    """
    Register a new Pipe
    """
    pipe_object = get_pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key)
    pipe_object.meta = pipe
    if is_pipe_registered(pipe_object, pipes(refresh=True)):
        print('memes')
        raise fastapi.HTTPException(status_code=409, detail="Pipe already registered")
    results = connector.register_pipe(pipe_object)
    pipes(refresh=True)

    return results

@fast_api.patch(pipes_endpoint)
async def edit_pipe(pipe : MetaPipe, patch : bool = False):
    """
    Edit a Pipe's parameters.
    patch : bool : False
        If patch is True, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default)
    """
    from meerschaum.utils.debug import dprint
    pipes(refresh=True)
    if not is_pipe_registered(pipe, pipes()):
        raise fastapi.HTTPException(status_code=404, detail="Pipe is not registered.")
    
    results = connector.edit_pipe(pipe=pipe, patch=patch)

    pipes(refresh=True)
    return results

@fast_api.get(pipes_endpoint + '/keys')
async def fetch_pipes_keys(
        connector_keys : str = "",
        metric_keys : str = "",
        location_keys : str = "",
        params : str = "",
        debug : bool = False
    ) -> list:
    """
    Get a list of tuples of all registered Pipes' keys.
    """
    from meerschaum.utils.misc import string_to_dict
    from meerschaum.utils.debug import dprint
    import json

    if debug: dprint(f"location_keys: {len(location_keys)}")

    return connector.fetch_pipes_keys(
        connector_keys = json.loads(connector_keys),
        metric_keys = json.loads(metric_keys),
        location_keys = json.loads(location_keys),
        params = json.loads(params),
        debug = debug
    )

@fast_api.get(pipes_endpoint)
async def get_pipes(
        connector_keys : str = "",
        metric_keys : str = "",
        location_keys : str = "",
        debug : bool = False
    ) -> dict:
    """
    Get all registered Pipes with metadata, excluding parameters.
    """
    if connector_keys == "" and metric_keys == "" and location_keys == "":
        return pipes()

    import json

    return get_pipes_sql(
        connector_keys = json.loads(connector_keys),
        metric_keys = json.loads(metric_keys),
        location_keys = json.loads(location_keys),
        mrsm_instance = 'sql',
        debug = debug
    )

@fast_api.get(pipes_endpoint + '/{connector_keys}')
async def get_pipes_by_connector(
        connector_keys : str
    ) -> dict:
    """
    Get all registered Pipes by connector_keys with metadata, excluding parameters.
    """
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    return pipes()[connector_keys]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}')
async def get_pipes_by_connector_and_metric(
        connector_keys : str,
        metric_key : str,
        parent : bool = False,
    ):
    """
    Get all registered Pipes by connector_keys and metric_key with metadata, excluding parameters.

    parent : bool (default False)
        Return the parent Pipe (location_key is None)
    """
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if parent: return pipes()[connector_keys][metric_key][None]
    return pipes()[connector_keys][metric_key]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}')
async def get_pipes_by_connector_and_metric_and_location(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ):
    """
    Get a specific Pipe with metadata, excluding parameters.
    """
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if location_key == '[None]': location_key = None
    if location_key not in pipes()[connector_keys][metric_key]:
        raise fastapi.HTTPException(status_code=404, detail=f"location_key '{location_key}' not found.")
 
    return pipes()[connector_keys][metric_key][location_key]

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/sync_time')
async def get_sync_time(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ) -> 'datetime.datetime':
    """
    Get a Pipe's latest datetime value.
    """
    if location_key == '[None]': location_key = None
    if is_pipe_registered(pipe, pipes()):
        return pipe.sync_time

@fast_api.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data')
async def sync_pipe(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        data : dict = {},
    ) -> bool:
    """
    Add data to an existing Pipe.
    """
    from meerschaum.utils.misc import parse_df_datetimes
    from meerschaum import Pipe
    import json
    df = parse_df_datetimes(data)
    p = Pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(p, pipes()):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified"
        )

    return p.sync(df, debug=True)

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data')
def get_pipe_data(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        begin : datetime.datetime = None,
        end : datetime.datetime = None,
        orient : str = 'columns'
    ) -> bool:
    """
    Get a Pipe's data. Optionally set query boundaries
    """
    return fastapi.Response(
        content = get_pipe(
            connector_keys,
            metric_key,
            location_key
        ).get_data(
            begin = begin,
            end = end,
            debug = True
        ).to_json(
            date_format = 'iso',
            orient = orient
        ),
        media_type = 'application/json'
    )
@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/backtrack_data')
def get_backtrack_data(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        begin : datetime.datetime = None,
        backtrack_minutes : int = 0,
        orient : str = 'columns'
    ) -> bool:
    """
    Get a Pipe's data. Optionally set query boundaries
    """
    return fastapi.Response(
        content = get_pipe(
            connector_keys,
            metric_key,
            location_key
        ).get_backtrack_data(
            begin = begin,
            backtrack_minutes = backtrack_minutes,
            debug = True
        ).to_json(
            date_format = 'iso',
            orient = orient
        ),
        media_type = 'application/json'
    )

@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/id')
def get_pipe_id(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ) -> int:
    """
    Get a Pipe's ID
    """
    return int(get_pipe(
        connector_keys,
        metric_key,
        location_key
    ).id)


@fast_api.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/attributes')
def get_pipe_attributes(
        connector_keys : str,
        metric_key : str,
        location_key : str
    ) -> dict:
    """
    Get a Pipe's attributes
    """
    return get_pipe(connector_keys, metric_key, location_key).attributes
