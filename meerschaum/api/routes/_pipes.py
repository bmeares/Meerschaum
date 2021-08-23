#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Optional, Dict

from meerschaum.api import (
    fastapi,
    app,
    endpoints,
    get_api_connector,
    pipes,
    get_pipe,
    _get_pipes,
    manager,
    debug
)
from meerschaum.api.models import MetaPipe
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import is_pipe_registered, round_time
import meerschaum._internal.User
import datetime
pipes_endpoint = endpoints['pipes']

@app.post(pipes_endpoint)
def register_pipe(
        pipe : MetaPipe,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ):
    """
    Register a new pipe.
    """
    from meerschaum.config import get_config
    allow_pipes = get_config('system', 'api', 'permissions', 'registration', 'pipes', patch=True)
    if not allow_pipes:
        return False, (
            "The administrator for this server has not allowed pipe registration.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'." +
            " Under the keys `api:permissions:registration`, " +
            "you can toggle various registration types."
        )
    pipe_object = get_pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key)
    if is_pipe_registered(pipe_object, pipes(refresh=True)):
        raise fastapi.HTTPException(status_code=409, detail="Pipe already registered")
    pipe_object.parameters = pipe.parameters
    results = get_api_connector().register_pipe(pipe_object)
    pipes(refresh=True)

    return results

@app.patch(pipes_endpoint)
def edit_pipe(
        pipe : MetaPipe,
        patch : bool = False,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ):
    """
    Edit a Pipe's parameters.
    patch : bool : False
        If patch is True, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default)
    """
    from meerschaum.utils.debug import dprint
    pipe_object = get_pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key)
    pipe_object.parameters = pipe.parameters
    if not is_pipe_registered(pipe_object, pipes(refresh=True)):
        raise fastapi.HTTPException(status_code=404, detail="Pipe is not registered.")

    results = get_api_connector().edit_pipe(pipe=pipe_object, patch=patch)

    pipes(refresh=True)
    return results

@app.get(pipes_endpoint + '/keys')
async def fetch_pipes_keys(
        connector_keys : str = "[]",
        metric_keys : str = "[]",
        location_keys : str = "[]",
        params : str = "{}",
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
        debug : bool = False
    ) -> list:
    """
    Get a list of tuples of all registered Pipes' keys.
    """
    import json

    keys = get_api_connector().fetch_pipes_keys(
        connector_keys = json.loads(connector_keys),
        metric_keys = json.loads(metric_keys),
        location_keys = json.loads(location_keys),
        params = json.loads(params),
        debug = debug
    )
    return keys

@app.get(pipes_endpoint)
async def get_pipes(
        connector_keys : str = "",
        metric_keys : str = "",
        location_keys : str = "",
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
        debug : bool = False
    ) -> dict:
    """
    Get all registered Pipes with metadata, excluding parameters.
    """
    from meerschaum.utils.misc import replace_pipes_in_dict
    kw = {'debug' : debug, 'mrsm_instance' : get_api_connector()}
    if connector_keys != "":
        kw['connector_keys'] = connector_keys
    if metric_keys != "":
        kw['metric_keys'] = metric_keys
    if location_keys != "":
        kw['location_keys'] = location_keys
    return replace_pipes_in_dict(_get_pipes(**kw), str)

@app.get(pipes_endpoint + '/{connector_keys}')
async def get_pipes_by_connector(
        connector_keys : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> dict:
    """
    Get all registered Pipes by connector_keys with metadata, excluding parameters.
    """
    from meerschaum.utils.misc import replace_pipes_in_dict
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    return replace_pipes_in_dict(pipes()[connector_keys], str)

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}')
async def get_pipes_by_connector_and_metric(
        connector_keys : str,
        metric_key : str,
        parent : bool = False,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ):
    """
    Get all registered Pipes by connector_keys and metric_key with metadata, excluding parameters.

    parent : bool (default False)
        Return the parent Pipe (location_key is None)
    """
    from meerschaum.utils.misc import replace_pipes_in_dict
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if parent:
        return pipes()[connector_keys][metric_key][None]
    return replace_pipes_in_dict(pipes()[connector_keys][metric_key], str)

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}')
async def get_pipes_by_connector_and_metric_and_location(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ):
    """
    Get a specific Pipe with metadata, excluding parameters.
    """
    if connector_keys not in pipes():
        raise fastapi.HTTPException(status_code=404, detail=f"connector_keys '{connector_keys}' not found.")
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    if location_key not in pipes()[connector_keys][metric_key]:
        raise fastapi.HTTPException(status_code=404, detail=f"location_key '{location_key}' not found.")

    return str(pipes()[connector_keys][metric_key][location_key])

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/sync_time')
def get_sync_time(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        params : dict = None,
        newest: bool = True,
        debug : bool = False,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> 'datetime.datetime':
    """
    Get a Pipe's latest datetime value.
    """
    if location_key == '[None]':
        location_key = None
    pipe = get_pipe(connector_keys, metric_key, location_key)
    if is_pipe_registered(pipe, pipes()):
        return pipe.get_sync_time(params=params, newest=newest, debug=debug)

@app.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data')
def sync_pipe(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        data : dict = {},
        check_existing : bool = True,
        blocking : bool = True,
        force : bool = False,
        workers : Optional[int] = None,
        columns : Optional[str] = None,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
        debug : bool = False,
    ) -> tuple:
    """
    Add data to an existing Pipe.
    """
    from meerschaum.utils.misc import parse_df_datetimes
    from meerschaum import Pipe
    import json
    p = get_pipe(connector_keys, metric_key, location_key)
    if not p.columns and columns is not None:
        p.columns = json.loads(columns)
    if not p.columns and not is_pipe_registered(p, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified."
        )

    df = parse_df_datetimes(data)
    return p.sync(
        df,
        debug = debug,
        check_existing = check_existing,
        blocking = blocking,
        force = force,
        workers = workers
    )

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data')
def get_pipe_data(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        begin : datetime.datetime = None,
        end : datetime.datetime = None,
        params : Optional[str] = None,
        orient : str = 'records',
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> str:
    """
    Get a Pipe's data. Optionally set query boundaries.
    """
    _params = {}
    if params is not None:
        import json
        try:
            _params = json.loads(params)
        except Exception as e:
            _params = None

    if not isinstance(_params, dict):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Params must be a valid JSON-encoded dictionary.",
        )

    p = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(p, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified"
        )

    #  chunks = p.get_data(
        #  begin = begin,
        #  end = end, 
        #  params = params,
        #  as_chunks = True,
        #  debug = debug
    #  )

    return fastapi.Response(
        content = p.get_data(
            begin = begin,
            end = end,
            params = _params, 
            debug = debug
        ).to_json(
            date_format = 'iso',
            orient = orient,
            date_unit = 'us',
        ),
        media_type = 'application/json',
        #  headers = {'chunk' : chunk, 'max_chunk' : max_chunk},
    )
@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/backtrack_data')
def get_backtrack_data(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        begin : datetime.datetime = None,
        backtrack_minutes : int = 0,
        params : Optional[str] = None,
        orient : str = 'records',
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> fastapi.Response:
    """
    Get a Pipe's data. Optionally set query boundaries
    """
    _params = {}
    if params is not None:
        import json
        try:
            _params = json.loads(params)
        except Exception as e:
            _params = None

    pipe = get_pipe(connector_keys, metric_key, location_key)
    if not pipe.get_columns('datetime', error=False):
        raise HTTPException(
            status_code = 400,
            detail = f"Cannot fetch backtrackdata for pipe '{pipe}' without a datetime column.",
        )
    df = pipe.get_backtrack_data(
        begin = begin,
        backtrack_minutes = backtrack_minutes,
        params = _params,
        debug = debug
    )
    if df is None:
        return None
    js = df.to_json(
        date_format = 'iso',
        orient = orient,
        date_unit = 'us'
    )
    return fastapi.Response(
        content = js,
        media_type = 'application/json'
    )

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/id')
def get_pipe_id(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> int:
    """
    Get a Pipe's ID
    """
    try:
        pipe_id = int(
            get_pipe(
                connector_keys,
                metric_key,
                location_key
            ).id
        )
    except Exception as e:
        raise fastapi.HTTPException(status_code=404, detail=str(e))
    return pipe_id

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/attributes')
def get_pipe_attributes(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> dict:
    """
    Get a Pipe's attributes
    """
    return get_pipe(connector_keys, metric_key, location_key).attributes

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/exists')
def get_pipe_exists(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> dict:
    """
    Determine if a Pipe exists or not
    """
    return get_pipe(connector_keys, metric_key, location_key).exists()

@app.post(endpoints['metadata'])
def create_metadata(
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> bool:
    """
    Create Pipe metadata tables
    """
    from meerschaum.connectors.sql.tables import get_tables
    try:
        tables = get_tables(mrsm_instance=get_api_connector(), debug=debug)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))
    return True

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/rowcount')
def get_pipe_rowcount(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        begin : Optional[datetime.datetime] = None,
        end : Optional[datetime.datetime] = None,
        params : Optional[Dict[str, Any]] = None,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> int:
    """
    Return a Pipe's row count
    """
    return get_pipe(connector_keys, metric_key, location_key).get_rowcount(
        begin = begin,
        end = end,
        params = params,
        debug = debug
    )

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/columns/types')
def get_pipe_columns_types(
        connector_keys : str,
        metric_key : str,
        location_key : str,
    ) -> dict:
    """
    Return a dictionary of a pipe's table's columns to their data types.

    E.g. An example dictionary for a small table.

    ```
    >>> {
    ...   'dt': 'TIMESTAMP WITHOUT TIMEZONE',
    ...   'id': 'BIGINT',
    ...   'val': 'DOUBLE PRECISION',
    ... }
    >>> 
    ```
    """
    return get_pipe(connector_keys, metric_key, location_key).get_columns_types(debug=debug)
