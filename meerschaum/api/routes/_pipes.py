#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Optional, Dict, Union

from meerschaum.api import (
    fastapi,
    app,
    endpoints,
    get_api_connector,
    pipes,
    get_pipe,
    _get_pipes,
    manager,
    debug,
    no_auth, private,
)
import fastapi
from meerschaum.api.models import MetaPipe
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import is_pipe_registered, round_time
import meerschaum.core.User
import datetime
pipes_endpoint = endpoints['pipes']
from fastapi.responses import StreamingResponse
import io


@app.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/register', tags=['Pipes'])
def register_pipe(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        parameters: dict,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
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
    pipe_object = get_pipe(connector_keys, metric_key, location_key)
    if is_pipe_registered(pipe_object, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe_object} already registered."
        )
    pipe_object.parameters = parameters
    results = get_api_connector().register_pipe(pipe_object, debug=debug)
    pipes(refresh=True)

    return results


@app.delete(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/delete', tags=['Pipes'])
def delete_pipe(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ):
    """
    Delete a Pipe, dropping its table.
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
    pipe_object = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(pipe_object, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe_object} is not registered."
        )
    results = get_api_connector().delete_pipe(pipe_object, debug=debug)
    pipes(refresh=True)

    return results


@app.patch(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/edit', tags=['Pipes'])
def edit_pipe(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        parameters: dict,
        patch: bool = False,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ):
    """
    Edit an existing pipe.
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
    pipe_object = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(pipe_object, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe_object} is not registered."
        )
    pipe_object.parameters = parameters
    results = get_api_connector().edit_pipe(pipe_object, patch=patch, debug=debug)
    pipes(refresh=True)

    return results


@app.get(pipes_endpoint + '/keys', tags=['Pipes'])
async def fetch_pipes_keys(
        connector_keys: str = "[]",
        metric_keys: str = "[]",
        location_keys: str = "[]",
        tags: str = "[]",
        params: str = "{}",
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> list:
    """
    Get a list of tuples of all registered Pipes' keys.
    """
    import json

    keys = get_api_connector().fetch_pipes_keys(
        connector_keys = json.loads(connector_keys),
        metric_keys = json.loads(metric_keys),
        location_keys = json.loads(location_keys),
        tags = json.loads(tags),
        params = json.loads(params),
    )
    return keys


@app.get(pipes_endpoint, tags=['Pipes'])
async def get_pipes(
        connector_keys : str = "",
        metric_keys : str = "",
        location_keys : str = "",
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
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

@app.get(pipes_endpoint + '/{connector_keys}', tags=['Pipes'])
async def get_pipes_by_connector(
        connector_keys: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> dict:
    """
    Get all registered Pipes by connector_keys with metadata, excluding parameters.
    """
    from meerschaum.utils.misc import replace_pipes_in_dict
    if connector_keys not in pipes():
        raise fastapi.HTTPException(
            status_code=404, detail=f"connector_keys '{connector_keys}' not found."
        )
    return replace_pipes_in_dict(pipes()[connector_keys], str)

@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}', tags=['Pipes'])
async def get_pipes_by_connector_and_metric(
        connector_keys: str,
        metric_key: str,
        parent: bool = False,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ):
    """
    Get all registered Pipes by connector_keys and metric_key with metadata, excluding parameters.

    Parameters
    ----------
    parent: bool, default False
        Return the parent Pipe (`location_key` is `None`)
    """
    from meerschaum.utils.misc import replace_pipes_in_dict
    if connector_keys not in pipes():
        raise fastapi.HTTPException(
            status_code=404, detail=f"connector_keys '{connector_keys}' not found."
        )
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if parent:
        return pipes()[connector_keys][metric_key][None]
    return replace_pipes_in_dict(pipes()[connector_keys][metric_key], str)


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}', tags=['Pipes'])
async def get_pipes_by_connector_and_metric_and_location(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ):
    """
    Get a specific Pipe with metadata, excluding parameters.
    """
    if connector_keys not in pipes():
        raise fastapi.HTTPException(
            status_code=404, detail=f"connector_keys '{connector_keys}' not found."
        )
    if metric_key not in pipes()[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"metric_key '{metric_key}' not found.")
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    if location_key not in pipes()[connector_keys][metric_key]:
        raise fastapi.HTTPException(
            status_code=404, detail=f"location_key '{location_key}' not found."
        )

    return str(pipes()[connector_keys][metric_key][location_key])


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/sync_time', tags=['Pipes'])
def get_sync_time(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        params: dict = None,
        newest: bool = True,
        round_down: bool = True,
        debug: bool = False,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> 'datetime.datetime':
    """
    Get a Pipe's latest datetime value.
    See `meerschaum.Pipe.get_sync_time`.
    """
    if location_key == '[None]':
        location_key = None
    pipe = get_pipe(connector_keys, metric_key, location_key)
    if is_pipe_registered(pipe, pipes()):
        return pipe.get_sync_time(params=params, newest=newest, debug=debug)

@app.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data', tags=['Pipes'])
def sync_pipe(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        data: dict = None,
        check_existing: bool = True,
        blocking: bool = True,
        force: bool = False,
        workers: Optional[int] = None,
        columns: Optional[str] = None,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
        debug: bool = False,
    ) -> tuple:
    """
    Add data to an existing Pipe.
    See `meerschaum.Pipe.sync`.
    """
    from meerschaum.utils.misc import parse_df_datetimes
    from meerschaum import Pipe
    import json
    if data is None:
        data = {}
    p = get_pipe(connector_keys, metric_key, location_key)
    if not p.columns and columns is not None:
        p.columns = json.loads(columns)
    if not p.columns and not is_pipe_registered(p, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified."
        )

    df = parse_df_datetimes(data)
    result = list(p.sync(
        df,
        debug = debug,
        check_existing = check_existing,
        blocking = blocking,
        force = force,
        workers = workers
    ))
    return result


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data', tags=['Pipes'])
def get_pipe_data(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        begin: datetime.datetime = None,
        end: datetime.datetime = None,
        params: Optional[str] = None,
        orient: str = 'records',
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> str:
    """
    Get a Pipe's data. Optionally set query boundaries.
    """
    _params = {}
    if params == 'null':
        params = None
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


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/backtrack_data',
    tags=['Pipes']
)
def get_backtrack_data(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        begin: datetime.datetime = None,
        backtrack_minutes: int = 0,
        params: Optional[str] = None,
        orient: str = 'records',
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> fastapi.Response:
    """
    Get a Pipe's backtrack data. Optionally set query boundaries.
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
        raise fastapi.HTTPException(
            status_code = 400,
            detail = f"Cannot fetch backtrackdata for {pipe} without a datetime column.",
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


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/csv', tags=['Pipes'])
def get_pipe_csv(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        begin: datetime.datetime = None,
        end: datetime.datetime = None,
        params: Optional[str] = None,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> str:
    """
    Get a Pipe's data as a CSV file. Optionally set query boundaries.
    """
    _params = {}
    if params == 'null':
        params = None
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
            detail = "Pipe must be registered with the datetime column specified."
        )

    if begin is None:
        begin = p.get_sync_time(round_down=False, newest=False)
    if end is None:
        end = p.get_sync_time(round_down=False, newest=True)

    filename = str(p) + '-' + str(begin.timestamp()) + '-' + str(end.timestamp()) + '.csv'
    df = p.get_data(begin=begin, end=end, params=_params, debug=debug)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type='text/csv')
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/id', tags=['Pipes'])
def get_pipe_id(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> int:
    """
    Get a Pipe's ID.
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


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/attributes',
    tags = ['Pipes']
)
def get_pipe_attributes(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> dict:
    """Get a Pipe's attributes."""
    return get_pipe(connector_keys, metric_key, location_key).attributes


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/exists', tags=['Pipes'])
def get_pipe_exists(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> dict:
    """Determine whether a Pipe exists."""
    return get_pipe(connector_keys, metric_key, location_key).exists()


@app.post(endpoints['metadata'], tags=['Pipes'])
def create_metadata(
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> bool:
    """Create Pipe metadata tables"""
    from meerschaum.connectors.sql.tables import get_tables
    try:
        tables = get_tables(mrsm_instance=get_api_connector(), debug=debug)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))
    return True


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/rowcount', tags=['Pipes'])
def get_pipe_rowcount(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> int:
    """
    Return a pipe's rowcount.
    """
    return get_pipe(connector_keys, metric_key, location_key).get_rowcount(
        begin = begin,
        end = end,
        params = params,
        debug = debug
    )


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/columns/types',
    tags=['Pipes']
)
def get_pipe_columns_types(
        connector_keys : str,
        metric_key : str,
        location_key : str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> dict:
    """
    Returm a dictionary of column names and types.

    ```
    {
        "dt": "TIMESTAMP WITHOUT TIMEZONE",
        "id": "BIGINT",
        "val": "DOUBLE PRECISION",
    }
    ```
    """
    return get_pipe(connector_keys, metric_key, location_key).get_columns_types(debug=debug)
