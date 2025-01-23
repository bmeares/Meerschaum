#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register Pipes via the Meerschaum API.
"""

from __future__ import annotations


import io
import json
from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Optional, Dict, Union, List
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
    no_auth,
)
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.dataframe import to_json
from meerschaum.utils.misc import (
    is_pipe_registered,
    is_int,
    replace_pipes_in_dict,
)
from meerschaum.connectors.sql.tables import get_tables

fastapi_responses = attempt_import('fastapi.responses', lazy=False)
StreamingResponse = fastapi_responses.StreamingResponse
dateutil_parser = attempt_import('dateutil.parser', lazy=False)
pipes_endpoint = endpoints['pipes']
pd = attempt_import('pandas', lazy=False)

MAX_RESPONSE_ROW_LIMIT: int = mrsm.get_config('system', 'api', 'data', 'max_response_row_limit')


@app.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/register', tags=['Pipes'])
def register_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Register a new pipe.
    """
    allow_pipes = mrsm.get_config('system', 'api', 'permissions', 'registration', 'pipes')
    if not allow_pipes:
        return False, (
            "The administrator for this server has not allowed pipe registration.\n\n"
            "Please contact the system administrator, or if you are running this server, "
            "open the configuration file with `edit config system` and search for 'permissions'."
            " Under the keys `api:permissions:registration`, " +
            "you can toggle various registration types."
        )
    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if is_pipe_registered(pipe, pipes(instance_keys, refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe} already registered."
        )
    if parameters:
        pipe.parameters = parameters
    results = get_api_connector(instance_keys).register_pipe(pipe, debug=debug)
    pipes(instance_keys, refresh=True)
    return results


@app.delete(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/delete', tags=['Pipes'])
def delete_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Delete a Pipe (without dropping its table).
    """
    allow_actions = mrsm.get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_actions:
        return False, (
            "The administrator for this server has not allowed actions.\n\n"
            "Please contact the system administrator, or if you are running this server, "
            "open the configuration file with `edit config system` and search for 'permissions'."
            " Under the keys `api:permissions:actions`, "
            "you can toggle non-admin actions."
        )
    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if not is_pipe_registered(pipe, pipes(instance_keys, refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe} is not registered."
        )
    results = get_api_connector(instance_keys).delete_pipe(pipe, debug=debug)
    pipes(instance_keys, refresh=True)
    return results


@app.delete(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/drop', tags=['Pipes'])
def drop_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Drop a pipes' underlying target table.
    """
    allow_actions = mrsm.get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_actions:
        return False, (
            "The administrator for this server has not allowed actions.\n\n"
            "Please contact the system administrator, or if you are running this server, "
            "open the configuration file with `edit config system` and search for 'permissions'."
            " Under the keys `api:permissions:actions`, " +
            "you can toggle non-admin actions."
        )
    pipe_object = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    results = get_api_connector(instance_keys=instance_keys).drop_pipe(pipe_object, debug=debug)
    pipes(instance_keys, refresh=True)
    return results



@app.patch(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/edit', tags=['Pipes'])
def edit_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    parameters: dict,
    instance_keys: Optional[str] = None,
    patch: bool = False,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Edit an existing pipe's parameters.
    """
    allow_actions = mrsm.get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_actions:
        return False, (
            "The administrator for this server has not allowed actions.\n\n"
            "Please contact the system administrator, or if you are running this server, "
            "open the configuration file with `edit config system` and search for 'permissions'."
            " Under the keys `api:permissions:actions`, "
            "you can toggle non-admin actions."
        )
    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if not is_pipe_registered(pipe, pipes(instance_keys, refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe} is not registered."
        )
    pipe.parameters = parameters
    results = get_api_connector(instance_keys).edit_pipe(pipe, patch=patch, debug=debug)
    pipes(instance_keys, refresh=True)
    return results


@app.get(pipes_endpoint + '/keys', tags=['Pipes'])
async def fetch_pipes_keys(
    connector_keys: str = "[]",
    metric_keys: str = "[]",
    location_keys: str = "[]",
    instance_keys: Optional[str] = None,
    tags: str = "[]",
    params: str = "{}",
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Get a list of tuples of all registered pipes' keys.
    """
    keys = get_api_connector(instance_keys).fetch_pipes_keys(
        connector_keys=json.loads(connector_keys),
        metric_keys=json.loads(metric_keys),
        location_keys=json.loads(location_keys),
        tags=json.loads(tags),
        params=json.loads(params),
    )
    return keys


@app.get(pipes_endpoint, tags=['Pipes'])
async def get_pipes(
    connector_keys: str = "",
    metric_keys: str = "",
    location_keys: str = "",
    instance_keys: Optional[str] = None,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Get all registered Pipes with metadata, excluding parameters.
    """
    kw = {'debug': debug, 'mrsm_instance': get_api_connector(instance_keys)}
    if connector_keys != "":
        kw['connector_keys'] = connector_keys
    if metric_keys != "":
        kw['metric_keys'] = metric_keys
    if location_keys != "":
        kw['location_keys'] = location_keys
    return replace_pipes_in_dict(_get_pipes(**kw), lambda p: p.attributes)


@app.get(pipes_endpoint + '/{connector_keys}', tags=['Pipes'])
async def get_pipes_by_connector(
    connector_keys: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, Any]:
    """
    Get all registered Pipes by connector_keys with metadata, excluding parameters.
    """
    if connector_keys not in pipes(instance_keys):
        raise fastapi.HTTPException(
            status_code=404, detail=f"Connector '{connector_keys}' not found."
        )
    return replace_pipes_in_dict(pipes(instance_keys)[connector_keys], lambda p: p.attributes)


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}', tags=['Pipes'])
async def get_pipes_by_connector_and_metric(
    connector_keys: str,
    metric_key: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Get all registered Pipes by connector_keys and metric_key with metadata, excluding parameters.
    """
    if connector_keys not in pipes(instance_keys):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Connector '{connector_keys}' not found.",
        )
    if metric_key not in pipes(instance_keys)[connector_keys]:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Metric '{metric_key}' not found.",
        )
    return replace_pipes_in_dict(
        pipes(instance_keys)[connector_keys][metric_key],
        lambda p: p.attributes
    )


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}', tags=['Pipes'])
async def get_pipes_by_connector_and_metric_and_location(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Get a specific Pipe with metadata, excluding parameters.
    """
    if connector_keys not in pipes(instance_keys):
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Connector '{connector_keys}' not found.",
        )
    if metric_key not in pipes(instance_keys)[connector_keys]:
        raise fastapi.HTTPException(status_code=404, detail=f"Metric '{metric_key}' not found.")
    if location_key in ('[None]', 'None', 'null'):
        location_key = None
    if location_key not in pipes(instance_keys)[connector_keys][metric_key]:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"location_key '{location_key}' not found."
        )

    return str(pipes(instance_keys)[connector_keys][metric_key][location_key])


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/sync_time', tags=['Pipes'])
def get_sync_time(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    remote: bool = False,
    round_down: bool = True,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Union[str, int, None]:
    """
    Get a Pipe's latest datetime value.
    See [`meerschaum.Pipe.get_sync_time`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_sync_time).
    """
    if location_key == '[None]':
        location_key = None
    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    sync_time = pipe.get_sync_time(
        params=params,
        newest=newest,
        round_down=round_down,
    )
    if isinstance(sync_time, datetime):
        sync_time = sync_time.isoformat()
    return sync_time


@app.post(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data', tags=['Pipes'])
def sync_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    data: Union[List[Dict[Any, Any]], Dict[Any, Any]],
    instance_keys: Optional[str] = None,
    check_existing: bool = True,
    blocking: bool = True,
    force: bool = False,
    workers: Optional[int] = None,
    columns: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
    debug: bool = False,
) -> List[Union[bool, str]]:
    """
    Add data to an existing Pipe.
    See [`meerschaum.Pipe.sync`](https://docs.meerschaum.io/meerschaum.html#Pipe.sync).
    """
    if not data:
        return [True, "No data to sync."]
    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if pipe.target in ('mrsm_users', 'mrsm_plugins', 'mrsm_pipes'):
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Cannot sync data to protected table '{pipe.target}'.",
        )

    if not pipe.columns and columns is not None:
        pipe.columns = json.loads(columns)

    success, msg = pipe.sync(
        data,
        debug=debug,
        check_existing=check_existing,
        blocking=blocking,
        force=force,
        workers=workers,
    )
    return list((success, msg))


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/data', tags=['Pipes'])
def get_pipe_data(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    select_columns: Optional[str] = None,
    omit_columns: Optional[str] = None,
    begin: Union[str, int, None] = None,
    end: Union[str, int, None] = None,
    params: Optional[str] = None,
    limit: int = MAX_RESPONSE_ROW_LIMIT,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> str:
    """
    Get a pipe's data, applying any filtering.
    See [`Pipe.get_data()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_data).

    Note that `select_columns`, `omit_columns`, and `params` are JSON-encoded strings.
    """
    if is_int(begin):
        begin = int(begin)
    if is_int(end):
        end = int(end)
    if limit > MAX_RESPONSE_ROW_LIMIT:
        raise fastapi.HTTPException(
            status_code=413,
            detail=(
                f"Requested limit {limit} exceeds the maximum response size of "
                f"{MAX_RESPONSE_ROW_LIMIT} rows."
            )
        )

    _params = {}
    if params == 'null':
        params = None
    if params is not None:
        try:
            _params = json.loads(params)
        except Exception:
            _params = None
    if not isinstance(_params, dict):
        raise fastapi.HTTPException(
            status_code=409,
            detail="Params must be a valid JSON-encoded dictionary.",
        )

    _select_columns = []
    if select_columns == 'null':
        select_columns = None
    if select_columns is not None:
        try:
            _select_columns = json.loads(select_columns)
        except Exception:
            _select_columns = None
    if not isinstance(_select_columns, list):
        raise fastapi.HTTPException(
            status_code=409,
            detail="Selected columns must be a JSON-encoded list."
        )

    _omit_columns = []
    if omit_columns == 'null':
        omit_columns = None
    if omit_columns is not None:
        try:
            _omit_columns = json.loads(omit_columns)
        except Exception:
            _omit_columns = None
    if _omit_columns is None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="Omitted columns must be a JSON-encoded list.",
        )

    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if not is_pipe_registered(pipe, pipes(instance_keys, refresh=True)):
        raise fastapi.HTTPException(
            status_code=409,
            detail="Pipe must be registered with the datetime column specified."
        )

    if pipe.target in ('users', 'plugins', 'pipes'):
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Cannot retrieve data from protected table '{pipe.target}'.",
        )

    df = pipe.get_data(
        select_columns=_select_columns,
        omit_columns=_omit_columns,
        begin=begin,
        end=end,
        params=_params,
        limit=min(limit, MAX_RESPONSE_ROW_LIMIT),
        debug=debug,
    )
    if df is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Could not fetch data with the given parameters.",
        )

    json_content = to_json(df)
    return fastapi.Response(
        json_content,
        media_type='application/json',
    )


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/csv', tags=['Pipes'])
def get_pipe_csv(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    begin: Union[str, int, None] = None,
    end: Union[str, int, None] = None,
    params: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> str:
    """
    Get a pipe's data as a CSV file. Optionally set query boundaries.
    """
    if begin is not None:
        begin = (
            int(begin)
            if is_int(begin)
            else dateutil_parser.parse(begin)
        )
    if end is not None:
        end = (
            int(end)
            if is_int(end)
            else dateutil_parser.parse(end)
        )

    _params = {}
    if params == 'null':
        params = None
    if params is not None:
        import json
        try:
            _params = json.loads(params)
        except Exception:
            _params = None

    if not isinstance(_params, dict):
        raise fastapi.HTTPException(
            status_code=409,
            detail="Params must be a valid JSON-encoded dictionary.",
        )

    pipe = get_pipe(connector_keys, metric_key, location_key, instance_keys)
    if not is_pipe_registered(pipe, pipes(instance_keys, refresh=True)):
        raise fastapi.HTTPException(
            status_code=409,
            detail="Pipe must be registered."
        )

    dt_col = pipe.columns.get('datetime', None)
    if dt_col:
        if begin is None:
            begin = pipe.get_sync_time(round_down=False, newest=False)
        if end is None:
            end = pipe.get_sync_time(round_down=False, newest=True)
            if end is not None:
                end += (
                    1
                    if is_int(str(end))
                    else timedelta(minutes=1)
                )

    bounds_text = (
        ('-' + str(begin) + '-' + str(end))
        if begin is not None and end is not None
        else ''
    )
    filename = pipe.target + bounds_text + '.csv'
    df = pipe.get_data(begin=begin, end=end, params=_params, debug=debug)
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
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Union[int, str]:
    """
    Get a pipe's ID.
    """
    pipe_id = get_pipe(connector_keys, metric_key, location_key, instance_keys).get_id(debug=debug)
    if pipe_id is None:
        raise fastapi.HTTPException(status_code=404, detail="Pipe is not registered.")
    return pipe_id


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/attributes',
    tags=['Pipes']
)
def get_pipe_attributes(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, Any]:
    """Get a pipe's attributes."""
    return get_pipe(
        connector_keys,
        metric_key,
        location_key,
        instance_keys,
        refresh=True,
    ).attributes


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/exists', tags=['Pipes'])
def get_pipe_exists(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> bool:
    """Determine whether a pipe's target table exists."""
    return get_pipe(connector_keys, metric_key, location_key, instance_keys).exists(debug=debug)


@app.post(endpoints['metadata'], tags=['Pipes'])
def create_metadata(
    instance_keys: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> bool:
    """Create pipe instance metadata tables."""
    conn = get_api_connector(instance_keys)
    if conn.type not in ('sql', 'api'):
        return False
    try:
        _ = get_tables(mrsm_instance=conn, debug=debug)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))
    return True


@app.get(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/rowcount', tags=['Pipes'])
def get_pipe_rowcount(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    begin: Union[str, int, None] = None,
    end: Union[str, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> int:
    """
    Return a pipe's rowcount.
    See [`Pipe.get_rowcount()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_rowcount).

    Parameters
    ----------
    begin: Union[str, int, None], default None
        If provided, only count rows newer than or equal to `begin`.
        
    end: Union[str, int, None], defaut None
        If provided, only count rows older than `end`.

    params: Optional[Dict[str, Any]], default None
        If provided, only count rows which match the provided `params` dictionary.

    remote: bool, default False
        If `True`, return the rowcount for the fetch definition instead of the target table.

    Returns
    -------
    The rowcount for a pipe's target table or fetch definition (if applicable).
    """
    if is_int(begin):
        begin = int(begin)
    if is_int(end):
        end = int(end)
    return get_pipe(connector_keys, metric_key, location_key, instance_keys).get_rowcount(
        begin=begin,
        end=end,
        params=params,
        remote=remote,
        debug=debug,
    )


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/columns/types',
    tags=['Pipes']
)
def get_pipe_columns_types(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, str]:
    """
    Return a dictionary of column names and types.
    See [`Pipe.dtypes`](https://meerschaum.io/reference/pipes/parameters/#dtypes) for supported types.

    ```json
    {
        "dt": "datetime",
        "id": "int",
        "val": "float"
    }
    ```
    """
    return get_pipe(connector_keys, metric_key, location_key, instance_keys).dtypes


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/columns/indices',
    tags=['Pipes']
)
def get_pipe_columns_indices(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dictionary of column names and related indices.
    See [`Pipe.get_columns_indices()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_columns_indices).

    ```json
    {
        "datetime": [
            {
                "name": "plugin_stress_test_0_datetime_idx",
                "type": "INDEX"
            },
            {
                "name": "IX_plugin_stress_test_0_id_datetime",
                "type": "INDEX"
            },
            {
                "name": "UQ_plugin_stress_test_0_id_datetime",
                "type": "INDEX"
            }
        ],
        "id": [
            {
                "name": "IX_plugin_stress_test_0_id",
                "type": "INDEX"
            },
            {
                "name": "UQ_plugin_stress_test_0_id_datetime",
                "type": "INDEX"
            }
        ]
    }
    ```
    """
    return get_pipe(
        connector_keys,
        metric_key,
        location_key,
        instance_keys,
    ).get_columns_indices(debug=debug)


@app.get(
    pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/indices/names',
    tags=['Pipes']
)
def get_pipe_index_names(
    connector_keys: str,
    metric_key: str,
    location_key: str,
    instance_keys: Optional[str] = None,
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dictionary of index keys and index names.

    See [`Pipe.get_indices()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_indices).
    """
    return get_pipe(
        connector_keys,
        metric_key,
        location_key,
        instance_keys,
    ).get_indices(debug=debug)
