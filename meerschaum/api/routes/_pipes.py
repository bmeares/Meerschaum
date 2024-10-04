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
import json
import fastapi
from decimal import Decimal
from meerschaum import Pipe
from meerschaum.api.models import MetaPipe
from meerschaum.utils.packages import attempt_import, import_pandas
from meerschaum.utils.dataframe import get_numeric_cols, to_json
from meerschaum.utils.misc import (
    is_pipe_registered, round_time, is_int, parse_df_datetimes,
    replace_pipes_in_dict,
)
from meerschaum.utils.typing import List, Dict, Any, Union
import meerschaum.core.User
import datetime
pipes_endpoint = endpoints['pipes']
from fastapi.responses import StreamingResponse
import io
dateutil_parser = attempt_import('dateutil.parser', lazy=False)
pd = attempt_import('pandas')


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
    Delete a Pipe (without dropping its table).
    """
    from meerschaum.config import get_config
    allow_actions = get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_actions:
        return False, (
            "The administrator for this server has not allowed actions.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'." +
            " Under the keys `api:permissions:actions`, " +
            "you can toggle non-admin actions."
        )
    pipe = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(pipe, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code=409, detail=f"{pipe} is not registered."
        )
    results = get_api_connector().delete_pipe(pipe, debug=debug)
    pipes(refresh=True)

    return results


@app.delete(pipes_endpoint + '/{connector_keys}/{metric_key}/{location_key}/drop', tags=['Pipes'])
def drop_pipe(
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ):
    """
    Dropping a pipes' table (without deleting its registration).
    """
    from meerschaum.config import get_config
    allow_actions = get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_actions:
        return False, (
            "The administrator for this server has not allowed actions.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'." +
            " Under the keys `api:permissions:actions`, " +
            "you can toggle non-admin actions."
        )
    pipe_object = get_pipe(connector_keys, metric_key, location_key)
    results = get_api_connector().drop_pipe(pipe_object, debug=debug)
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
    allow_pipes = get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_pipes:
        return False, (
            "The administrator for this server has not allowed actions.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'." +
            " Under the keys `api:permissions:actions`, " +
            "you can toggle non-admin actions."
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
    ):
    """
    Get a list of tuples of all registered Pipes' keys.
    """
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
        connector_keys: str = "",
        metric_keys: str = "",
        location_keys: str = "",
        curr_user=(
            fastapi.Depends(manager) if not no_auth else None
        ),
        debug: bool = False,
    ) -> Dict[str, Any]:
    """
    Get all registered Pipes with metadata, excluding parameters.
    """
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
    ) -> Dict[str, Any]:
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
        params: Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug: bool = False,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> Union[str, int, None]:
    """
    Get a Pipe's latest datetime value.
    See `meerschaum.Pipe.get_sync_time`.
    """
    if location_key == '[None]':
        location_key = None
    pipe = get_pipe(connector_keys, metric_key, location_key)
    sync_time = pipe.get_sync_time(
        params = params,
        newest = newest,
        debug = debug,
        round_down = round_down,
    )
    if isinstance(sync_time, datetime.datetime):
        sync_time = sync_time.isoformat()
    return sync_time


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
    ) -> List[Union[bool, str]]:
    """
    Add data to an existing Pipe.
    See `meerschaum.Pipe.sync`.
    """
    if data is None:
        data = {}
    p = get_pipe(connector_keys, metric_key, location_key)
    if p.target in ('users', 'plugins', 'pipes'):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = f"Cannot sync data to protected table '{p.target}'.",
        )

    if not p.columns and columns is not None:
        p.columns = json.loads(columns)
    if not p.columns and not is_pipe_registered(p, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified."
        )

    result = list(p.sync(
        data,
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
    select_columns: Optional[str] = None,
    omit_columns: Optional[str] = None,
    begin: Union[str, int, None] = None,
    end: Union[str, int, None] = None,
    params: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> str:
    """
    Get a pipe's data, applying any filtering.

    Note that `select_columns`, `omit_columns`, and `params` are JSON-encoded strings.
    """
    if is_int(begin):
        begin = int(begin)
    if is_int(end):
        end = int(end)

    _params = {}
    if params == 'null':
        params = None
    if params is not None:
        try:
            _params = json.loads(params)
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
            _omit_columns = None
    if _omit_columns is None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="Omitted columns must be a JSON-encoded list.",
        )

    pipe = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(pipe, pipes(refresh=True)):
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
        debug=debug,
    )
    if df is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Could not fetch data with the given parameters.",
        )

    ### NaN cannot be JSON-serialized.
    df = df.fillna(pd.NA)
    numeric_cols = get_numeric_cols(df)
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: f'{x:f}' if isinstance(x, Decimal) else x)

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
    begin: Union[str, int, None] = None,
    end: Union[str, int, None] = None,
    params: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> str:
    """
    Get a Pipe's data as a CSV file. Optionally set query boundaries.
    """
    if begin is not None:
        begin = (
            int(begin) if is_int(begin)
            else dateutil_parser.parse(begin)
        )
    if end is not None:
        end = (
            int(end) if is_int(end)
            else dateutil_parser.parse(end)
        )

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
            status_code=409,
            detail="Params must be a valid JSON-encoded dictionary.",
        )

    p = get_pipe(connector_keys, metric_key, location_key)
    if not is_pipe_registered(p, pipes(refresh=True)):
        raise fastapi.HTTPException(
            status_code = 409,
            detail = "Pipe must be registered with the datetime column specified."
        )

    dt_col = p.columns.get('datetime', None)
    if dt_col:
        if begin is None:
            begin = p.get_sync_time(round_down=False, newest=False)
        if end is None:
            end = p.get_sync_time(round_down=False, newest=True)

    bounds_text = (
        ('-' + str(begin) + '-' + str(end))
        if begin is not None and end is not None
        else ''
    )
    filename = p.target + bounds_text + '.csv'
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
    pipe_id = get_pipe(connector_keys, metric_key, location_key).id
    if pipe_id is None:
        raise fastapi.HTTPException(status_code=404, detail="Pipe is not registered.")
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
    ) -> Dict[str, Any]:
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
    ) -> bool:
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
        begin: Union[str, int, None] = None,
        end: Union[str, int, None] = None,
        params: Optional[Dict[str, Any]] = None,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> int:
    """
    Return a pipe's rowcount.
    """
    if is_int(begin):
        begin = int(begin)
    if is_int(end):
        end = int(end)
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
        connector_keys: str,
        metric_key: str,
        location_key: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> Dict[str, str]:
    """
    Return a dictionary of column names and types.

    ```
    {
        "dt": "datetime",
        "id": "int",
        "val": "float",
    }
    ```
    """
    return get_pipe(connector_keys, metric_key, location_key).dtypes
