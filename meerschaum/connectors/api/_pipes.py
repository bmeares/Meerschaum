#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register or fetch Pipes from the API
"""

from __future__ import annotations
import time
import json
from io import StringIO
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.typing import SuccessTuple, Union, Any, Optional, Mapping, List, Dict, Tuple

def pipe_r_url(
    pipe: mrsm.Pipe
) -> str:
    """Return a relative URL path from a Pipe's keys."""
    from meerschaum.config.static import STATIC_CONFIG
    location_key = pipe.location_key
    if location_key is None:
        location_key = '[None]'
    return (
        f"{STATIC_CONFIG['api']['endpoints']['pipes']}/"
        + f"{pipe.connector_keys}/{pipe.metric_key}/{location_key}"
    )

def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> SuccessTuple:
    """Submit a POST to the API to register a new Pipe object.
    Returns a tuple of (success_bool, response_dict).
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import STATIC_CONFIG
    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.
    r_url = pipe_r_url(pipe)
    response = self.post(
        r_url + '/register',
        json = pipe.parameters,
        debug = debug,
    )
    if debug:
        dprint(response.text)

    if not response:
        return False, response.text

    response_data = response.json()
    if isinstance(response_data, list):
        response_tuple = response_data[0], response_data[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response_data['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple


def edit_pipe(
    self,
    pipe: mrsm.Pipe,
    patch: bool = False,
    debug: bool = False,
) -> SuccessTuple:
    """Submit a PATCH to the API to edit an existing Pipe object.
    Returns a tuple of (success_bool, response_dict).
    """
    from meerschaum.utils.debug import dprint
    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.
    r_url = pipe_r_url(pipe)
    response = self.patch(
        r_url + '/edit',
        params = {'patch': patch,},
        json = pipe.parameters,
        debug = debug,
    )
    if debug:
        dprint(response.text)

    response_data = response.json()

    if isinstance(response.json(), list):
        response_tuple = response_data[0], response_data[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response_data['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> Union[List[Tuple[str, str, Union[str, None]]]]:
    """
    Fetch registered Pipes' keys from the API.
    
    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        The connector keys for the query.

    metric_keys: Optional[List[str]], default None
        The metric keys for the query.

    location_keys: Optional[List[str]], default None
        The location keys for the query.

    tags: Optional[List[str]], default None
        A list of tags for the query.

    params: Optional[Dict[str, Any]], default None
        A parameters dictionary for filtering against the `pipes` table
        (e.g. `{'connector_keys': 'plugin:foo'}`).
        Not recommeded to be used.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of tuples containing pipes' keys.
    """
    from meerschaum.config.static import STATIC_CONFIG
    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    if tags is None:
        tags = []

    r_url = STATIC_CONFIG['api']['endpoints']['pipes'] + '/keys'
    try:
        j = self.get(
            r_url,
            params = {
                'connector_keys': json.dumps(connector_keys),
                'metric_keys': json.dumps(metric_keys),
                'location_keys': json.dumps(location_keys),
                'tags': json.dumps(tags),
                'params': json.dumps(params),
            },
            debug=debug
        ).json()
    except Exception as e:
        error(str(e))

    if 'detail' in j:
        error(j['detail'], stack=False)
    return [tuple(r) for r in j]


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: Optional[Union['pd.DataFrame', Dict[Any, Any], str]] = None,
    chunksize: Optional[int] = -1,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Sync a DataFrame into a Pipe."""
    from decimal import Decimal
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import json_serialize_datetime, items_str
    from meerschaum.config import get_config
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.dataframe import get_numeric_cols, to_json
    begin = time.time()
    more_itertools = attempt_import('more_itertools')
    if df is None:
        msg = f"DataFrame is `None`. Cannot sync {pipe}."
        return False, msg

    def get_json_str(c):
        ### allow syncing dict or JSON without needing to import pandas (for IOT devices)
        if isinstance(c, (dict, list)):
            return json.dumps(c, default=json_serialize_datetime)
        return to_json(c, orient='columns')

    df = json.loads(df) if isinstance(df, str) else df

    _chunksize: Optional[int] = (1 if chunksize is None else (
        get_config('system', 'connectors', 'sql', 'chunksize') if chunksize == -1
        else chunksize
    ))
    keys: List[str] = list(df.columns)
    chunks = []
    if hasattr(df, 'index'):
        df = df.reset_index(drop=True)
        is_dask = 'dask' in df.__module__
        chunks = (
            (df.iloc[i] for i in more_itertools.chunked(df.index, _chunksize))
            if not is_dask
            else [partition.compute() for partition in df.partitions]
        )

        numeric_cols = get_numeric_cols(df)
        if numeric_cols:
            for col in numeric_cols:
                df[col] = df[col].apply(lambda x: f'{x:f}' if isinstance(x, Decimal) else x)
            pipe_dtypes = pipe.dtypes
            new_numeric_cols = [
                col
                for col in numeric_cols
                if pipe_dtypes.get(col, None) != 'numeric'
            ]
            pipe.dtypes.update({
                col: 'numeric'
                for col in new_numeric_cols
            })
            edit_success, edit_msg = pipe.edit(debug=debug)
            if not edit_success:
                warn(
                    "Failed to update new numeric columns "
                    + f"{items_str(new_numeric_cols)}:\n{edit_msg}"
                )
    elif isinstance(df, dict):
        ### `_chunks` is a dict of lists of dicts.
        ### e.g. {'a' : [ {'a':[1, 2]}, {'a':[3, 4]} ] }
        _chunks = {k: [] for k in keys}
        for k in keys:
            chunk_iter = more_itertools.chunked(df[k], _chunksize)
            for l in chunk_iter:
                _chunks[k].append({k: l})

        ### `chunks` is a list of dicts (e.g. orient by rows in pandas JSON).
        for k, l in _chunks.items():
            for i, c in enumerate(l):
                try:
                    chunks[i].update(c)
                except IndexError:
                    chunks.append(c)
    elif isinstance(df, list):
        chunks = (df[i] for i in more_itertools.chunked(df, _chunksize))

    ### Send columns in case the user has defined them locally.
    if pipe.columns:
        kw['columns'] = json.dumps(pipe.columns)
    r_url = pipe_r_url(pipe) + '/data'

    rowcount = 0
    num_success_chunks = 0
    for i, c in enumerate(chunks):
        if debug:
            dprint(f"[{self}] Posting chunk {i} to {r_url}...")
        if len(c) == 0:
            if debug:
                dprint(f"[{self}] Skipping empty chunk...")
            continue
        json_str = get_json_str(c)

        try:
            response = self.post(
                r_url,
                ### handles check_existing
                params = kw,
                data = json_str,
                debug = debug
            )
        except Exception as e:
            msg = f"Failed to post a chunk to {pipe}:\n{e}"
            warn(msg)
            return False, msg
            
        if not response:
            return False, f"Failed to sync a chunk:\n{response.text}"

        try:
            j = json.loads(response.text)
        except Exception as e:
            return False, f"Failed to parse response from syncing {pipe}:\n{e}"

        if isinstance(j, dict) and 'detail' in j:
            return False, j['detail']

        try:
            j = tuple(j)
        except Exception as e:
            return False, response.text

        if debug:
            dprint("Received response: " + str(j))
        if not j[0]:
            return j

        rowcount += len(c)
        num_success_chunks += 1

    success_tuple = True, (
        f"It took {round(time.time() - begin, 2)} seconds to sync {rowcount} row"
        + ('s' if rowcount != 1 else '')
        + f" across {num_success_chunks} chunk" + ('s' if num_success_chunks != 1 else '') +
        f" to {pipe}."
    )
    return success_tuple


def delete_pipe(
    self,
    pipe: Optional[meerschaum.Pipe] = None,
    debug: bool = None,        
) -> SuccessTuple:
    """Delete a Pipe and drop its table."""
    if pipe is None:
        error(f"Pipe cannot be None.")
    r_url = pipe_r_url(pipe)
    response = self.delete(
        r_url + '/delete',
        debug = debug,
    )
    if debug:
        dprint(response.text)

    response_data = response.json()
    if isinstance(response.json(), list):
        response_tuple = response_data[0], response_data[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response_data['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple


def get_pipe_data(
    self,
    pipe: meerschaum.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[str, datetime, int, None] = None,
    end: Union[str, datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    as_chunks: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union[pandas.DataFrame, None]:
    """Fetch data from the API."""
    r_url = pipe_r_url(pipe)
    chunks_list = []
    while True:
        try:
            response = self.get(
                r_url + "/data",
                params={
                    'select_columns': json.dumps(select_columns),
                    'omit_columns': json.dumps(omit_columns),
                    'begin': begin,
                    'end': end,
                    'params': json.dumps(params, default=str)
                },
                debug=debug
            )
            if not response.ok:
                return None
            j = response.json()
        except Exception as e:
            warn(f"Failed to get data for {pipe}:\n{e}")
            return None
        if isinstance(j, dict) and 'detail' in j:
            return False, j['detail']
        break

    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.dataframe import parse_df_datetimes, add_missing_cols_to_df
    from meerschaum.utils.dtypes import are_dtypes_equal
    pd = import_pandas()
    try:
        df = pd.read_json(StringIO(response.text))
    except Exception as e:
        warn(f"Failed to parse response for {pipe}:\n{e}")
        return None

    if len(df.columns) == 0:
        return add_missing_cols_to_df(df, pipe.dtypes)

    df = parse_df_datetimes(
        df,
        ignore_cols = [
            col
            for col, dtype in pipe.dtypes.items()
            if not are_dtypes_equal(str(dtype), 'datetime')
        ],
        strip_timezone=(pipe.tzinfo is None),
        debug=debug,
    )
    return df


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> int:
    """Get a Pipe's ID from the API."""
    from meerschaum.utils.misc import is_int
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + '/id',
        debug = debug
    )
    if debug:
        dprint(f"Got pipe ID: {response.text}")
    try:
        if is_int(response.text):
            return int(response.text)
    except Exception as e:
        warn(f"Failed to get the ID for {pipe}:\n{e}")
    return None


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, Any]:
    """Get a Pipe's attributes from the API

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe whose attributes we are fetching.
        
    Returns
    -------
    A dictionary of a pipe's attributes.
    If the pipe does not exist, return an empty dictionary.
    """
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/attributes', debug=debug)
    try:
        return json.loads(response.text)
    except Exception as e:
        warn(f"Failed to get the attributes for {pipe}:\n{e}")
    return {}


def get_sync_time(
    self,
    pipe: mrsm.Pipe,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    debug: bool = False,
) -> Union[datetime, int, None]:
    """Get a Pipe's most recent datetime value from the API.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to select from.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the WHERE clause.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    Returns
    -------
    The most recent (or oldest if `newest` is `False`) datetime of a pipe,
    rounded down to the closest minute.
    """
    from meerschaum.utils.misc import is_int
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + '/sync_time',
        json = params,
        params = {'newest': newest, 'debug': debug},
        debug = debug,
    )
    if not response:
        warn(f"Failed to get the sync time for {pipe}:\n" + response.text)
        return None

    j = response.json()
    if j is None:
        dt = None
    else:
        try:
            dt = (
                datetime.fromisoformat(j)
                if not is_int(j)
                else int(j)
            )
        except Exception as e:
            warn(f"Failed to parse the sync time '{j}' for {pipe}:\n{e}")
            dt = None
    return dt


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> bool:
    """Check the API to see if a Pipe exists.

    Parameters
    ----------
    pipe: 'meerschaum.Pipe'
        The pipe which were are querying.
        
    Returns
    -------
    A bool indicating whether a pipe's underlying table exists.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/exists', debug=debug)
    if not response:
        warn(f"Failed to check if {pipe} exists:\n{response.text}")
        return False
    if debug:
        dprint("Received response: " + str(response.text))
    j = response.json()
    if isinstance(j, dict) and 'detail' in j:
        warn(j['detail'])
    return j


def create_metadata(
    self,
    debug: bool = False
) -> bool:
    """Create metadata tables.

    Returns
    -------
    A bool indicating success.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import STATIC_CONFIG
    r_url = STATIC_CONFIG['api']['endpoints']['metadata']
    response = self.post(r_url, debug=debug)
    if debug:
        dprint("Create metadata response: {response.text}")
    try:
        metadata_response = json.loads(response.text)
    except Exception as e:
        warn(f"Failed to create metadata on {self}:\n{e}")
        metadata_response = False
    return False


def get_pipe_rowcount(
    self,
    pipe: mrsm.Pipe,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    debug: bool = False,
) -> int:
    """Get a pipe's row count from the API.

    Parameters
    ----------
    pipe: 'meerschaum.Pipe':
        The pipe whose row count we are counting.
        
    begin: Optional[datetime], default None
        If provided, bound the count by this datetime.

    end: Optional[datetime]
        If provided, bound the count by this datetime.

    params: Optional[Dict[str, Any]], default None
        If provided, bound the count by these parameters.

    remote: bool, default False

    Returns
    -------
    The number of rows in the pipe's table, bound the given parameters.
    If the table does not exist, return 0.
    """
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + "/rowcount",
        json = params,
        params = {
            'begin': begin,
            'end': end,
            'remote': remote,
        },
        debug = debug
    )
    if not response:
        warn(f"Failed to get the rowcount for {pipe}:\n{response.text}")
        return 0
    try:
        return int(json.loads(response.text))
    except Exception as e:
        warn(f"Failed to get the rowcount for {pipe}:\n{e}")
    return 0


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> SuccessTuple:
    """
    Drop a pipe's table but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to be dropped.
        
    Returns
    -------
    A success tuple (bool, str).
    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    if pipe is None:
        error(f"Pipe cannot be None.")
    r_url = pipe_r_url(pipe)
    response = self.delete(
        r_url + '/drop',
        debug = debug,
    )
    if debug:
        dprint(response.text)

    try:
        data = response.json()
    except Exception as e:
        return False, f"Failed to drop {pipe}."

    if isinstance(data, list):
        response_tuple = data[0], data[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), data['detail']
    else:
        response_tuple = response.__bool__(), response.text

    return response_tuple


def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Delete rows in a pipe's table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe with rows to be deleted.
        
    Returns
    -------
    A success tuple.
    """
    kw.pop('metric_keys', None)
    kw.pop('connector_keys', None)
    kw.pop('location_keys', None)
    kw.pop('action', None)
    kw.pop('force', None)
    return self.do_action_legacy(
        ['clear', 'pipes'],
        connector_keys=pipe.connector_keys,
        metric_keys=pipe.metric_key,
        location_keys=pipe.location_key,
        force=True,
        debug=debug,
        **kw
    )


def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union[Dict[str, str], None]:
    """
    Fetch the columns and types of the pipe's table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe whose columns to be queried.

    Returns
    -------
    A dictionary mapping column names to their database types.

    Examples
    --------
    >>> {
    ...   'dt': 'TIMESTAMP WITHOUT TIMEZONE',
    ...   'id': 'BIGINT',
    ...   'val': 'DOUBLE PRECISION',
    ... }
    >>>
    """
    r_url = pipe_r_url(pipe) + '/columns/types'
    response = self.get(
        r_url,
        debug=debug
    )
    j = response.json()
    if isinstance(j, dict) and 'detail' in j and len(j.keys()) == 1:
        warn(j['detail'])
        return None
    if not isinstance(j, dict):
        warn(response.text)
        return None
    return j


def get_pipe_columns_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union[Dict[str, str], None]:
    """
    Fetch the index information for a pipe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose columns to be queried.

    Returns
    -------
    A dictionary mapping column names to a list of associated index information.
    """
    r_url = pipe_r_url(pipe) + '/columns/indices'
    response = self.get(
        r_url,
        debug=debug
    )
    j = response.json()
    if isinstance(j, dict) and 'detail' in j and len(j.keys()) == 1:
        warn(j['detail'])
        return None
    if not isinstance(j, dict):
        warn(response.text)
        return None
    return j
