#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register or fetch Pipes from the API
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Union, Any, Optional, Mapping, List, Dict, Tuple

def pipe_r_url(
        pipe : 'meerschaum.Pipe'
    ) -> str:
    """Generate a relative URL path from a Pipe's keys.

    Parameters
    ----------
    pipe : 'meerschaum.Pipe' :
        

    Returns
    -------

    """
    from meerschaum.config.static import _static_config
    location_key = pipe.location_key
    if location_key is None:
        location_key = '[None]'
    return (
        f"{_static_config()['api']['endpoints']['pipes']}/"
        + f"{pipe.connector_keys}/{pipe.metric_key}/{location_key}"
    )

def register_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False
    ) -> SuccessTuple:
    """Submit a POST to the API to register a new Pipe object.
    Returns a tuple of (success_bool, response_dict).

    Parameters
    ----------
    pipe: meerschaum.Pipe :
        
    debug: bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import _static_config
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
    if isinstance(response.json(), list):
        response_tuple = response.__bool__(), response.json()[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response.json()['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple


def edit_pipe(
        self,
        pipe: meerschaum.Pipe,
        patch: bool = False,
        debug: bool = False,
    ) -> SuccessTuple:
    """Submit a PATCH to the API to edit an existing Pipe object.
    Returns a tuple of (success_bool, response_dict).

    Parameters
    ----------
    pipe: meerschaum.Pipe :
        
    patch: bool :
         (Default value = False)
    debug: bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import _static_config
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
    if isinstance(response.json(), list):
        response_tuple = response.__bool__(), response.json()[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response.json()['detail']
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
    from meerschaum.utils.warnings import error
    from meerschaum.config.static import _static_config
    import json
    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    if tags is None:
        tags = []

    r_url = _static_config()['api']['endpoints']['pipes'] + '/keys'
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
        pipe: Optional[meerschaum.Pipe] = None,
        df: Optional[Union[pandas.DataFrame, Dict[Any, Any], str]] = None,
        chunksize: Optional[int] = -1,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """Append a pandas DataFrame to a Pipe.
    If Pipe does not exist, it is registered with supplied metadata.
        NOTE: columns['datetime'] must be set for new Pipes.

    Parameters
    ----------
    pipe : Optional[meerschaum.Pipe] :
         (Default value = None)
    df : Optional[Union[pandas.DataFrame :
        
    Dict[Any :
        
    Any] :
        
    str]] :
         (Default value = None)
    chunksize : Optional[int] :
         (Default value = -1)
    debug : bool :
         (Default value = False)
    **kw : Any :
        

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import json_serialize_datetime
    from meerschaum.config import get_config
    from meerschaum.utils.packages import attempt_import
    import json, time
    begin = time.time()
    more_itertools = attempt_import('more_itertools')
    if df is None:
        msg = f"DataFrame is `None`. Cannot sync {pipe}."
        return False, msg

    def get_json_str(c):
        ### allow syncing dict or JSON without needing to import pandas (for IOT devices)
        return (
            json.dumps(c, default=json_serialize_datetime) if isinstance(c, dict)
            else c.to_json(date_format='iso', date_unit='us')
        )

    df = json.loads(df) if isinstance(df, str) else df

    ### TODO Make separate chunksize for API?
    _chunksize : Optional[int] = (1 if chunksize is None else (
        get_config('system', 'connectors', 'sql', 'chunksize') if chunksize == -1
        else chunksize
    ))
    keys : list = list(df.keys())
    chunks = []
    if hasattr(df, 'index'):
        rowcount = len(df)
        chunks = [df.iloc[i] for i in more_itertools.chunked(df.index, _chunksize)]
    elif isinstance(df, dict):
        ### `_chunks` is a dict of lists of dicts.
        ### e.g. {'a' : [ {'a':[1, 2]}, {'a':[3, 4]} ] }
        _chunks = {k:[] for k in keys}
        rowcount = len(df[keys[0]])
        for k in keys:
            if len(df[k]) != rowcount:
                return False, "Arrays must all be the same length."
            chunk_iter = more_itertools.chunked(df[k], _chunksize)
            for l in chunk_iter:
                _chunks[k].append({k:l})

        ### `chunks` is a list of dicts (e.g. orient by rows in pandas JSON).
        for k, l in _chunks.items():
            for i, c in enumerate(l):
                try:
                    chunks[i].update(c)
                except IndexError:
                    chunks.append(c)

    ### Send columns in case the user has defined them locally.
    if pipe.columns:
        kw['columns'] = json.dumps(pipe.columns)
    r_url = pipe_r_url(pipe) + '/data'

    for i, c in enumerate(chunks):
        if debug:
            dprint(f"Posting chunk ({i + 1} / {_chunksize}) to {r_url}...")
            print(c)
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
            warn(str(e))
            return False, str(e)
            
        if not response:
            return False, f"Failed to receive response. Response text: {response.text}"

        try:
            j = json.loads(response.text)
        except Exception as e:
            return False, str(e)

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

    len_chunks = len(chunks)

    success_tuple = True, (
        f"It took {round(time.time() - begin, 2)} seconds to sync {rowcount} row"
        + ('s' if rowcount != 1 else '')
        + f" across {len_chunks} chunk" + ('s' if len_chunks != 1 else '') +
        f" to {pipe}."
    )
    return success_tuple


def delete_pipe(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        debug: bool = None,        
    ) -> SuccessTuple:
    """Delete a Pipe and drop its table.

    Parameters
    ----------
    pipe: Optional[meerscahum.Pipe] :
         (Default value = None)
    debug: bool :
         (Default value = None)

    Returns
    -------

    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    if pipe is None:
        error(f"Pipe cannot be None.")
    r_url = pipe_r_url(pipe)
    response = self.delete(
        r_url + '/delete',
        debug = debug,
    )
    if debug:
        dprint(response.text)
    if isinstance(response.json(), list):
        response_tuple = response.__bool__(), response.json()[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response.json()['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple


def get_pipe_data(
        self,
        pipe : meerschaum.Pipe,
        begin : Optional[datetime.datetime] = None,
        end : Optional[datetime.datetime] = None,
        params : Optional[Dict[str, Any]] = None,
        as_chunks : bool = False,
        debug : bool = False,
        **kw: Any
    ) -> Optional[pandas.DataFrame]:
    """Fetch data from the API.

    Parameters
    ----------
    pipe : meerschaum.Pipe :
        
    begin : Optional[datetime.datetime] :
         (Default value = None)
    end : Optional[datetime.datetime] :
         (Default value = None)
    params : Optional[Dict[str :
        
    Any]] :
         (Default value = None)
    as_chunks : bool :
         (Default value = False)
    debug : bool :
         (Default value = False)
    **kw: Any :
        

    Returns
    -------

    """
    import json
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    chunks_list = []
    while True:
        try:
            response = self.get(
                r_url + "/data",
                params = {'begin': begin, 'end': end, 'params': json.dumps(params)},
                debug = debug
            )
            j = response.json()
        except Exception as e:
            warn(str(e))
            return None
        if isinstance(j, dict) and 'detail' in j:
            return False, j['detail']
        break
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import parse_df_datetimes
    pd = import_pandas()
    try:
        df = pd.read_json(response.text)
    except Exception as e:
        warn(str(e))
        return None
    df = parse_df_datetimes(pd.read_json(response.text), debug=debug)
    return df

def get_backtrack_data(
        self,
        pipe : meerschaum.Pipe,
        begin : datetime.datetime,
        backtrack_minutes : int = 0,
        params: Optional[Dict[str, Any]] = None,
        debug : bool = False,
        **kw : Any,
    ) -> pandas.DataFrame:
    """Get a Pipe's backtrack data from the API.

    Parameters
    ----------
    pipe : meerschaum.Pipe :
        
    begin : datetime.datetime :
        
    backtrack_minutes : int :
         (Default value = 0)
    params: Optional[Dict[str :
        
    Any]] :
         (Default value = None)
    debug : bool :
         (Default value = False)
    **kw : Any :
        

    Returns
    -------

    """
    import json
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    try:
        response = self.get(
            r_url + "/backtrack_data",
            params = {
                'begin': begin,
                'backtrack_minutes': backtrack_minutes,
                'params': json.dumps(params),
            },
            debug = debug
        )
    except Exception as e:
        warn(f"Failed to parse backtrack data JSON for {pipe}. Exception:\n" + str(e))
        return None
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import parse_df_datetimes
    if debug:
        dprint(response.text)
    pd = import_pandas()
    try:
        df = pd.read_json(response.text)
    except Exception as e:
        warn(str(e))
        return None
    df = parse_df_datetimes(pd.read_json(response.text), debug=debug)
    return df

def get_pipe_id(
        self,
        pipe : meerschuam.Pipe,
        debug : bool = False,
    ) -> int:
    """Get a Pipe's ID from the API

    Parameters
    ----------
    pipe : meerschuam.Pipe :
        
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + '/id',
        debug = debug
    )
    if debug:
        dprint(response.text)
    try:
        return int(response.text)
    except Exception as e:
        return None

def get_pipe_attributes(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False,
    ) -> Mapping[str, Any]:
    """Get a Pipe's attributes from the API

    Parameters
    ----------
    pipe : meerschaum.Pipe :
        
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/attributes', debug=debug)
    import json
    try:
        return json.loads(response.text)
    except Exception as e:
        return None

def get_sync_time(
        self,
        pipe : 'meerschaum.Pipe',
        params : Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug : bool = False,
    ) -> datetime.datetime:
    """Get a Pipe's most recent datetime value from the API.

    Parameters
    ----------
    pipe :
        The pipe to select from.
    params :
        Optional params dictionary to build the WHERE clause.
    newest :
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).
        Defaults to `True`.
    round_down :
        If `True`, round the resulting datetime value down to the nearest minute.
        Defaults to `True`.
    pipe : 'meerschaum.Pipe' :
        
    params : Optional[Dict[str :
        
    Any]] :
         (Default value = None)
    newest: bool :
         (Default value = True)
    round_down: bool :
         (Default value = True)
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    import datetime, json
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + '/sync_time',
        json = params,
        params = {'newest': newest, 'debug': debug, 'round_down': round_down},
        debug = debug
    )
    if not response:
        return None
    j = response.json()
    if j is None:
        dt = None
    else:
        try:
            dt = datetime.datetime.fromisoformat(json.loads(response.text))
        except Exception as e:
            dt = None
    return dt

def pipe_exists(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> bool:
    """Consult the API to see if a Pipe exists

    Parameters
    ----------
    pipe : 'meerschaum.Pipe' :
        
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/exists', debug=debug)
    if debug:
        dprint("Received response: " + str(response.text))
    j = response.json()
    if isinstance(j, dict) and 'detail' in j:
        return False, j['detail']
    return j

def create_metadata(
        self,
        debug : bool = False
    ) -> bool:
    """Create Pipe metadata tables

    Parameters
    ----------
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import _static_config
    import json
    r_url = _static_config()['api']['endpoints']['metadata']
    response = self.post(r_url, debug=debug)
    if debug:
        dprint("Create metadata response: {response.text}")
    try:
        metadata_response = json.loads(response.text)
    except Exception as e:
        metadata_response = False
    return False

def get_pipe_rowcount(
        self,
        pipe : 'meerschaum.Pipe',
        begin : 'datetime.datetime' = None,
        end : 'datetime.datetime' = None,
        params : Optional[Dict[str, Any]] = None,
        remote : bool = False,
        debug : bool = False,
    ) -> Optional[int]:
    """Get a pipe's row couunt from the API.

    Parameters
    ----------
    pipe : 'meerschaum.Pipe' :
        
    begin : 'datetime.datetime' :
         (Default value = None)
    end : 'datetime.datetime' :
         (Default value = None)
    params : Optional[Dict[str :
        
    Any]] :
         (Default value = None)
    remote : bool :
         (Default value = False)
    debug : bool :
         (Default value = False)

    Returns
    -------

    """
    import json
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + "/rowcount",
        json = params,
        params = {
            'begin' : begin,
            'end' : end,
            'remote' : remote,
        },
        debug = debug
    )
    try:
        return int(json.loads(response.text))
    except Exception as e:
        return None


def drop_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False
    ) -> SuccessTuple:
    """Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe :
        
    debug: bool :
         (Default value = False)

    Returns
    -------

    """
    return self.do_action(
        ['drop', 'pipes'],
        connector_keys = pipe.connector_keys,
        metric_keys = pipe.metric_key,
        location_keys = pipe.location_key,
        force = True,
        debug = debug
    )


def clear_pipe(
        self,
        pipe: meerschaum.Pipe,
        force: bool = False,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe :
        
    force: bool :
         (Default value = False)
    debug: bool :
         (Default value = False)
    **kw :
        

    Returns
    -------

    """
    kw.pop('metric_keys', None)
    kw.pop('connector_keys', None)
    kw.pop('location_keys', None)
    kw.pop('action', None)
    return self.do_action(
        ['clear', 'pipes'],
        connector_keys = pipe.connector_keys,
        metric_keys = pipe.metric_key,
        location_keys = pipe.location_key,
        force = True,
        debug = debug,
        **kw
    )


def get_pipe_columns_types(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False,
    ) -> Optional[Dict[str, str]]:
    """

    Parameters
    ----------
    pipe : meerschaum.Pipe :
        
    debug : bool :
         (Default value = False)

    Returns
    -------
    type
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
    r_url = pipe_r_url(pipe) + '/columns/types'
    response = self.get(
        r_url,
        debug = debug
    )
    j = response.json()
    if isinstance(j, dict) and 'detail' in j and len(j.keys()) == 1:
        from meerschaum.utils.warnings import warn
        warn(j['detail'])
        return None
    if not isinstance(j, dict):
        warn(response.text)
        return None
    return j
