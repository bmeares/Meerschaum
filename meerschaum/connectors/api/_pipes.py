#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register or fetch Pipes from the API
"""

from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import error

def pipe_r_url(
        pipe : 'meerschaum.Pipe'
    ) -> str:
    """
    Generate a relative URL path from a Pipe's keys.
    """
    location_key = pipe.location_key
    if location_key is None: location_key = '[None]'
    return f'/mrsm/pipes/{pipe.connector_keys}/{pipe.metric_key}/{location_key}'

def register_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> tuple:
    """
    Submit a POST to the API to register a new Pipe object.
    Returns a tuple of (success_bool, response_dict)
    """

    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.
    response = self.post('/mrsm/pipes', json=pipe.meta)
    if debug: dprint(response.text)
    if isinstance(response.json(), list):
        response_tuple = response.__bool__(), response.json()[1]
    elif 'detail' in response.json():
        response_tuple = response.__bool__(), response.json()['detail']
    else:
        response_tuple = response.__bool__(), response.text
    return response_tuple

def edit_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        patch : bool = False,
        debug : bool = False
    ) -> tuple:
    """
    Submit a PATCH to the API to edit an existing Pipe.
    Returns a tuple of (success_bool, response_dict)
    """
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"patch: {patch}")
    response = self.patch(
        '/mrsm/pipes',
        json = pipe.meta,
        params = {'patch' : patch}
    )
    return response.__bool__(), response.json()

def fetch_pipes_keys(
        self,
        connector_keys : list = [],
        metric_keys : list = [],
        location_keys : list = [],
        params : dict = dict(),
        mrsm_instance : str = 'api',
        debug : bool = False
    ) -> 'dict or list':
    """
    NOTE: This function no longer builds Pipes. Use the main `get_pipes()` function
          with the arguments `mrsm_instance = 'api' and `method = 'registered'` (default).

    Fetch registered Pipes' keys from the API.

    keys_only : bool : True
        If True, only return a list of tuples of the keys
        E.g. [ (connector_keys, metric_key, location_key) ]
        This is used by the main `get_pipes()` function for the 'api' method.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error
    import json

    r_url = '/mrsm/pipes/keys'
    try:
        j = self.get(
            r_url,
            params = {
                'connector_keys' : json.dumps(connector_keys),
                'metric_keys' : json.dumps(metric_keys),
                'location_keys' : json.dumps(location_keys),
                'params' : json.dumps(params),
                'debug' : debug,
            }
        ).json()
    except Exception as e:
        error(str(e))

    result = []
    for t in j:
        result.append( (t['connector_keys'], t['metric_key'], t['location_key']) )
    return result

def sync_pipe(
        self,
        pipe : 'meerschaum.Pipe' = None,
        df : 'pd.DataFrame' = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Append a pandas DataFrame to a Pipe.
    If Pipe does not exist, it is registered with supplied metadata
        NOTE: columns['datetime'] must be set for new Pipes.
    """
    from meerschaum.utils.warnings import warn
    if df is None:
        warn(f"DataFrame is None. Cannot sync pipe '{pipe}")
        return None
    r_url = pipe_r_url(pipe) + '/data'
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(r_url)
    try:
        response = self.post(
            r_url,
            ### handles check_existing
            params = kw,
            data = df.to_json(date_format='iso', date_unit='us')
        )
    except Exception as e:
        warn(e)
        return None

    return tuple(response.json())

def delete_pipe(
        self,
        pipe : 'mrsm.Pipe' = None,
        debug : bool = None,        
    ) -> tuple:
    """
    Delete a Pipe and drop its table.
    """
    return self.do_action(
        ['delete', 'pipes'],
        connector_keys = pipe.connector_keys,
        metric_keys = pipe.metric_key,
        location_keys = pipe.location_key,
        force = True,
        debug = debug
    )

def get_pipe_data(
        self,
        pipe : 'meerschaum.Pipe',
        begin : 'datetime.datetime' = None,
        end : 'datetime.datetime' = None,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Fetch data from the API
    """
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    try:
        response = self.get(r_url + "/data", params={'begin': begin, 'end': end})
    except Exception as e:
        warn(e)
        return None
    from meerschaum.utils.misc import import_pandas, parse_df_datetimes
    pd = import_pandas()
    try:
        df = pd.read_json(response.text)
    except Exception as e:
        warn(str(e))
        return None
    df = parse_df_datetimes(pd.read_json(response.text), debug=debug)
    if debug: dprint(df)
    return df

def get_backtrack_data(
        self,
        pipe : 'meerschaum.Pipe',
        begin : 'datetime.datetime',
        backtrack_minutes : int = 0,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Get a Pipe's backtrack data from the API
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    r_url = pipe_r_url(pipe)
    try:
        response = self.get(
            r_url + "/backtrack_data",
            params = {
                'begin': begin,
                'backtrack_minutes': backtrack_minutes,
            }
        )
    except Exception as e:
        warn(f"Failed to parse backtrack data JSON for pipe '{pipe}'. Exception:\n" + str(e))
        return None
    from meerschaum.utils.misc import import_pandas, parse_df_datetimes
    if debug: dprint(response.text)
    pd = import_pandas()
    try:
        df = pd.read_json(response.text)
    except Exception as e:
        warn(str(e))
        return None
    df = parse_df_datetimes(pd.read_json(response.text), debug=debug)
    if debug: dprint(df)
    return df

def get_pipe_id(
        self,
        pipe : 'meerschuam.Pipe',
        debug : bool = False,
    ) -> int:
    """
    Get a Pipe's ID from the API
    """
    from meerschaum.utils.debug import dprint
    r_url = pipe_r_url(pipe)
    response = self.get(
        r_url + '/id'
    )
    if debug: dprint(response.text)
    try:
        return int(response.text)
    except:
        return None

def get_pipe_attributes(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> dict:
    """
    Get a Pipe's attributes from the API
    """
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/attributes')
    import json
    try:
        return json.loads(response.text)
    except:
        return None

def get_sync_time(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> 'datetime.datetime':
    """
    Get a Pipe's most recent datetime value from the API
    """
    import datetime, json
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/sync_time')
    dt = datetime.datetime.fromisoformat(json.loads(response.text))
    return dt

def pipe_exists(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> bool:
    """
    Consult the API to see if a Pipe exists
    """
    import json
    r_url = pipe_r_url(pipe)
    response = self.get(r_url + '/exists')
    if debug: dprint("Received response: " + str(response.text))
    return json.loads(response.text)

def create_metadata(
        self,
        debug : bool = False
    ) -> bool:
    """
    Create Pipe metadata tables
    """
    import json
    r_url = '/mrsm/metadata'
    response = self.post(r_url)
    if debug: dprint("Create metadata response: {response.text}")
    return json.loads(response.text)
