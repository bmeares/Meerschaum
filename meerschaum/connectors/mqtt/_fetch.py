#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Standard Connector fetch method for grabbing new data.

Fetch is called when implicitly syncing pipes with connectors.
"""

def fetch(
        self,
        pipe : 'meerschaum.Pipe',
        callback : 'function' = None,
        debug : bool = False,
        **kw
    ) -> 'None':
    """Subscribe to a topic, parse the JSON when messages come in, and send data to Pipe.
    
    Unlike other fetch functions, MQTT fetch depends on callbacks and calls pipe.sync() directly,
    rather than being a subroutine like SQL or API.

    Parameters
    ----------
    pipe : 'meerschaum.Pipe' :
        
    callback : 'function' :
         (Default value = None)
    debug : bool :
         (Default value = False)
    **kw :
        

    Returns
    -------

    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.debug import dprint

    if 'fetch' not in pipe.parameters:
        warn(f"Parameters for pipe {pipe} must include \"fetch\".")
        return None
    
    instructions = pipe.parameters.get('fetch', {})

    topic = instructions.get('topic', None)
    if topic is None:
        warn(f"Missing topic from parameters for pipe {pipe}. Defaulting to \"#\" (all possible topics!).")
        topic = '#'

    ### default: only include values that have changed
    skip_duplicates = True
    if 'skip_duplicates' in instructions:
        skip_duplicates = instructions['skip_duplicates']

    ### callback is executed each time a message is published
    def _fetch_callback(msg : str):
        from meerschaum.utils.packages import import_pandas
        from meerschaum.utils.misc import parse_df_datetimes, df_from_literal
        pd = import_pandas()

        df = None
        ### first, try to parse JSON
        try:
            df = parse_df_datetimes(pd.read_json(msg))
        except Exception as e:
            pass

        ### if parsing JSON fails, see if we can parse it literally
        if df is None:
            df = df_from_literal(pipe, msg, debug=debug)

        if debug:
            dprint(f"{df}")
        pipe.sync(df, debug=debug)

    ### optional: user may override callback
    if callback is None:
        callback = _fetch_callback

    ### subscribe to the Pipe's topic
    self.subscribe(
        topic,
        callback,
        skip_duplicates = skip_duplicates,
        debug = debug
    )
