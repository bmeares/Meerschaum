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
        debug : bool = False,
        **kw
    ) -> 'None':
    """
    Subscribe to a topic, parse the JSON when messages come in, and send data to Pipe.

    Unlike other fetch functions, MQTT fetch depends on callbacks and calls pipe.sync() directly,
    rather than being a subroutine like SQL or API.
    """
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.debug import dprint

    if 'fetch' not in pipe.parameters:
        warn(f"Parameters for pipe {pipe} must include \"fetch\".")
        return None
    
    instructions = pipe.parameters['fetch']
    if 'topic' not in instructions:
        warn(f"Missing topic from parameters for pipe {pipe}. Defaulting to \"#\" (all possible topics!).")

    ### callback is executed each time a message is published
    def _fetch_callback(msg : str):
        from meerschaum.utils.misc import import_pandas, parse_df_datetimes
        pd = import_pandas()
        df = parse_df_datetimes(pd.read_json(msg))
        if debug: dprint(f"{df}")
        pipe.sync(df, debug=debug)

    self.subscribe(instructions['topic'], _fetch_callback, debug=debug)
