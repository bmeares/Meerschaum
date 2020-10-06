#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register or fetch Pipes from the API
"""

from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import error

def register_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> tuple:
    """
    Submit a POST to the API to register a new Pipe object.
    Returns a tuple of (success_bool, response_dict)
    """
    #  if pipe.parameters is None:
        #  error(f"""
        #  Parameters is None for {pipe}. Please set the `parameters` member and re-register (see below):
        #  """ + """
        #  >>> pipe.parameters = {
        #  >>>     "fetch" : {
        #  >>>         "definition" : "SELECT * FROM test",
        #  >>>         "backtrack_minutes" : 1000,
        #  >>>     },
        #  >>> }
        #  >>> 
        #  >>> pipe.register()
        #  """)
    pipe.meta.parameters = pipe.parameters
    if pipe.meta.parameters is None: pipe.meta.parameters = dict()
    response = self.post('/mrsm/pipes', json=pipe.meta.dict())
    return response.__bool__(), response.json()

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
    pipe.meta.parameters = pipe.parameters
    if pipe.meta.parameters is None: pipe.meta.parameters = dict()
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"patch: {patch}")
    response = self.patch(
        '/mrsm/pipes',
        json=pipe.meta.dict(),
        params={'patch' : patch}
    )
    return response.__bool__(), response.json()

def get_pipes(
        self,
        connector_keys : str = None,
        metric_key : str = None,
        location_key : str = None,
        debug : bool = False
    ) -> dict:
    """
    Build a pipes dictionary from the API response.
    See meerschaum.utils._get_pipes.get_pipes for more information.
    """
    r_url = '/mrsm/pipes'
    if connector_keys is not None:
        r_url += f'/{connector_keys}'
    if metric_key is not None:
        if connector_keys is None:
            raise Exception("connector_keys cannot be None if metric_key is provided")
        r_url += f"/{metric_key}"
    if location_key is not None:
        if connector_keys is None or metric_key is None:
            raise Exception("connector_keys and metric_key cannot be None if location_key is provided")
        r_url += f"/{location_key}"
    j = self.get(r_url).json()
    from meerschaum.Pipe import Pipe
    pipes = dict(j)
    for ck in j:
        if ck not in pipes: pipes[ck] = dict()
        for mk in j[ck]:
            if mk not in pipes[ck]: pipes[ck][mk] = dict()
            for lk in j[ck][mk]:
                meta = j[ck][mk][lk]['meta']
                pipes[ck][mk][lk] = Pipe(**meta, source='api', debug=debug)
    return pipes

