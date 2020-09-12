#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

def api(
        action : list = [''],
        port : int = None,
        workers : int = None,
        debug : bool = False,
        **kw
    ):
    """
    Run the Meerschaum WebAPI

    Usage: `api {options}`
    Options:
        - `-p, --port {number}`
            Port to listen to
        - `-w, --workers {number}`
            How many worker threads to run

    """
    from meerschaum.utils.misc import is_int
    from meerschaum.api import sys_config as api_config, __version__
    from pprintpp import pprint
    import uvicorn

    uvicorn_config = dict(api_config['uvicorn'])
    if port is None:
        if is_int(action[0]):
            port = int(action[0])
        else: port = uvicorn_config['port']

    if workers is not None:
        uvicorn_config['workers'] = workers

    uvicorn_config['port'] = port

    if debug:
        from meerschaum.api import connector
        print(f"Connection to database: {connector.host}")

    print(f"Starting Meerschaum API v{__version__} with the following configuration:")
    pprint(uvicorn_config)
    uvicorn.run(**uvicorn_config)

    return (True, "Success")
