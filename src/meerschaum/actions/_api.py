#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

def api(
        action : list = [''],
        sysargs : list = [],
        debug : bool = False,
        **kw
    ):
    """
    Send commands to a Meerschaum WebAPI instance or boot a new instance

    Usage: `api [commands] {options}`
    Examples:
        - `api [start, boot, init]`
            - start the API server
        - `api show config`
            - execute `show config` on the `main` api instance
        - `api main show config`
            - see above
    
    If command is `server`, launch the Meerschaum WebAPI. If command is an api connector label,
        connect to that label. Otherwise connect to `main` api connector.
    """
    if action[0] == '':
        print(api.__doc__)
        return False, "Please provide a command to excecute (see above)"

    boot_keywords = {'start', 'boot', 'init'}
    if action[0] in boot_keywords:
        return _api_server(action=action, debug=debug, **kw)

    from meerschaum.config import config as cf
    from meerschaum.connectors import get_connector
    import requests
    if debug: from pprintpp import pprint
    api_configs = cf['meerschaum']['connectors']['api']

    api_label = "main"
    args_to_send = list(sysargs)
    ### remove `api`
    del args_to_send[0]
    if action[0] in api_configs:
        api_label = action[0]
        ### remove label from actions
        del action[0]
        del args_to_send[0]
    kw['action'] = action
    del kw['action'][0]
    kw['debug'] = debug
    kw['sysargs'] = args_to_send
 
    api_conn = get_connector(type='api', label=api_label)
    response = api_conn.do_action(**kw)
    status = (response.status_code == 200)
    succeeded = response.text[0]
    msg = response.text[1]
    response_json = json.loads(response.text)
    if debug: print(response_json)
    if debug: print(response.text)
    if debug: print(type(response.text))
    if debug: print(succeeded, msg)
    return succeeded, msg

def _api_server(
        action : list = [''],
        port : int = None,
        workers : int = None,
        debug : bool = False,
        **kw
    ):
    """
    Usage: `api server {options}`
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
        ### default
        port = uvicorn_config['port']
        if len(action) > 1:
            if is_int(action[1]):
                port = int(action[1])

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

