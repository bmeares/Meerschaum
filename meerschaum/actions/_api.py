#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

import sys

def api(
        action : list = [''],
        sysargs : list = [],
        debug : bool = False,
        mrsm_instance : str = None,
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
    
    If command is `start`, launch the Meerschaum WebAPI. If command is an api connector label,
        connect to that label. Otherwise connect to `main` api connector.
    """
    if action[0] == '':
        print(api.__doc__)
        return False, "Please provide a command to excecute (see above)"

    boot_keywords = {'start', 'boot', 'init'}
    if action[0] in boot_keywords:
        return _api_start(action=action, debug=debug, **kw)

    from meerschaum.config import config as cf, get_config
    from meerschaum.connectors import get_connector
    from meerschaum.utils.warnings import warn
    import requests
    if debug: from pprintpp import pprint
    api_configs = get_config('meerschaum', 'connectors', 'api', patch=True)

    api_label = "main"
    args_to_send = list(sysargs)
    ### remove `api`
    if 'api' in args_to_send: del args_to_send[0]
    if action[0] in api_configs:
        api_label = action[0]
        ### remove label from actions
        del action[0]
        if len(args_to_send) > 1: del args_to_send[0]
    kw['action'] = action
    kw['debug'] = debug
    kw['sysargs'] = args_to_send
    kw['yes'] = True
 
    api_conn = get_connector(type='api', label=api_label)
    
    if mrsm_instance is not None and str(mrsm_instance) == str(api_conn):
        warn(f"Cannot send Meerschaum instance keys '{mrsm_instance}' to itself. Removing from arguments...")
    elif mrsm_instance is not None: kw['mrsm_instance'] = str(mrsm_instance)

    success, message = api_conn.do_action(**kw)
    msg = f"Action " + ('succeeded' if success else 'failed') + " with message:\n" + str(message)
    print(msg)
    return success, message

def _api_start(
        action : list = [''],
        port : int = None,
        workers : int = None,
        mrsm_instance : str = None,
        debug : bool = False,
        **kw
    ):
    """
    Usage: `api start {options}`
    Options:
        - `-p, --port {number}`
            Port to listen to
        - `-w, --workers {number}`
            How many worker threads to run
    """
    from meerschaum.utils.misc import is_int
    from meerschaum.api import sys_config as api_config, __version__
    from pprintpp import pprint
    from meerschaum.utils.misc import attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.config._paths import API_UVICORN_CONFIG_PATH
    from meerschaum.config import get_config
    uvicorn = attempt_import('uvicorn')

    uvicorn_config = dict(api_config['uvicorn'])
    if port is None:
        ### default
        port = uvicorn_config['port']
        if len(action) > 1:
            if is_int(action[1]):
                port = int(action[1])

    if workers is not None:
        uvicorn_config['workers'] = workers

    if mrsm_instance is None: mrsm_instance = get_config('meerschaum', 'api_instance', patch=True)

    uvicorn_config['port'] = port
    uvicorn_config['reload'] = debug
    uvicorn_config['mrsm_instance'] = mrsm_instance

    custom_keys = ['mrsm_instance']

    ### write config to a temporary file to communicate with uvicorn threads
    yaml = attempt_import('yaml')
    try:
        if debug: dprint(f"Removing and writing Uvicorn config: ({API_UVICORN_CONFIG_PATH})")
        os.remove(API_UVICORN_CONFIG_PATH)
    except:
        pass
    with open(API_UVICORN_CONFIG_PATH, 'w+') as f:
        yaml.dump(uvicorn_config, f)

    ### remove custom keys before callign uvicorn
    for k in custom_keys:
        del uvicorn_config[k]

    from meerschaum.api import get_connector
    if debug:
        ### instantiate the SQL connector
        dprint(f"Connecting to Meerschaum instance: {mrsm_instance}")

        dprint(f"Starting Meerschaum API v{__version__} with the following configuration:")
        pprint(uvicorn_config, stream=sys.stderr)

    uvicorn.run(**uvicorn_config)

    return (True, "Success")

