#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def api(
        action : Optional[List[str]] = None,
        sysargs : Optional[List[str]] = None,
        debug : bool = False,
        mrsm_instance : Optional[str] = None,
        **kw : Any
    ) -> SuccessTuple:
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
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import print_tuple
    if action is None:
        action = []
    if sysargs is None:
        sysargs = []
    if len(action) == 0:
        info(api.__doc__)
        return False, "Please provide a command to excecute (see above)"

    boot_keywords = {'start', 'boot', 'init'}
    if action[0] in boot_keywords:
        return _api_start(action=action, mrsm_instance=mrsm_instance, debug=debug, **kw)

    from meerschaum.config import get_config
    from meerschaum.connectors import get_connector
    import requests
    if debug:
        from meerschaum.utils.formatting import pprint
    api_configs = get_config('meerschaum', 'connectors', 'api', patch=True)

    api_label = "main"
    args_to_send = list(sysargs)
    ### remove `api`
    if 'api' in args_to_send:
        del args_to_send[0]
    if action[0] in api_configs:
        api_label = action[0]
        ### remove label from actions
        del action[0]
        if len(args_to_send) > 1:
            del args_to_send[0]
    kw['action'] = action
    kw['debug'] = debug
    kw['sysargs'] = args_to_send
    kw['yes'] = True
 
    api_conn = get_connector(type='api', label=api_label)
    
    if mrsm_instance is not None and str(mrsm_instance) == str(api_conn):
        warn(
            f"Cannot send Meerschaum instance keys '{mrsm_instance}' to itself. " +
            "Removing from arguments..."
        )
    elif mrsm_instance is not None:
        kw['mrsm_instance'] = str(mrsm_instance)

    success, message = api_conn.do_action(**kw)
    print_tuple((success, message), common_only=True)
    msg = f"Action " + ('succeeded' if success else 'failed') + " with message:\n" + str(message)
    return success, message

def _api_start(
        action : Optional[List[str]] = None,
        port : Optional[int] = None,
        workers : Optional[int] = None,
        mrsm_instance : Optional[str] = None,
        no_dash : bool = False,
        debug : bool = False,
        nopretty : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Start the API server.

    Usage:
        `api start {options}`

    Options:
        - `-p, --port {number}`
            Port to bind the API server to.

        - `-w, --workers {number}`
            How many worker threads to run.
            Defaults to the number of CPU cores or 1 on Android.
    """
    from meerschaum.utils.packages import attempt_import, venv_contains_package, pip_install
    from meerschaum.utils.misc import is_int
    from meerschaum.utils.formatting import pprint, ANSI
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error, warn
    from meerschaum.config import get_config, _config
    from meerschaum.config._paths import (
        API_UVICORN_CONFIG_PATH, CACHE_RESOURCES_PATH,
        PACKAGE_ROOT_PATH,
    )
    from meerschaum.config.static import _static_config
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.pool import get_pool
    import os

    if action is None:
        action = []

    old_cwd = os.getcwd()
    if not debug:
        os.chdir(CACHE_RESOURCES_PATH)

    ### Uvicorn must be installed on the host because of multiprocessing reasons.
    uvicorn = attempt_import('uvicorn', venv=None, lazy=False)

    api_config = get_config('system', 'api')
    cf = _config()
    uvicorn_config = dict(api_config['uvicorn'])
    if port is None:
        ### default
        port = uvicorn_config['port']
        if len(action) > 1:
            if is_int(action[1]):
                port = int(action[1])

    pool = get_pool(workers=workers)
    if pool is None:
        workers = 1
    else:
        pool.close()
        pool.join()
    
    uvicorn_config['workers'] = workers

    uvicorn_config['debug'] = debug

    if mrsm_instance is None:
        mrsm_instance = get_config('meerschaum', 'api_instance', patch=True)

    ### Check if the API instance connector is another API
    instance_connector = parse_instance_keys(mrsm_instance, debug=debug)
    if instance_connector.type == 'api' and instance_connector.protocol != 'https':
        allow_http_parent = get_config(
            'system', 'api', 'permissions', 'chaining', 'insecure_parent_instance'
        )
        if not allow_http_parent:
            return False, (
                "Chaining Meerschaum API instances over HTTP is disabled!\n\n" +
                f"To use '{instance_connector}' as the Meerschaum instance for this API server, " +
                "please do one of the following:\n\n" +
                f"  - Ensure that '{instance_connector}' is available over HTTPS, " +
                "and with `edit config`,\n" +
                f"    change the `protocol` for '{instance_connector}' to 'https'.\n\n" +
                "  - Run `edit config system` and search for `permissions`.\n" +
                "    Under `api:permissions:chaining`, change the value of " +
                "`insecure_parent_instance` to `true`,\n" +
                "    then restart the API process."
            )

    uvicorn_config.update({
        'port' : port,
        'reload' : debug,
        'mrsm_instance' : mrsm_instance,
        'no_dash' : no_dash,
    })
    if debug:
        uvicorn_config['reload_dirs'] = [str(PACKAGE_ROOT_PATH)]
    uvicorn_config['use_colors'] = (not nopretty) if nopretty else ANSI

    api_config['uvicorn'] = uvicorn_config
    cf['system']['api']['uvicorn'] = uvicorn_config
    from meerschaum.api import __version__

    custom_keys = ['mrsm_instance', 'no_dash']

    ### write config to a temporary file to communicate with uvicorn threads
    import json, sys
    try:
        if API_UVICORN_CONFIG_PATH.exists() and not debug:
            os.remove(API_UVICORN_CONFIG_PATH)
            assert(not API_UVICORN_CONFIG_PATH.exists())
    except Exception as e:
        error(e)
    with open(API_UVICORN_CONFIG_PATH, 'w+') as f:
        if debug:
            dprint(f"Dumping API config file:", nopretty=nopretty)
            pprint(uvicorn_config, stream=sys.stderr, nopretty=nopretty)
        json.dump(uvicorn_config, f)

    ### remove custom keys before calling uvicorn
    for k in custom_keys:
        del uvicorn_config[k]

    if debug:
        dprint(f"Connecting to Meerschaum instance: '{mrsm_instance}'.", nopretty=nopretty)
        dprint(
            f"Starting Meerschaum API v{__version__} with the following configuration:",
            nopretty = nopretty
        )
        pprint(uvicorn_config, stream=sys.stderr, nopretty=nopretty)

    try:
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        pass

    os.chdir(old_cwd)

    return (True, "Success")
