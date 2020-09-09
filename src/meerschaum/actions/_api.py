#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Start the Meerschaum WebAPI with the `api` action.
"""

def api(
        action : list = [''],
        debug=False,
        port=None,
        **kw
    ):
    """
    Run the Meerschaum WebAPI
    """
    from meerschaum.api import port as default_port
    from meerschaum.utils.misc import is_int
    import uvicorn

    if port is None:
        if is_int(action[0]):
            port = int(action[0])
        else: port = default_port
    
    if debug: print(f"Starting Meerschaum Web API on port {port}")
    uvicorn.run('meerschaum.api:fast_api', port=port, host="0.0.0.0")

    return (True, "Success")
