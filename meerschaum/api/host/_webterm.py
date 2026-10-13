#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The web terminal in the API host (v5 design §7.1 "Webterm").

v4 serves `/webterm/{session_id}` and its websocket from the API, proxying to a `start webterm`
process the API starts on its own startup. In v5 the Rust console links to that route and the
Rust server proxies it to the host, which keeps v4's implementation: the routes in
`meerschaum.api.routes._webterm` (authenticating the console session through the shared session
store) and the terminal process managed by `meerschaum.api.dash.webterm`.
"""

from __future__ import annotations

import traceback

from meerschaum.utils.typing import Any


def mount_webterm(app: Any, debug: bool = False) -> bool:
    """
    Register the webterm routes on the host app, start the terminal process, and stop it when
    the host shuts down.

    Returns
    -------
    Whether the routes were mounted.
    """
    from meerschaum.utils.warnings import warn
    try:
        import meerschaum.api.routes._webterm  # noqa: F401 (registers the routes on `app`)
        from meerschaum.api import webterm_port
        from meerschaum.api.dash.webterm import start_webterm, stop_webterm
    except Exception:
        warn(f"Failed to mount the web terminal:\n{traceback.format_exc()}", stack=False)
        return False

    start_webterm(webterm_port=webterm_port)
    ### `on_event` is how v4's API registers its shutdown work; it is what the app supports.
    app.router.add_event_handler('shutdown', stop_webterm)
    return True
