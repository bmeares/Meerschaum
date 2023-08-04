#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the Webterm via the dashboard.
"""

import time
from meerschaum.api import debug, CHECK_UPDATE
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple, Optional
from meerschaum.utils.packages import attempt_import, import_html, import_dcc, run_python_package
from meerschaum._internal.term.tools import is_webterm_running
from meerschaum.config.static import STATIC_CONFIG
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

MAX_WEBTERM_ATTEMPTS: int = 10

def get_webterm(state: WebState) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    """
    Start the webterm and return its iframe.
    """
    protocol, host, port = 'http', '127.0.0.1', 8765
    if not is_webterm_running(host, port, protocol):
        run_python_package(
            'meerschaum', [
                'start', 'webterm', '-d', '--noask',
                '--name', STATIC_CONFIG['api']['webterm_job_name'],
            ],
            foreground = False,
            venv = None,
        )

    webterm_iframe = html.Iframe(
        src = f'{protocol}://{host}:{port}',
        id = "webterm-iframe",
    )
    for i in range(MAX_WEBTERM_ATTEMPTS):
        if is_webterm_running(host, port, protocol):
            return webterm_iframe, []
        time.sleep(1)
    return console_div, [alert_from_success_tuple((False, "Could not start the webterm server."))]
