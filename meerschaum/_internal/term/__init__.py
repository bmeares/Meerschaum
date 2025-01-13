#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the web console virtual terminal using Tornado and xterm.js.
"""

from __future__ import annotations

from typing import Optional, Tuple

import meerschaum as mrsm
from meerschaum.utils.packages import attempt_import
from meerschaum._internal.term.TermPageHandler import TermPageHandler, CustomTermSocket
from meerschaum.config._paths import API_TEMPLATES_PATH, API_STATIC_PATH
from meerschaum.utils.venv import venv_executable
from meerschaum.utils.misc import is_tmux_available
from meerschaum.utils.daemon._names import get_new_daemon_name

tornado, tornado_ioloop, terminado = attempt_import(
    'tornado', 'tornado.ioloop', 'terminado', lazy=False,
)


def get_webterm_app_and_manager(
    instance_keys: Optional[str] = None,
) -> Tuple[
    tornado.web.Application,
    terminado.UniqueTermManager,
]:
    """
    Construct the Tornado web app and term manager from the provided sysargs.

    Returns
    -------
    A tuple of the Tornado web application and term manager.
    """
    shell_kwargs_str = f"mrsm_instance='{instance_keys}'" if instance_keys else ""
    commands = [
        venv_executable(None),
        '-c',
        "import os; _ = os.environ.pop('COLUMNS', None); _ = os.environ.pop('LINES', None); "
        "from meerschaum._internal.entry import get_shell; "
        f"get_shell({shell_kwargs_str}).cmdloop()"
    ]
    webterm_cf = mrsm.get_config('system', 'webterm')
    if webterm_cf.get('tmux', {}).get('enabled', False) and is_tmux_available():
        commands = ['tmux', 'new-session', '-A', '-s', 'MRSM_SESSION'] + commands

    term_manager = terminado.NamedTermManager(shell_command=commands)
    handlers = [
        (
            r"/websocket/(.+)/?",
            CustomTermSocket,
            {'term_manager': term_manager}
        ),
        (
            r"/webterm/(.+)/?",
            TermPageHandler
        ),
    ]
    tornado_app = tornado.web.Application(
        handlers,
        static_path=API_STATIC_PATH,
        template_path=API_TEMPLATES_PATH,
        term_manager=term_manager,
    )
    return tornado_app, term_manager
