#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the web console virtual terminal using Tornado and xterm.js.
"""

from __future__ import annotations

import os
import json
from typing import Optional, Tuple

import meerschaum as mrsm
from meerschaum.utils.packages import attempt_import
from meerschaum._internal.term.TermPageHandler import TermPageHandler, CustomTermSocket
from meerschaum.config._paths import API_TEMPLATES_PATH, API_STATIC_PATH
from meerschaum.utils.venv import venv_executable
from meerschaum.utils.misc import is_tmux_available

tornado, tornado_ioloop, terminado = attempt_import(
    'tornado', 'tornado.ioloop', 'terminado', lazy=False,
)


def get_webterm_app_and_manager(
    instance_keys: Optional[str] = None,
    port: Optional[int] = None,
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
    from meerschaum.config.environment import get_env_vars
    env_vars = get_env_vars()
    env_dict = {
        env_var: os.environ[env_var].replace('"', '\\"')
        for env_var in env_vars
    }
    quote_str = "__QUOTE__"
    env_str = json.dumps(env_dict, separators=(',', ':')).replace("'", quote_str)
    shell_kwargs_str = f"mrsm_instance='{instance_keys}'" if instance_keys else ""
    commands = [
        venv_executable(None),
        '-c',
        "import os; import json; "
        f"env_str = '{env_str}'.replace('{quote_str}', \"'\"); "
        "env_dict = json.loads(env_str); "
        "os.environ.update(env_dict); "
        "_ = os.environ.pop('COLUMNS', None); _ = os.environ.pop('LINES', None); "
        "from meerschaum._internal.entry import get_shell; "
        f"get_shell({shell_kwargs_str}).cmdloop()"
    ]
    webterm_cf = mrsm.get_config('api', 'webterm')
    if webterm_cf.get('tmux', {}).get('enabled', False) and is_tmux_available():
        commands = ['tmux', 'new-session', '-A', '-s', f'MRSM_SESSION--{port}'] + commands

    term_manager = terminado.NamedTermManager(shell_command=commands, extra_env=env_dict)
    term_manager._port = port
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
