#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the web console virtual terminal using Tornado and xterm.js.
"""

from __future__ import annotations

import os
import json
import pathlib
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
    env_path: Optional[pathlib.Path] = None,
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
    from meerschaum.config.environment import get_env_vars, get_daemon_env_vars
    if env_path is None:
        from meerschaum.config.paths import WEBTERM_INTERNAL_RESOURCES_PATH
        env_path = WEBTERM_INTERNAL_RESOURCES_PATH / (str(port) + '.json')

    daemon_env_vars = get_daemon_env_vars()
    env_dict = {
        env_var: env_val
        for env_var, env_val in get_env_vars().items()
        if env_var not in daemon_env_vars
    }
    with open(env_path, 'w+', encoding='utf-8') as f:
        json.dump(env_dict, f)

    shell_kwargs_str = f"mrsm_instance='{instance_keys}'" if instance_keys else ""
    commands = [
        venv_executable(None),
        '-c',
        "import os\n"
        "import pathlib\n"
        "import json\n"
        f"env_path = pathlib.Path('{env_path.as_posix()}')\n"
        "with open(env_path, 'r', encoding='utf-8') as f:\n"
        "    env_dict = json.load(f)\n"
        "os.environ.update(env_dict)\n"
        "_ = os.environ.pop('COLUMNS', None)\n"
        "_ = os.environ.pop('LINES', None)\n"
        "from meerschaum._internal.entry import get_shell\n"
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
