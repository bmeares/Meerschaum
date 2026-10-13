#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build and serve the API host's FastAPI app (§7.3).
"""

from __future__ import annotations

import os
import traceback

from meerschaum.utils.typing import Any, Dict, List, Optional

_built: Dict[str, Any] = {'app': None}


def _require_host_mode() -> None:
    from meerschaum.api.host import is_host_mode, HOST_ENV
    if not is_host_mode():
        raise RuntimeError(
            f"The API host requires `{HOST_ENV}=1` before `meerschaum.api` is imported."
        )
    import meerschaum.api as api
    if not getattr(api, '_host_mode', False):
        raise RuntimeError(
            "`meerschaum.api` was imported before host mode was set; "
            "start the host with `python -m meerschaum.api.host`."
        )


def plugin_name_of(module_name: Optional[str]) -> Optional[str]:
    """
    Return the plugin name for a registry key (`plugins.foo` or `foo`), as `meerschaum.api` does.
    """
    if not module_name:
        return module_name
    if module_name.startswith('plugins.'):
        return module_name.split('.')[1]
    return module_name


def run_api_plugins(app, debug: bool = False) -> List[str]:
    """
    Run every `@api_plugin` function against `app` in plugin order, inside each plugin's venv.
    A failing function is reported and skipped, as in v4.

    Returns
    -------
    The names of the plugins whose functions ran.
    """
    import meerschaum as mrsm
    from meerschaum.plugins import _api_plugins
    from meerschaum.utils.warnings import warn

    ran = []
    for module_name, functions in list(_api_plugins.items()):
        plugin_name = plugin_name_of(module_name)
        plugin = mrsm.Plugin(plugin_name) if plugin_name else None
        if plugin is not None:
            plugin.activate_venv(debug=debug)
        for function in functions:
            try:
                function(app)
            except Exception as e:
                traceback.print_exc()
                warn(
                    f"Failed to load API plugin '{plugin}' "
                    + f"when executing function '{function.__name__}' with exception:\n{e}",
                    stack=False,
                )
        if plugin_name:
            ran.append(plugin_name)
    return ran


def build_app(load_plugins: bool = True, debug: bool = False):
    """
    Build the host app once: install the `meerschaum.api.dash` surface and the user loader,
    import the plugins, run the API plugins, mount the plugin Dash apps, add the internal routes.
    """
    if _built['app'] is not None:
        return _built['app']

    _require_host_mode()
    from meerschaum.api import app
    from meerschaum.api.host._auth import install_user_loader
    from meerschaum.api.host import dash as host_dash
    from meerschaum.api.host import _internal

    install_user_loader()
    host_dash.install_dash_surface()

    if load_plugins:
        from meerschaum.plugins import load_plugins as _load_plugins
        _load_plugins(skip_if_loaded=True, debug=debug)

    ### The internal routes come first so a plugin cannot shadow them.
    _internal.register(app)
    run_api_plugins(app, debug=debug)
    host_dash.mount_dash_apps(app, debug=debug)

    _built['app'] = app
    return app


def run(socket_path: str, debug: bool = False) -> None:
    """
    Serve the host app with uvicorn on a Unix socket.
    """
    from meerschaum.utils.packages import attempt_import
    uvicorn = attempt_import('uvicorn', lazy=False, check_update=False)
    app = build_app(debug=debug)
    if os.path.exists(socket_path):
        os.remove(socket_path)
    os.makedirs(os.path.dirname(os.path.abspath(socket_path)), exist_ok=True)
    uvicorn.run(
        app,
        uds=socket_path,
        log_level=('debug' if debug else 'warning'),
        proxy_headers=False,
        ### Websockets and lifespan events behave as under `start api`.
        lifespan='on',
    )
