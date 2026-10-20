#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build and serve the API host's FastAPI app (§7.3).
"""

from __future__ import annotations

import contextlib
import os
import sys
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


@contextlib.contextmanager
def _keep_venv_order():
    """
    Leave `sys.path` (and the venv activation order) as they were when the block ends.

    Building the console imports its packages through the `mrsm` venv, which moves that venv ahead
    of the plugins' venvs that `run_api_plugins()` activated. v4's (and rc1's) order, which an
    `@api_plugin` route sees when it runs, is the plugin venvs, the base interpreter, then `mrsm`:
    a package that a plugin pins in its own venv must keep winning over the shared one.
    """
    from meerschaum.utils import venv as _venv
    saved_path = list(sys.path)
    saved_order = list(_venv.active_venvs_order)
    try:
        yield
    finally:
        with _venv.LOCKS['active_venvs']:
            present = set(sys.path)
            previous = set(saved_path)
            sys.path[:] = (
                [path for path in saved_path if path in present]
                + [path for path in sys.path if path not in previous]
            )
            _venv.active_venvs_order[:] = saved_order


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
    Build the host app once: install the user loader, import the plugins, run the API plugins,
    mount the web console (the one Dash app, with the plugins' pages) and add the internal routes.
    """
    if _built['app'] is not None:
        return _built['app']

    _require_host_mode()
    from meerschaum.api.host import dash_enabled
    from meerschaum.api import app
    from meerschaum.api.host._auth import install_user_loader
    from meerschaum.api.host import dash as host_dash
    from meerschaum.api.host import _internal

    install_user_loader()

    if load_plugins:
        from meerschaum.plugins import load_plugins as _load_plugins
        _load_plugins(skip_if_loaded=True, debug=debug)

    ### The internal routes come first so a plugin cannot shadow them.
    _internal.register(app)
    run_api_plugins(app, debug=debug)
    ### PARITY: v4's `--no-dash` serves no console and no plugin pages (no Dash app is mounted).
    if dash_enabled():
        with _keep_venv_order():
            if host_dash.mount_console(app, debug=debug):
                host_dash.mount_page_contract_routes(app, debug=debug)
                host_dash.mount_native_page_routes(app)

    _built['app'] = app
    return app


def _import_uvicorn():
    """
    Import uvicorn, or raise an `ImportError` naming the extra that provides it.
    """
    from meerschaum.utils.packages import attempt_import
    uvicorn = attempt_import('uvicorn', lazy=False, check_update=False)
    if uvicorn is None:
        raise ImportError(
            "The API host needs uvicorn: install the `api` extra "
            "(`pip install \"meerschaum[api]\"`), or unset `MRSM_NO_AUTO_INSTALL` "
            "so that Meerschaum installs it into its `mrsm` virtual environment."
        )
    return uvicorn


class ProxyTokenMiddleware:
    """
    A pure ASGI middleware answering `403` (HTTP) or closing the connection (websocket) unless the
    request carries `X-Mrsm-Host-Proxy-Token` equal to `token`. On a TCP port any local process
    can connect; the token (known to the Rust server that started the host) is what makes the
    host reachable only through the proxy.
    """

    def __init__(self, app, token: str):
        self.app = app
        self.token = token.encode('utf-8')

    def _authorized(self, scope) -> bool:
        import hmac
        from meerschaum.api.host import PROXY_TOKEN_HEADER
        for name, value in scope.get('headers', []):
            if name.decode('latin-1').lower() == PROXY_TOKEN_HEADER:
                return hmac.compare_digest(bytes(value), self.token)
        return False

    async def _prove(self, scope, send) -> None:
        """
        Answer the proxy's identity check (`GET /_mrsm/host/proof?nonce=...`): the HMAC-SHA256 of
        the nonce keyed with the token, which only this host and the Rust server know. A local
        process that grabbed the port in the window between the proxy choosing it and this host
        binding it cannot answer, so the proxy never forwards a request (or a token) to it.
        """
        import hashlib
        import hmac
        from urllib.parse import parse_qs
        query = parse_qs(scope.get('query_string', b'').decode('latin-1'))
        nonce = (query.get('nonce') or [''])[0]
        body = hmac.new(self.token, nonce.encode('utf-8'), hashlib.sha256).hexdigest().encode('ascii')
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                (b'content-type', b'text/plain'),
                (b'content-length', str(len(body)).encode('ascii')),
            ],
        })
        await send({'type': 'http.response.body', 'body': body})

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http' and scope.get('path') == '/_mrsm/host/proof':
            await self._prove(scope, send)
            return
        if scope['type'] in ('http', 'websocket') and not self._authorized(scope):
            if scope['type'] == 'websocket':
                await send({'type': 'websocket.close', 'code': 1008})
                return
            body = b'{"detail":"Forbidden."}'
            await send({
                'type': 'http.response.start',
                'status': 403,
                'headers': [
                    (b'content-type', b'application/json'),
                    (b'content-length', str(len(body)).encode('ascii')),
                ],
            })
            await send({'type': 'http.response.body', 'body': body})
            return
        await self.app(scope, receive, send)


def run_tcp(host: str, port: int, debug: bool = False) -> None:
    """
    Serve the host app with uvicorn on a TCP port (the Windows transport); see
    `ProxyTokenMiddleware`.
    """
    from meerschaum.api.host import PROXY_TOKEN_ENV
    from meerschaum.utils.venv import Venv
    with Venv('mrsm', debug=debug):
        uvicorn = _import_uvicorn()
        app = build_app(debug=debug)
        token = os.environ.get(PROXY_TOKEN_ENV, '')
        if not token:
            # Any local process can connect to a TCP port: never serve one without the token.
            raise SystemExit(
                f"The API host on a TCP port needs `{PROXY_TOKEN_ENV}` (the per-start token "
                "the Meerschaum server sets); refusing to serve it unauthenticated."
            )
        served = ProxyTokenMiddleware(app, token)
        uvicorn.run(
            served,
            host=host,
            port=port,
            log_level=('debug' if debug else 'warning'),
            proxy_headers=False,
            lifespan='on',
        )


def run(socket_path: str, debug: bool = False) -> None:
    """
    Serve the host app with uvicorn on a Unix socket.

    As in v4, the whole run happens inside the `mrsm` virtual environment: uvicorn, FastAPI and
    Starlette import modules lazily (`h11`, `python_multipart`, ...) long after `attempt_import()`
    has returned, so the environment holding them must stay on `sys.path` until the server stops.
    """
    from meerschaum.utils.venv import Venv
    with Venv('mrsm', debug=debug):
        uvicorn = _import_uvicorn()
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
