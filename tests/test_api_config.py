#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test configuration-dependent API startup behavior.

The FastAPI app is built at import time from the loaded configuration, so each
scenario boots `meerschaum.api` in a subprocess with `MRSM_CONFIG` overrides.
No databases or API server are required.
"""

import json
import os
import socket
import subprocess
import sys


def _api_boot_output(code: str, config: dict) -> str:
    """
    Run `code` in a fresh interpreter with `config` patched via `MRSM_CONFIG`
    and return its stdout.
    """
    env = os.environ.copy()
    env.pop('MRSM_SERVER_ID', None)
    env['MRSM_CONFIG'] = json.dumps(config)
    result = subprocess.run(
        [sys.executable, '-c', code],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stdout


def _get_closed_port() -> int:
    """
    Return a port on which nothing is listening.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _get_registered_route_paths(config: dict) -> list:
    code = (
        "import json\n"
        "from meerschaum.api import app\n"
        "paths = sorted({getattr(route, 'path', '') for route in app.routes})\n"
        "print('ROUTES=' + json.dumps(paths))\n"
    )
    stdout = _api_boot_output(code, config)
    for line in stdout.splitlines():
        if line.startswith('ROUTES='):
            return json.loads(line[len('ROUTES='):])
    raise AssertionError(f"No routes printed:\n{stdout}")


def test_unreachable_cache_connector_degrades_to_none():
    """
    A configured but unreachable cache connector must resolve to `None`
    (the documented no-cache path), not to a connector whose every
    operation fails at the call site.
    """
    config = {
        'api': {
            'uvicorn': {'production': True, 'no_dash': True},
            'cache': {'connector': 'valkey:test_probe'},
        },
        'system': {'experimental': {'valkey_session_cache': True}},
        'meerschaum': {
            'connectors': {
                'valkey': {
                    'test_probe': {'host': '127.0.0.1', 'port': _get_closed_port()},
                },
            },
        },
    }
    code = (
        "from meerschaum.api import get_cache_connector\n"
        "print('CACHE_CONNECTOR=' + repr(get_cache_connector()))\n"
    )
    stdout = _api_boot_output(code, config)
    assert 'CACHE_CONNECTOR=None' in stdout


def test_routes_allowlist_restricts_core_routes():
    """
    With `api:permissions:routes:allowlist` set, disallowed core route groups
    must never be registered; `/login` and allowed groups remain served.
    """
    config = {
        'api': {
            'uvicorn': {'no_dash': True},
            'permissions': {'routes': {'allowlist': ['version']}},
        },
    }
    paths = _get_registered_route_paths(config)
    assert '/login' in paths
    assert '/version' in paths
    for prefix in (
        '/actions', '/connectors', '/jobs', '/mcp',
        '/pipes', '/plugins', '/tokens', '/users',
    ):
        assert not any(
            path == prefix or path.startswith(prefix + '/')
            for path in paths
        ), f"Disallowed route group '{prefix}' is still registered."


def test_routes_allowlist_default_serves_core_routes():
    """
    The default allowlist (`['*']`) must register every core route group.
    """
    config = {'api': {'uvicorn': {'no_dash': True}}}
    paths = _get_registered_route_paths(config)
    for prefix in ('/actions', '/connectors', '/jobs', '/pipes', '/users', '/version'):
        assert any(
            path == prefix or path.startswith(prefix + '/')
            for path in paths
        ), f"Core route group '{prefix}' is not registered by default."


def _get_console_page_paths(config: dict) -> list:
    code = (
        "import json\n"
        "import meerschaum.api\n"
        "from meerschaum.api.dash.callbacks.dashboard import _paths\n"
        "print('PATHS=' + json.dumps(sorted(_paths)))\n"
    )
    stdout = _api_boot_output(code, config)
    for line in stdout.splitlines():
        if line.startswith('PATHS='):
            return json.loads(line[len('PATHS='):])
    raise AssertionError(f"No console paths printed:\n{stdout}")


def test_routes_allowlist_dash_group_gates_console_pages():
    """
    An allowlist without the `dash` group must strip the web console's own
    pages while keeping the login page (the auth entrypoint for plugin pages).
    """
    config = {
        'api': {
            'permissions': {'routes': {'allowlist': ['version']}},
        },
    }
    paths = _get_console_page_paths(config)
    assert '/dash/login' in paths
    for console_path in ('/dash', '/dash/pipes', '/dash/users', '/dash/jobs', '/dash/tokens'):
        assert console_path not in paths, (
            f"Console page '{console_path}' is still served under a restricted allowlist."
        )


def test_routes_allowlist_default_serves_console_pages():
    """
    The default allowlist (`['*']`) must serve the web console's own pages.
    """
    config = {'api': {}}
    paths = _get_console_page_paths(config)
    for console_path in ('/dash/login', '/dash', '/dash/pipes', '/dash/users', '/dash/jobs'):
        assert console_path in paths, f"Console page '{console_path}' is not served by default."


def test_routes_allowlist_empty_denies_core_routes():
    """
    An explicitly empty allowlist (`[]`) must register no core route groups
    (only a missing allowlist falls back to `['*']`); `/login` stays served.
    """
    config = {
        'api': {
            'uvicorn': {'no_dash': True},
            'permissions': {'routes': {'allowlist': []}},
        },
    }
    paths = _get_registered_route_paths(config)
    assert '/login' in paths
    for prefix in (
        '/actions', '/connectors', '/jobs', '/mcp',
        '/pipes', '/plugins', '/tokens', '/users', '/version',
    ):
        assert not any(
            path == prefix or path.startswith(prefix + '/')
            for path in paths
        ), f"Core route group '{prefix}' is registered under an empty allowlist."


def test_startup_event_survives_empty_allowlist():
    """
    The startup event normalizes the OpenAPI schema; with a restricted
    allowlist the generated schema may lack `components.securitySchemes`,
    which must not crash the boot (the reported production crash loop).
    """
    config = {
        'api': {
            'uvicorn': {'no_dash': True},
            'permissions': {'routes': {'allowlist': []}},
        },
    }
    code = (
        "import asyncio\n"
        "import meerschaum.api._events as events\n"
        "asyncio.run(events.startup())\n"
        "from meerschaum.jobs import stop_check_jobs_thread\n"
        "stop_check_jobs_thread()\n"
        "from meerschaum.api import app\n"
        "schemes = app.openapi_schema['components']['securitySchemes']\n"
        "assert 'OAuth2PasswordBearer' in schemes and 'APIKey' in schemes\n"
        "print('STARTUP_OK')\n"
    )
    stdout = _api_boot_output(code, config)
    assert 'STARTUP_OK' in stdout
