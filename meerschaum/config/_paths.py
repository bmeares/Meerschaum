#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define file paths
"""

from __future__ import annotations

from pathlib import Path
import os, platform
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

package_root_context_manager = pkg_resources.path('meerschaum', '__init__.py')
with package_root_context_manager as file_path:
    PACKAGE_ROOT_PATH = Path(os.path.join(Path(file_path.parent.parent), 'meerschaum'))

paths = {
    'PACKAGE_ROOT_PATH' : PACKAGE_ROOT_PATH,
    'ROOT_DIR_PATH' : (
        Path(os.path.join(Path.home(), '.config', 'meerschaum'))
        if platform.system() != 'Windows'
        else Path(os.path.join(os.environ['AppData'], 'Meerschaum'))
    ),
    'VIRTENV_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'venvs'),
    'CONFIG_DIR_PATH' : ('{ROOT_DIR_PATH}', 'config'),
    'DEFAULT_CONFIG_DIR_PATH' : ('{ROOT_DIR_PATH}', 'default_config'),
    'PATCH_DIR_PATH' : ('{ROOT_DIR_PATH}', 'patch_config'),
    'PERMANENT_PATCH_DIR_PATH' : ('{ROOT_DIR_PATH}', 'permanent_patch_config'),

    'STACK_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'stack'),
    'STACK_COMPOSE_FILENAME' : 'docker-compose.yaml',
    'STACK_COMPOSE_PATH' : ('{STACK_RESOURCES_PATH}', '{STACK_COMPOSE_FILENAME}'),
    'STACK_ENV_FILENAME' : '.env',
    'STACK_ENV_PATH' : ('{STACK_RESOURCES_PATH}', '{STACK_ENV_FILENAME}'),

    'SHELL_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', ),
    'SHELL_HISTORY_PATH' : ('{SHELL_RESOURCES_PATH}', '.mrsm_history'),

    'API_RESOURCES_PATH' : ('{PACKAGE_ROOT_PATH}', 'api', 'resources'),
    'API_STATIC_PATH' : ('{API_RESOURCES_PATH}', 'static'),
    'API_TEMPLATES_PATH' : ('{API_RESOURCES_PATH}', 'templates'),
    'API_CONFIG_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'api'),
    'API_SECRET_KEY_PATH' : ('{API_CONFIG_RESOURCES_PATH}', '.api_secret_key'),
    'API_UVICORN_RESOURCES_PATH' : ('{API_CONFIG_RESOURCES_PATH}', 'uvicorn'),
    'API_UVICORN_CONFIG_PATH' : ('{API_UVICORN_RESOURCES_PATH}', '.thread_config.json'),

    'CACHE_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', '.cache'),
    'PIPES_CACHE_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'pipes'),
    'USERS_CACHE_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'users'),

    'PLUGINS_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'plugins'),
    'PLUGINS_ARCHIVES_RESOURCES_PATH' : ('{PLUGINS_RESOURCES_PATH}', '.archives'),
    'PLUGINS_TEMP_RESOURCES_PATH' : ('{PLUGINS_RESOURCES_PATH}', '.tmp'),
    'PLUGINS_INIT_PATH' : ('{PLUGINS_RESOURCES_PATH}', '__init__.py'),

    'SQLITE_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'sqlite'),
    'SQLITE_DB_PATH' : ('{SQLITE_RESOURCES_PATH}', 'mrsm_local.db'),

    'DUCKDB_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'duckdb'),
    'DUCKDB_PATH' : ('{DUCKDB_RESOURCES_PATH}', 'duck.db'),

    'GRAFANA_RESOURCES_PATH' : ('{STACK_RESOURCES_PATH}', 'grafana', 'resources'),
    'GRAFANA_DATASOURCE_PATH' : (
        '{GRAFANA_RESOURCES_PATH}', 'provisioning', 'datasources', 'datasource.yaml'
    ),
    'GRAFANA_DASHBOARD_PATH' : (
        '{GRAFANA_RESOURCES_PATH}', 'provisioning', 'dashboards', 'dashboard.yaml'
    ),
    'MOSQUITTO_RESOURCES_PATH' : ('{STACK_RESOURCES_PATH}', 'mosquitto', 'resources'),
    'MOSQUITTO_CONFIG_PATH' : ('{MOSQUITTO_RESOURCES_PATH}', 'mosquitto.conf'),

    'PORTABLE_CHECK_READLINE_PATH' : ('{SHELL_RESOURCES_PATH}', '.readline_attempted_install'),

    'DAEMON_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'jobs'),
    'LOGS_RESOURCES_PATH' : ('{ROOT_DIR_PATH}', 'logs'),
}

def set_root(root : Union[Path, str]):
    """
    Modify the value of `ROOT_DIR_PATH`.
    """
    global paths
    paths['ROOT_DIR_PATH'] = Path(root)

def __getattr__(name : str) -> Path:
    if name not in paths:
        if name not in globals():
            raise AttributeError(f"Could not import '{name}'.")
        return globals()[name]

    if isinstance(paths[name], (list, tuple)):
        ### recurse through paths to create resource directories.
        parts = []
        for p in paths[name]:
            if str(p).startswith('{') and str(p).endswith('}'):
                parts.append(__getattr__(p[1:-1]))
                #  parts.append(paths[p[1:-1]])
            else:
                parts.append(p)
        path = Path(os.path.join(*parts))
    else:
        path = Path(paths[name])

    ### Create directories or touch files.
    if 'RESOURCES_PATH' in name or name == 'CONFIG_DIR_PATH':
        path.mkdir(parents=True, exist_ok=True)
    elif 'FILENAME' in name:
        path = str(path)

    return path

