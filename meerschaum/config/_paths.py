#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define file paths
"""

from __future__ import annotations

from pathlib import Path
import os, platform, sys, json
from meerschaum.utils.typing import Union
from meerschaum.config.static import STATIC_CONFIG

DOT_CONFIG_DIR_PATH = Path(
    os.environ.get('XDG_CONFIG_HOME', Path.home() / '.config')
    if platform.system() != 'Windows'
    else os.environ.get('AppData', Path.home() / 'AppData' / 'Roaming')
)

DEFAULT_ROOT_DIR_PATH = (
    (DOT_CONFIG_DIR_PATH / 'meerschaum')
    if platform.system() != 'Windows'
    else (DOT_CONFIG_DIR_PATH / 'Meerschaum')
)


ENVIRONMENT_ROOT_DIR = STATIC_CONFIG['environment']['root']
if ENVIRONMENT_ROOT_DIR in os.environ:
    _ROOT_DIR_PATH = Path(os.environ[ENVIRONMENT_ROOT_DIR]).resolve()
    if not _ROOT_DIR_PATH.exists():
        print(
            f"Invalid root directory '{str(_ROOT_DIR_PATH)}' set for " +
            f"environment variable '{ENVIRONMENT_ROOT_DIR}'.\n" +
            f"Please enter a valid path for {ENVIRONMENT_ROOT_DIR}.",
            file=sys.stderr,
        )
        sys.exit(1)
else:
    _ROOT_DIR_PATH = DEFAULT_ROOT_DIR_PATH


ENVIRONMENT_CONFIG_DIR = STATIC_CONFIG['environment']['config_dir']
if ENVIRONMENT_CONFIG_DIR in os.environ:
    _CONFIG_DIR_PATH = Path(os.environ[ENVIRONMENT_CONFIG_DIR]).resolve()
    if not _CONFIG_DIR_PATH.exists():
        print(
            (
                f"Invalid configuration directory '{_CONFIG_DIR_PATH}' set"
                + f" for environment variable '{ENVIRONMENT_CONFIG_DIR}'\n"
                + f"Please enter a valid path for {ENVIRONMENT_CONFIG_DIR}."
            ),
            file=sys.stderr,
        )
        sys.exit(1)
else:
    _CONFIG_DIR_PATH = _ROOT_DIR_PATH / 'config'

ENVIRONMENT_PLUGINS_DIR = STATIC_CONFIG['environment']['plugins']
if ENVIRONMENT_PLUGINS_DIR in os.environ:
    try:
        PLUGINS_DIR_PATHS = (
            [
                Path(path).resolve()
                for path in json.loads(os.environ[ENVIRONMENT_PLUGINS_DIR])
            ] if os.environ[ENVIRONMENT_PLUGINS_DIR].lstrip().startswith('[')
            else [
                Path(path_str).resolve()
                for path_str in os.environ[ENVIRONMENT_PLUGINS_DIR].split(':')
                if path_str
            ]
        )
    except Exception as e:
        PLUGINS_DIR_PATHS = []

    if not PLUGINS_DIR_PATHS:
        print(
            "Invalid plugins directories set for "
            f"environment variable '{ENVIRONMENT_PLUGINS_DIR}'.\n\n"
            f"Set this to a colon-separated path string:\n\n"
            f"`export {ENVIRONMENT_PLUGINS_DIR}=./plugins:/another/path/to/plugins`\n\n"
            "or a JSON-encoded path list:\n\n"
            f"`export {ENVIRONMENT_PLUGINS_DIR}=" + "'[\"./plugins\", \"/another/path/to/plugins\"]'`"
            f"",
        )
        sys.exit(1)
else:
    PLUGINS_DIR_PATHS = [_ROOT_DIR_PATH / 'plugins']

### Remove duplicate plugins paths.
_seen_plugins_paths, _plugins_paths_to_remove = set(), set()
for _plugin_path in PLUGINS_DIR_PATHS:
    if _plugin_path in _seen_plugins_paths:
        _plugins_paths_to_remove.add(_plugin_path)
    _seen_plugins_paths.add(_plugin_path)
for _plugin_path in _plugins_paths_to_remove:
    PLUGINS_DIR_PATHS.remove(_plugin_path)

ENVIRONMENT_VENVS_DIR = STATIC_CONFIG['environment']['venvs']
if ENVIRONMENT_VENVS_DIR in os.environ:
    _VENVS_DIR_PATH = Path(os.environ[ENVIRONMENT_VENVS_DIR]).resolve()
    if not _VENVS_DIR_PATH.exists():
        try:
            _VENVS_DIR_PATH.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(
                f"Invalid path set for environment variable '{ENVIRONMENT_VENVS_DIR}':\n"
                + f"{_VENVS_DIR_PATH}"
            )
            _VENVS_DIR_PATH = (_ROOT_DIR_PATH / 'venvs').resolve()
            print(f"Will use the following path for venvs instead:\n{_VENVS_DIR_PATH}")
else:
    _VENVS_DIR_PATH = _ROOT_DIR_PATH / 'venvs'

paths = {
    'PACKAGE_ROOT_PATH'              : Path(__file__).parent.parent.resolve().as_posix(),
    'ROOT_DIR_PATH'                  : _ROOT_DIR_PATH.as_posix(),
    'VIRTENV_RESOURCES_PATH'         : _VENVS_DIR_PATH.as_posix(),
    'CONFIG_DIR_PATH'                : _CONFIG_DIR_PATH.as_posix(),
    'DEFAULT_CONFIG_DIR_PATH'        : ('{ROOT_DIR_PATH}', 'default_config'),
    'PATCH_DIR_PATH'                 : ('{ROOT_DIR_PATH}', 'patch_config'),
    'PERMANENT_PATCH_DIR_PATH'       : ('{ROOT_DIR_PATH}', 'permanent_patch_config'),
    'INTERNAL_RESOURCES_PATH'        : ('{ROOT_DIR_PATH}', '.internal'),

    'UPDATES_RESOURCES_PATH'         : ('{INTERNAL_RESOURCES_PATH}', 'updates'),
    'UPDATES_CACHE_PATH'             : ('{UPDATES_RESOURCES_PATH}', 'cache.json'),
    'UPDATES_LOCK_PATH'              : ('{UPDATES_RESOURCES_PATH}', '.updates.lock'),

    'STACK_RESOURCES_PATH'           : ('{ROOT_DIR_PATH}', 'stack'),
    'STACK_COMPOSE_FILENAME'         : 'docker-compose.yaml',
    'STACK_COMPOSE_PATH'             : ('{STACK_RESOURCES_PATH}', '{STACK_COMPOSE_FILENAME}'),
    'STACK_ENV_FILENAME'             : '.env',
    'STACK_ENV_PATH'                 : ('{STACK_RESOURCES_PATH}', '{STACK_ENV_FILENAME}'),

    'SHELL_RESOURCES_PATH'           : ('{ROOT_DIR_PATH}', ),
    'SHELL_HISTORY_PATH'             : ('{SHELL_RESOURCES_PATH}', '.mrsm_history'),
    'PYTHON_RESOURCES_PATH'          : ('{INTERNAL_RESOURCES_PATH}', 'python'),

    'API_RESOURCES_PATH'             : ('{PACKAGE_ROOT_PATH}', 'api', 'resources'),
    'API_STATIC_PATH'                : ('{API_RESOURCES_PATH}', 'static'),
    'API_TEMPLATES_PATH'             : ('{API_RESOURCES_PATH}', 'templates'),
    'API_CONFIG_RESOURCES_PATH'      : ('{ROOT_DIR_PATH}', 'api'),
    'API_SECRET_KEY_PATH'            : ('{API_CONFIG_RESOURCES_PATH}', '.api_secret_key'),
    'API_UVICORN_RESOURCES_PATH'     : ('{API_CONFIG_RESOURCES_PATH}', 'uvicorn'),
    'API_UVICORN_CONFIG_PATH'        : ('{API_UVICORN_RESOURCES_PATH}', '.thread_config.json'),

    'CACHE_RESOURCES_PATH'           : ('{ROOT_DIR_PATH}', '.cache'),
    'PIPES_CACHE_RESOURCES_PATH'     : ('{CACHE_RESOURCES_PATH}', 'pipes'),
    'USERS_CACHE_RESOURCES_PATH'     : ('{CACHE_RESOURCES_PATH}', 'users'),

    'PLUGINS_RESOURCES_PATH'         : ('{INTERNAL_RESOURCES_PATH}', 'plugins'),
    'PLUGINS_INTERNAL_LOCK_PATH'     : ('{INTERNAL_RESOURCES_PATH}', 'plugins.lock'),
    'PLUGINS_PACKAGES_INTERNAL_PATH' : ('{INTERNAL_RESOURCES_PATH}', 'packaged_plugins'),
    'PLUGINS_ARCHIVES_RESOURCES_PATH': ('{PLUGINS_RESOURCES_PATH}', '.archives'),
    'PLUGINS_TEMP_RESOURCES_PATH'    : ('{PLUGINS_RESOURCES_PATH}', '.tmp'),
    'PLUGINS_INIT_PATH'              : ('{PLUGINS_RESOURCES_PATH}', '__init__.py'),

    'SQLITE_RESOURCES_PATH'          : ('{ROOT_DIR_PATH}', 'sqlite'),
    'SQLITE_DB_PATH'                 : ('{SQLITE_RESOURCES_PATH}', 'mrsm_local.db'),

    'BACKUP_RESOURCES_PATH'          : ('{ROOT_DIR_PATH}', 'backup'),
    'BACKUP_DATABASE_PATH'           : ('{BACKUP_RESOURCES_PATH}', 'backup.sql'),

    'DUCKDB_RESOURCES_PATH'          : ('{ROOT_DIR_PATH}', 'duckdb'),
    'DUCKDB_PATH'                    : ('{DUCKDB_RESOURCES_PATH}', 'duck.db'),

    'GRAFANA_RESOURCES_PATH'         : ('{STACK_RESOURCES_PATH}', 'grafana', 'resources'),
    'GRAFANA_DATASOURCE_PATH'        : (
        '{GRAFANA_RESOURCES_PATH}', 'provisioning', 'datasources', 'datasource.yaml'
    ),
    'GRAFANA_DASHBOARD_PATH'         : (
        '{GRAFANA_RESOURCES_PATH}', 'provisioning', 'dashboards', 'dashboard.yaml'
    ),
    'MOSQUITTO_RESOURCES_PATH'       : ('{STACK_RESOURCES_PATH}', 'mosquitto', 'resources'),
    'MOSQUITTO_CONFIG_PATH'          : ('{MOSQUITTO_RESOURCES_PATH}', 'mosquitto.conf'),

    'PORTABLE_CHECK_READLINE_PATH'   : ('{SHELL_RESOURCES_PATH}', '.readline_attempted_install'),

    'DAEMON_RESOURCES_PATH'          : ('{ROOT_DIR_PATH}', 'jobs'),
    'LOGS_RESOURCES_PATH'            : ('{ROOT_DIR_PATH}', 'logs'),
    'DAEMON_ERROR_LOG_PATH'          : ('{ROOT_DIR_PATH}', 'daemon_errors.log'),
    'CHECK_JOBS_LOCK_PATH'           : ('{INTERNAL_RESOURCES_PATH}', 'check-jobs.lock'),
    
    'SYSTEMD_RESOURCES_PATH'         : ('{DOT_CONFIG_DIR_PATH}', 'systemd'),
    'SYSTEMD_USER_RESOURCES_PATH'    : ('{SYSTEMD_RESOURCES_PATH}', 'user'),
    'SYSTEMD_ROOT_RESOURCES_PATH'    : ('{ROOT_DIR_PATH}', 'systemd'),
    'SYSTEMD_JOBS_RESOURCES_PATH'    : ('{SYSTEMD_ROOT_RESOURCES_PATH}', 'services'),
    'SYSTEMD_LOGS_RESOURCES_PATH'    : ('{SYSTEMD_ROOT_RESOURCES_PATH}', 'logs'),
}

def set_root(root: Union[Path, str]):
    """Modify the value of `ROOT_DIR_PATH`."""
    paths['ROOT_DIR_PATH'] = Path(root).resolve()
    for path_name, path_parts in paths.items():
        if isinstance(path_parts, tuple) and path_parts[0] == '{ROOT_DIR_PATH}':
            globals()[path_name] = __getattr__(path_name)


def __getattr__(name: str) -> Path:
    if name not in paths:
        if name not in globals():
            raise AttributeError(f"Could not import '{name}'.")
        return globals()[name]

    if isinstance(paths[name], (list, tuple)) and len(paths[name]) > 0:
        ### recurse through paths to create resource directories.
        parts = []
        for p in paths[name]:
            if str(p).startswith('{') and str(p).endswith('}'):
                parts.append(__getattr__(p[1:-1]))
            else:
                parts.append(p)
        path = Path(os.path.join(*parts))
    else:
        path = Path(paths[name])

    ### Create directories or touch files.
    if name.endswith('RESOURCES_PATH') or name == 'CONFIG_DIR_PATH':
        path.mkdir(parents=True, exist_ok=True)
    elif 'FILENAME' in name:
        path = path.as_posix()

    return path
