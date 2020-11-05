#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define file paths
"""
from pathlib import Path
import os, platform
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

CONFIG_ROOT_PATH = Path(os.path.join(Path.home(), '.config', 'meerschaum'))
if platform.system() == 'Windows':
    CONFIG_ROOT_PATH = Path(os.path.join(os.environ['AppData'], 'Meerschaum'))

### bootstrap the root
CONFIG_ROOT_PATH.mkdir(parents=True, exist_ok=True)

package_root_context_manager = pkg_resources.path('meerschaum', '__init__.py')
with package_root_context_manager as file_path:
    PACKAGE_ROOT_PATH = Path(os.path.join(Path(file_path.parent.parent), 'meerschaum'))

### file path of the resources package
#  RESOURCES_PATH = Path(os.path.join(CONFIG_ROOT_PATH, 'config', 'resources'))
RESOURCES_PATH = CONFIG_ROOT_PATH

CONFIG_FILENAME = "config.yaml"
CONFIG_PATH = Path(os.path.join(RESOURCES_PATH, CONFIG_FILENAME))

#  CONNECTORS_FILENAME = ""
#  CONNECTORS_PATH = Path(os.path.join(RESOURCES_PATH, CONNECTORS_FILENAME))

DEFAULT_CONFIG_FILENAME = "default_config.yaml"
DEFAULT_CONFIG_PATH = Path(os.path.join(CONFIG_ROOT_PATH, DEFAULT_CONFIG_FILENAME))

PATCH_FILENAME = "patch.yaml"
PATCH_PATH = Path(os.path.join(RESOURCES_PATH, PATCH_FILENAME))

PERMANENT_PATCH_FILENAME = "permanent_patch.yaml"
PERMANENT_PATCH_PATH = Path(os.path.join(RESOURCES_PATH, PERMANENT_PATCH_FILENAME))

GRAFANA_RESOURCES_PATH = Path(os.path.join(CONFIG_ROOT_PATH, 'stack', 'grafana', 'resources'))
GRAFANA_DATASOURCE_PATH = Path(os.path.join(GRAFANA_RESOURCES_PATH, 'provisioning', 'datasources', 'datasource.yaml'))
GRAFANA_DASHBOARD_PATH = Path(os.path.join(GRAFANA_RESOURCES_PATH, 'provisioning', 'dashboards', 'dashboard.yaml'))
#  GRAFANA_INI_PATH = Path(os.path.join(GRAFANA_RESOURCES_PATH, 'grafana.ini'))

MOSQUITTO_RESOURCES_PATH = Path(os.path.join(CONFIG_ROOT_PATH, 'stack', 'mosquitto', 'resources'))
MOSQUITTO_CONFIG_PATH = Path(os.path.join(MOSQUITTO_RESOURCES_PATH, 'mosquitto.conf'))

STACK_RESOURCES_PATH = Path(os.path.join(CONFIG_ROOT_PATH, 'stack'))
STACK_COMPOSE_FILENAME = "docker-compose.yaml"
STACK_COMPOSE_PATH = Path(os.path.join(STACK_RESOURCES_PATH, STACK_COMPOSE_FILENAME))
STACK_ENV_FILENAME = ".env"
STACK_ENV_PATH = Path(os.path.join(STACK_RESOURCES_PATH, STACK_ENV_FILENAME))

#  SHELL_RESOURCES_PATH = Path(os.path.join(CONFIG_ROOT_PATH, 'actions', 'shell', 'resources'))
SHELL_RESOURCES_PATH = RESOURCES_PATH
SHELL_HISTORY_PATH = Path(os.path.join(SHELL_RESOURCES_PATH, '.mrsm_history'))

API_RESOURCES_PATH = Path(os.path.join(PACKAGE_ROOT_PATH, 'api', 'resources'))
API_STATIC_PATH = Path(os.path.join(API_RESOURCES_PATH, 'static'))
API_TEMPLATES_PATH = Path(os.path.join(API_RESOURCES_PATH, 'templates'))

API_UVICORN_RESOURCES_PATH = Path(os.path.join(RESOURCES_PATH, 'uvicorn'))
API_UVICORN_CONFIG_PATH = Path(os.path.join(API_UVICORN_RESOURCES_PATH, '.thread_config.yaml'))

PIPES_RESOURCES_PATH = Path(os.path.join(RESOURCES_PATH, 'pipes'))

SPLITGRAPH_CONFIG_PATH = Path(os.path.join(Path.home(), '.splitgraph', '.sgconfig'))

### NOTE: This must be the bottom of the module
paths_glob = dict(globals())
for var, path in paths_glob.items():
    if 'RESOURCES_PATH' in var:
        path.mkdir(parents=True, exist_ok=True)
