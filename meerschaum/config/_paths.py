#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define file paths
"""
from pathlib import Path
import os
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

root_context_manager = pkg_resources.path('meerschaum', '__init__.py')
with root_context_manager as file_path:
    ROOT_PATH = Path(file_path.parent.parent)

### file path of the resources package
RESOURCES_PATH = Path(os.path.join(ROOT_PATH, 'meerschaum', 'config', 'resources'))

CONFIG_FILENAME = "config.yaml"
CONFIG_PATH = Path(os.path.join(RESOURCES_PATH, CONFIG_FILENAME))

DEFAULT_CONFIG_FILENAME = "default_config.yaml"
DEFAULT_CONFIG_PATH = Path(os.path.join(RESOURCES_PATH, DEFAULT_CONFIG_FILENAME))

PATCH_FILENAME = "patch.yaml"
PATCH_PATH = Path(os.path.join(RESOURCES_PATH, PATCH_FILENAME))

GRAFANA_RESOURCES_PATH = Path(os.path.join(ROOT_PATH, 'meerschaum', 'config', 'stack', 'grafana', 'resources'))
GRAFANA_DATASOURCE_PATH = Path(os.path.join(GRAFANA_RESOURCES_PATH, 'provisioning', 'datasources', 'datasource.yaml'))
GRAFANA_DASHBOARD_PATH = Path(os.path.join(GRAFANA_RESOURCES_PATH, 'provisioning', 'dashboards', 'dashboard.yaml'))

STACK_RESOURCES_PATH = Path(os.path.join(ROOT_PATH, 'meerschaum', 'config', 'stack', 'resources'))
STACK_COMPOSE_FILENAME = "docker-compose.yaml"
STACK_COMPOSE_PATH = Path(os.path.join(STACK_RESOURCES_PATH, STACK_COMPOSE_FILENAME))
STACK_ENV_FILENAME = ".env"
STACK_ENV_PATH = Path(os.path.join(STACK_RESOURCES_PATH, STACK_ENV_FILENAME))

SHELL_RESOURCES_PATH = Path(os.path.join(ROOT_PATH, 'meerschaum', 'actions', 'shell', 'resources'))
SHELL_HISTORY_PATH = Path(os.path.join(SHELL_RESOURCES_PATH, '.mrsm_history'))

API_RESOURCES_PATH = Path(os.path.join(ROOT_PATH, 'meerschaum', 'api', 'resources'))
API_STATIC_PATH = Path(os.path.join(API_RESOURCES_PATH, 'static'))
API_TEMPLATES_PATH = Path(os.path.join(API_RESOURCES_PATH, 'templates'))
