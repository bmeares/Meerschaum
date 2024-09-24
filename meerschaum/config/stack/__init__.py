#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Docker Compose stack configuration goes here
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, List, Any, SuccessTuple, Dict

import os
import json
from meerschaum.config._paths import (
    GRAFANA_DATASOURCE_PATH,
    GRAFANA_DASHBOARD_PATH,
    ROOT_DIR_PATH,
)
from meerschaum.config._paths import STACK_COMPOSE_FILENAME, STACK_ENV_FILENAME
from meerschaum.config._paths import CONFIG_DIR_PATH, STACK_ENV_PATH, STACK_COMPOSE_PATH
from meerschaum.config._paths import GRAFANA_DATASOURCE_PATH, GRAFANA_DASHBOARD_PATH

db_port = "MRSM{meerschaum:connectors:sql:main:port}"
db_user = "MRSM{meerschaum:connectors:sql:main:username}"
db_pass = "MRSM{meerschaum:connectors:sql:main:password}"
db_base = "MRSM{meerschaum:connectors:sql:main:database}"

### default localhost, db for docker network
db_hostname = "db"
db_host = 'MRSM{stack:' + str(STACK_COMPOSE_FILENAME) + ':services:db:hostname}'
api_port = "MRSM{meerschaum:connectors:api:main:port}"
api_host = "api"

valkey_hostname = "valkey"
valkey_host = 'MRSM{stack:' + str(STACK_COMPOSE_FILENAME) + ':services:valkey:hostname}'
valkey_port = "MRSM{meerschaum:connectors:valkey:main:port}"
valkey_username = 'MRSM{meerschaum:connectors:valkey:main:username}'
valkey_password = 'MRSM{meerschaum:connectors:valkey:main:password}'

env_dict = {
    'COMPOSE_PROJECT_NAME': 'mrsm',
    'TIMESCALEDB_VERSION': 'latest-pg16-oss',
    'POSTGRES_USER': db_user,
    'POSTGRES_PASSWORD': db_pass,
    'POSTGRES_DB': db_base,
    'VALKEY_USERNAME': valkey_username,
    'VALKEY_PASSWORD': valkey_password,
    'MEERSCHAUM_API_HOSTNAME': api_host,
    'ALLOW_IP_RANGE': '0.0.0.0/0',
    'MEERSCHAUM_API_CONFIG_RESOURCES': '/meerschaum',
}
### apply patch to host config to change hostname to the Docker service name
env_dict['MEERSCHAUM_API_CONFIG'] = json.dumps(
    {
        'meerschaum': 'MRSM{!meerschaum}',
        'system': 'MRSM{!system}',
    },
    indent=4,
).replace(
    '"MRSM{!system}"', 'MRSM{!system}'
).replace(
    '"MRSM{!meerschaum}"', 'MRSM{!meerschaum}',
)

volumes = {
    'api_root': '/meerschaum',
    'meerschaum_db_data': '/var/lib/postgresql/data',
    'grafana_storage': '/var/lib/grafana',
    'valkey_data': '/bitnami/valkey/data',
}
networks = {
    'frontend': None,
    'backend': None,
}
env_dict['MEERSCHAUM_API_PATCH'] = json.dumps(
    {
        'meerschaum': {
            'connectors': {
                'sql': {
                    'main': {
                        'host': db_host,
                        'port': 5432,
                    },
                    'local': {
                        'database': volumes['api_root'] + '/sqlite/mrsm_local.db',
                    },
                },
                'valkey': {
                    'main': {
                        'host': valkey_host,
                        'port': 6379,
                    },
                },
            },
        },
    },
    indent=4,
)

compose_header = """
##############################################################
#                                                            #
#                   DO NOT EDIT THIS FILE!                   #
#                                                            #
#          Any changes you make will be overwritten.         #
#                                                            #
# Instead, you can change this file's configuration with     #
# `edit config stack` under the docker-compose.yaml section. #
#                                                            #
##############################################################
"""


default_docker_compose_config = {
    'services': {
        'db': {
            'environment': {
                'TIMESCALEDB_TELEMETRY': 'off',
                'POSTGRES_USER': '<DOLLAR>POSTGRES_USER',
                'POSTGRES_DB': '<DOLLAR>POSTGRES_DB',
                'POSTGRES_PASSWORD': '<DOLLAR>POSTGRES_PASSWORD',
                'ALLOW_IP_RANGE': env_dict['ALLOW_IP_RANGE'],
            },
            'command': 'postgres -c max_connections=1000 -c shared_buffers=1024MB',
            'healthcheck': {
                'test': [
                    'CMD-SHELL', 'pg_isready -d <DOLLAR>POSTGRES_DB -U <DOLLAR>POSTGRES_USER',
                ],
                'interval': '5s',
                'timeout': '3s',
                'retries': 5
            },
            'restart': 'always',
            'image': 'timescale/timescaledb:' + env_dict['TIMESCALEDB_VERSION'],
            'ports': [
                f'{db_port}:5432',
            ],
            'hostname': db_hostname,
            'volumes': [
                'meerschaum_db_data:' + volumes['meerschaum_db_data'],
            ],
            'shm_size': '1024m',
            'networks': [
                'backend',
            ],
        },
        'api': {
            'image': 'bmeares/meerschaum:api',
            'ports': [f'{api_port}:{api_port}'],
            'hostname': f'{api_host}',
            'networks': [
                'frontend',
                'backend',
            ],
            'command': f'start api --production --port {api_port}',
            'healthcheck': {
                'test': [
                    'CMD', 'curl', '--fail', f'http://localhost:{api_port}/healthcheck',
                ],
                'interval': '5s',
                'timeout': '3s',
                'retries': 3
            },
            'environment': {
                'MRSM_CONFIG': env_dict['MEERSCHAUM_API_CONFIG'],
                'MRSM_PATCH': env_dict['MEERSCHAUM_API_PATCH'],
            },
            'restart': 'always',
            'init': True,
            'depends_on': {
                'db': {
                    'condition': 'service_healthy',
                },
                'valkey': {
                    'condition': 'service_healthy',
                },
            },
            'volumes': [
                'api_root:' + volumes['api_root'],
            ],
        },
        'valkey': {
            'image': 'bitnami/valkey:latest',
            'restart': 'always',
            'environment': {
                'VALKEY_PASSWORD': '<DOLLAR>VALKEY_PASSWORD',
                'VALKEY_RDB_POLICY_DISABLED': 'no',
                'VALKEY_RDB_POLICY': '900#1 600#5 300#10 120#50 60#1000 30#10000',
            },
            'hostname': valkey_hostname,
            'ports': [
                f'{valkey_port}:6379',
            ],
            'volumes': [
                'valkey_data:' + volumes['valkey_data'],
            ],
            'healthcheck': {
                'test': [
                    'CMD', 'valkey-cli', 'ping',
                ],
                'interval': '5s',
                'timeout': '3s',
                'retries': 5,
            },
            'networks': [
                'backend',
            ],
        },
        'grafana': {
            'image': 'grafana/grafana:latest',
            'ports': [
                '3000:3000',
            ],
            'networks': [
                'frontend',
                'backend',
            ],
            'restart': 'always',
            'depends_on': {
                'db': {
                    'condition': 'service_healthy',
                },
            },
            'volumes': [
                'grafana_storage' + ':' + volumes['grafana_storage'],
                ### NOTE: Mount with the 'z' option for SELinux.
                f'{GRAFANA_DATASOURCE_PATH.parent}:/etc/grafana/provisioning/datasources:z,ro',
                f'{GRAFANA_DASHBOARD_PATH.parent}:/etc/grafana/provisioning/dashboards:z,ro',
            ],
            'environment': {
                'GF_SECURITY_ALLOW_EMBEDDING': 'true',
                'GF_ANALYTICS_REPORTING_ENABLED': 'false',
                'GF_AUTH_ANONYMOUS_ENABLED': 'true',
                'GF_AUTH_ANONYMOUS_ORGANIZATION': 'public',
            },
        },
    },
}
default_docker_compose_config['networks'] = networks
default_docker_compose_config['volumes'] = {}
for key in volumes:
    default_docker_compose_config['volumes'][key] = None

default_stack_config = {}
### compose project name (prepends to all services)
default_stack_config['project_name'] = 'mrsm'
compose_filename = os.path.split(STACK_COMPOSE_PATH)[1]
default_stack_config[compose_filename] = default_docker_compose_config
from meerschaum.config.stack.grafana import default_grafana_config
default_stack_config['grafana'] = default_grafana_config
default_stack_config['filetype'] = 'yaml'

### check if configs are in sync

def _sync_stack_files():
    from meerschaum.config._sync import sync_yaml_configs
    sync_yaml_configs(
        CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', STACK_COMPOSE_FILENAME],
        STACK_COMPOSE_PATH,
        substitute = True,
    )
    sync_yaml_configs(
        CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', 'grafana', 'datasource'],
        GRAFANA_DATASOURCE_PATH,
        substitute = True,
    )
    sync_yaml_configs(
        CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', 'grafana', 'dashboard'],
        GRAFANA_DASHBOARD_PATH,
        substitute = True,
    )

NECESSARY_FILES = [STACK_COMPOSE_PATH, GRAFANA_DATASOURCE_PATH, GRAFANA_DASHBOARD_PATH]
def get_necessary_files():
    from meerschaum.config import get_config
    return {
        STACK_COMPOSE_PATH: (
            get_config('stack', STACK_COMPOSE_FILENAME, substitute=True), compose_header
        ),
        GRAFANA_DATASOURCE_PATH: get_config('stack', 'grafana', 'datasource', substitute=True),
        GRAFANA_DASHBOARD_PATH: get_config('stack', 'grafana', 'dashboard', substitute=True),
    }


def write_stack(
        debug: bool = False 
    ):
    """Write Docker Compose configuration files."""
    from meerschaum.config._edit import general_write_yaml_config
    from meerschaum.config._sync import sync_files
    general_write_yaml_config(get_necessary_files(), debug=debug)
    return sync_files(['stack'])
   
def edit_stack(
        action: Optional[List[str]] = None,
        debug: bool = False,
        **kw
    ):
    """Open docker-compose.yaml or .env for editing."""
    from meerschaum.config._edit import general_edit_config
    if action is None:
        action = []
    files = {
        'compose' : STACK_COMPOSE_PATH,
        'docker-compose' : STACK_COMPOSE_PATH,
        'docker-compose.yaml' : STACK_COMPOSE_PATH,
    }
    return general_edit_config(action=action, files=files, default='compose', debug=debug)
