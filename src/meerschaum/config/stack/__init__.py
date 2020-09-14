#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Docker Compose stack configuration goes here
"""

import yaml, os
from meerschaum.config._read_yaml import config as cf
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

stack_context_manager = pkg_resources.path('meerschaum.config', 'stack')
with stack_context_manager as file_path:
    stack_path = file_path

stack_resources_path = os.path.join(stack_path, 'resources')
compose_filename = 'docker-compose.yaml'
compose_path = os.path.join(stack_resources_path, compose_filename)
env_filename = '.env'
env_path = os.path.join(stack_resources_path, env_filename)

db_port = cf['meerschaum']['connectors']['sql']['main']['port']
db_user = cf['meerschaum']['connectors']['sql']['main']['username']
db_pass = cf['meerschaum']['connectors']['sql']['main']['password']
#  db_host = cf['meerschaum']['connectors']['sql']['main']['host']
### default localhost, meerschaum_db for docker network
db_host = "meerschaum_db"
db_base = cf['meerschaum']['connectors']['sql']['main']['database']

api_port = cf['meerschaum']['connectors']['api']['main']['port']
api_host = "meerschaum_api"

env_text = (f"""
### Edit environment variables with `edit stack env`
COMPOSE_PROJECT_NAME="meerschaum_stack"
TIMESCALEDB_VERSION="latest-pg12"
POSTGRES_USER="{db_user}"
POSTGRES_PASSWORD="{db_pass}"
POSTGRES_DB="{db_base}"
MEERSCHAUM_DB_HOSTNAME="{db_host}"
MEERSCHAUM_API_HOSTNAME="{api_host}"
ALLOW_IP_RANGE='0.0.0.0/0'
"""
"""
MEERSCHAUM_API_CONFIG='{"meerschaum":{"connectors":{"sql":{"meta":{"host":"${MEERSCHAUM_DB_HOSTNAME}"}}}}}'
MEERSCHAUM_API_CONFIG_RESOURCES=/usr/local/lib/python3.8/site-packages/meerschaum/config/resources/
"""
)
compose_header = """
#########################################
# Edit the Docker Compose configuration #
# for the Meerschaum stack below        #
#########################################
"""

from meerschaum.config.stack.grafana import grafana_datasources_dir_path, grafana_dashboards_dir_path
volumes = {
    'meerschaum_api_config_resources' : '${MEERSCHAUM_API_CONFIG_RESOURCES}',
    'meerschaum_db_data' : '/var/lib/postgresql/data',
    'grafana_storage' : '/var/lib/grafana',
}
networks = {
    'frontend' : None,
    'backend' : None,
}

stack_config = {
    'version' : '3.8',
    'services': {
        'meerschaum_db' : {
            'environment' : [
                'TIMESCALEDB_TELEMETRY=off',
                'POSTGRES_USER=${POSTGRES_USER}',
                'POSTGRES_DB=${POSTGRES_DB}',
                'POSTGRES_PASSWORD=${POSTGRES_PASSWORD}',
                'ALLOW_IP_RANGE=${ALLOW_IP_RANGE}',
            ],
            'image' : 'timescale/timescaledb:${TIMESCALEDB_VERSION}',
            'ports' : [
                f'{db_port}:{db_port}'
            ],
            'hostname' : '${MEERSCHAUM_DB_HOSTNAME}',
            'volumes' : [
                'meerschaum_db_data' + ':' + volumes['meerschaum_db_data']
            ],
            'networks' : [
                'backend'
            ],
        },
        'meerschaum_api' : {
            'image' : 'bmeares/meerschaum:latest',
            'ports' : [f'{api_port}:{api_port}'],
            'hostname' : f'{api_host}',
            'networks' : [
                'frontend',
                'backend'
            ],
            'command' : 'api start',
            'environment' : [
                'MEERSCHAUM_CONFIG=${MEERSCHAUM_API_CONFIG}' 
            ],
            'depends_on' : [
                'meerschaum_db'
            ],
            'volumes' : [
                'meerschaum_api_config_resources' + ':' + volumes['meerschaum_api_config_resources']
            ],
        },
        'grafana' : {
            'image' : 'grafana/grafana:latest',
            'ports' : [
                '3000:3000'
            ],
            'networks' : [
                'frontend',
                'backend'
            ],
            'volumes' : [
                'grafana_storage' + ':' + volumes['grafana_storage'],
                f'{grafana_datasources_dir_path}:/etc/grafana/provisioning/datasources',
                f'{grafana_dashboards_dir_path}:/etc/grafana/provisioning/dashboards',
            ],
        },
    },
}
stack_config['networks'] = networks
stack_config['volumes'] = {}
for key in volumes:
    stack_config['volumes'][key] = None

### collect necessary files for stack to work
from meerschaum.config.stack.grafana import grafana_datasource_yaml_path, grafana_dashboard_yaml_path, datasource, dashboard
necessary_files = {
    env_path : env_text,
    compose_path : (stack_config, compose_header),
    grafana_datasource_yaml_path : datasource,
    grafana_dashboard_yaml_path : dashboard,
}

def write_stack(
        debug : bool = False 
    ):
    """
    Write Docker Compose configuration files
    """

    from meerschaum.config._edit import general_write_config
    return general_write_config(necessary_files, debug=debug)
   
def edit_stack(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    """
    Open docker-compose.yaml or .env for editing
    """
    from meerschaum.config._edit import general_edit_config
    files = {
        'compose' : compose_path,
        'docker-compose' : compose_path,
        'docker-compose.yaml' : compose_path,
        'env' : env_path,
        'environment' : env_path,
        '.env' : env_path,
    }
    return general_edit_config(action=action, files=files, default='compose', debug=debug)

