#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Docker Compose stack configuration goes here
"""

from __future__ import annotations

import os
import json

import meerschaum.config.paths as paths
from meerschaum.utils.typing import Optional, List

db_port = "MRSM{meerschaum:connectors:sql:main:port}"
db_user = "MRSM{meerschaum:connectors:sql:main:username}"
db_pass = "MRSM{meerschaum:connectors:sql:main:password}"
db_base = "MRSM{meerschaum:connectors:sql:main:database}"

### default localhost, db for docker network
db_hostname = "db"
db_host = 'MRSM{stack:' + str(paths.STACK_COMPOSE_FILENAME) + ':services:db:hostname}'
api_port = "MRSM{meerschaum:connectors:api:main:port}"
api_host = "api"

valkey_hostname = "valkey"
valkey_host = 'MRSM{stack:' + str(paths.STACK_COMPOSE_FILENAME) + ':services:valkey:hostname}'
valkey_port = "MRSM{meerschaum:connectors:valkey:main:port}"
valkey_username = 'MRSM{meerschaum:connectors:valkey:main:username}'
valkey_password = 'MRSM{meerschaum:connectors:valkey:main:password}'

env_dict = {
    'COMPOSE_PROJECT_NAME': 'mrsm',
    'TIMESCALEDB_VERSION': 'pg18',
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
        'api': 'MRSM{!api}',
    },
    indent=4,
).replace(
    '"MRSM{!system}"', 'MRSM{!system}'
).replace(
    '"MRSM{!meerschaum}"', 'MRSM{!meerschaum}'
).replace(
    '"MRSM{!api}"', 'MRSM{!api}'
)

volumes = {
    'api_root': '/meerschaum',
    'meerschaum_db_data': '/home/postgres/pgdata',
    'grafana_storage': '/var/lib/grafana',
    'valkey_data': '/data',
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

### The database setup script `mrsm stack` writes to `DB_CREATE_EXTENSIONS_PATH` when it is absent
### and mounts as `/docker-entrypoint-initdb.d/100_meerschaum.sql`. Byte-identical to the Rust
### `meerschaum_core::config::sync::CREATE_EXTENSIONS_TEXT`
### (`crates/meerschaum-core/src/config/create-extensions.sql`; `tests/test_stack_setup.py`).
CREATE_EXTENSIONS_TEXT = r"""-- Meerschaum stack: database setup.
--
-- `mrsm stack` writes this file once, when it is absent (your edits are kept), and mounts it into
-- the `db` container as `/docker-entrypoint-initdb.d/100_meerschaum.sql`. The image's entrypoint
-- runs it with `psql` only when the data volume is empty (the first start of a new stack), after
-- the image's own setup scripts (TimescaleDB, the telemetry opt-out, `timescaledb-tune`, the
-- TimescaleDB Toolkit). It is safe to run more than once and with any PostgreSQL image: an
-- extension the image does not ship is skipped with a NOTICE.
--
-- To change an existing database, see "Upgrading the stack" in the Meerschaum documentation
-- (`mrsm upgrade extensions`).

-- Create each extension the server offers. TimescaleDB must be preloaded to be created.
DO $mrsm$
DECLARE
    ext TEXT;
BEGIN
    FOREACH ext IN ARRAY ARRAY['timescaledb', 'postgis', 'timescaledb_toolkit', 'pg_stat_statements'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = ext) THEN
            RAISE NOTICE 'Meerschaum: skipping the extension "%": this image does not provide it.', ext;
        ELSIF ext = 'timescaledb' AND NOT (
            'timescaledb' = ANY (
                SELECT btrim(lib, ' "')
                FROM unnest(string_to_array(current_setting('shared_preload_libraries'), ',')) AS l(lib)
            )
        ) THEN
            RAISE NOTICE 'Meerschaum: skipping the extension "timescaledb": it is not in shared_preload_libraries.';
        ELSE
            EXECUTE format('CREATE EXTENSION IF NOT EXISTS %I CASCADE', ext);
        END IF;
    END LOOP;
END
$mrsm$;

-- `pg_stat_statements` can only be queried when the server preloads it: append it to
-- `shared_preload_libraries` (keeping the current entries) with `ALTER SYSTEM`, which writes
-- `postgresql.auto.conf` and takes effect at the server's next start (the first real start, after
-- this setup). `ALTER SYSTEM` cannot run inside a `DO` block, so psql runs the generated
-- statement (`\gexec`); nothing is generated when it is unavailable or already listed.
WITH current_libs AS (
    SELECT btrim(lib, ' "') AS lib, ord
    FROM unnest(string_to_array(current_setting('shared_preload_libraries'), ','))
        WITH ORDINALITY AS l(lib, ord)
), wanted AS (
    SELECT lib, ord FROM current_libs WHERE lib <> ''
    UNION ALL
    SELECT 'pg_stat_statements', 2147483647
)
SELECT 'ALTER SYSTEM SET shared_preload_libraries = '
    || string_agg(quote_literal(lib), ', ' ORDER BY ord)
FROM wanted
WHERE EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements')
    AND NOT EXISTS (SELECT 1 FROM current_libs WHERE lib = 'pg_stat_statements')
HAVING count(*) > 0
\gexec
"""

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
            'command': 'postgres -c max_connections=1000 -c shared_buffers=1024MB -c max_prepared_transactions=100',
            ### `-h 127.0.0.1`: the entrypoint's setup server listens only on its Unix socket, so
            ### the check stays unhealthy until setup is done and the real server is up.
            'healthcheck': {
                'test': [
                    'CMD-SHELL',
                    'pg_isready -h 127.0.0.1 -p 5432 -d <DOLLAR>POSTGRES_DB -U <DOLLAR>POSTGRES_USER',
                ],
                'interval': '5s',
                'timeout': '3s',
                'retries': 5,
                'start_period': '300s',
            },
            'restart': 'always',
            'image': 'timescale/timescaledb-ha:' + env_dict['TIMESCALEDB_VERSION'],
            'ports': [
                f'{db_port}:5432',
            ],
            'hostname': db_hostname,
            'volumes': [
                'meerschaum_db_data:' + volumes['meerschaum_db_data'],
                ### Mount the setup FILE into the image's directory (not a directory over it), so
                ### the image's own setup scripts run first (telemetry, timescaledb-tune, ...).
                (
                    f'{paths.DB_CREATE_EXTENSIONS_PATH.as_posix()}'
                    ':/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro'
                ),
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
            'image': 'valkey/valkey-bundle:latest',
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
                f'{paths.GRAFANA_DATASOURCE_PATH.parent.as_posix()}:/etc/grafana/provisioning/datasources:z,ro',
                f'{paths.GRAFANA_DASHBOARD_PATH.parent.as_posix()}:/etc/grafana/provisioning/dashboards:z,ro',
            ],
            'environment': {
                'GF_SECURITY_ALLOW_EMBEDDING': 'true',
                'GF_ANALYTICS_REPORTING_ENABLED': 'false',
                'GF_AUTH_ANONYMOUS_ENABLED': 'true',
                'GF_AUTH_ANONYMOUS_ORG_NAME': 'Main Org.',
                'GF_AUTH_ANONYMOUS_ORG_ROLE': 'Viewer',
                'GF_AUTH_ANONYMOUS_HIDE_VERSION': 'true',
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
compose_filename = os.path.split(paths.STACK_COMPOSE_PATH)[1]
default_stack_config[compose_filename] = default_docker_compose_config
from meerschaum.config.stack.grafana import default_grafana_config
default_stack_config['grafana'] = default_grafana_config
default_stack_config['filetype'] = 'yaml'

### check if configs are in sync

def _sync_stack_files():
    from meerschaum.config._sync import sync_yaml_configs
    sync_yaml_configs(
        paths.CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', paths.STACK_COMPOSE_FILENAME],
        paths.STACK_COMPOSE_PATH,
        substitute = True,
    )
    sync_yaml_configs(
        paths.CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', 'grafana', 'datasource'],
        paths.GRAFANA_DATASOURCE_PATH,
        substitute = True,
    )
    sync_yaml_configs(
        paths.CONFIG_DIR_PATH / 'stack.yaml',
        ['stack', 'grafana', 'dashboard'],
        paths.GRAFANA_DASHBOARD_PATH,
        substitute = True,
    )

    _write_initdb()

def _write_initdb():
    """Write the database setup script when it is absent (a customized file is kept)."""
    if paths.DB_CREATE_EXTENSIONS_PATH.exists():
        return

    paths.DB_CREATE_EXTENSIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(paths.DB_CREATE_EXTENSIONS_PATH, 'w+', encoding='utf-8', newline='\n') as f:
        f.write(CREATE_EXTENSIONS_TEXT)

NECESSARY_FILES = [
    paths.STACK_COMPOSE_PATH,
    paths.GRAFANA_DATASOURCE_PATH,
    paths.GRAFANA_DASHBOARD_PATH,
]
def get_necessary_files():
    from meerschaum.config import get_config
    return {
        paths.STACK_COMPOSE_PATH: (
            get_config('stack', paths.STACK_COMPOSE_FILENAME, substitute=True), compose_header
        ),
        paths.GRAFANA_DATASOURCE_PATH: get_config('stack', 'grafana', 'datasource', substitute=True),
        paths.GRAFANA_DASHBOARD_PATH: get_config('stack', 'grafana', 'dashboard', substitute=True),
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
        'compose': paths.STACK_COMPOSE_PATH,
        'docker-compose': paths.STACK_COMPOSE_PATH,
        'docker-compose.yaml': paths.STACK_COMPOSE_PATH,
    }
    return general_edit_config(action=action, files=files, default='compose', debug=debug)
