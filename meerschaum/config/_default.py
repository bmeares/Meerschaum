#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The default configuration values to write to config.yaml.
"""

import sys, os, multiprocessing

from meerschaum.connectors import attributes as connector_attributes
from meerschaum.config._paths import SQLITE_DB_PATH

default_meerschaum_config = {
    'instance': 'sql:main',
    'api_instance': 'MRSM{meerschaum:instance}',
    'web_instance': 'MRSM{meerschaum:instance}',
    'default_repository': 'api:mrsm',
    'connectors': {
        'sql': {
            'default': {},
            'main': {
                'username': 'mrsm',
                'password': 'mrsm',
                'flavor': 'timescaledb',
                'host': 'localhost',
                'database': 'meerschaum',
                'port': 5432,
            },
            'local': {
                'flavor': 'sqlite',
                'database': str(SQLITE_DB_PATH),
            },
            'memory': {
                'flavor': 'sqlite',
                'database': ':memory:',
            },
        },
        'api': {
            'default': connector_attributes['api']['default'],
            'main': {
                'host': 'localhost',
                'port': 8000,
            },
            'local': {
                'host': 'localhost',
                'port': 8000,
            },
            'mrsm': {
                'host': 'api.mrsm.io',
                'port': 443,
                'protocol': 'https',
            },
        },
        'valkey': {
            'main': {
                'host': 'localhost',
                'username': 'default',
                'password': 'mrsm',
                'port': 6379,
            },
        },
    },
}
default_system_config = {
    'connectors': {
        'all': {
            'pandas': 'pandas',
        },
        'sql': {
            'instance': {
                'stale_temporary_tables_minutes': 1440,
            },
            'chunksize': 100_000,
            'poolclass': 'sqlalchemy.pool.QueuePool',
            'create_engine': {
                'method': 'multi',
                'pool_size': 8,
                'max_overflow': 12,
                'pool_recycle': 3600,
                'connect_args': {},
            },
        },

        'api': {
        },
    },
    ### not to be confused with system_config['connectors']['api'], this is the configuration
    ### for the API server itself.
    'api': {
        'uvicorn': {
            'app': 'meerschaum.api:app',
            'port': 8000,
            'host': '0.0.0.0',
            'workers': max(int(multiprocessing.cpu_count() / 2), 1),
            'proxy_headers': True,
            'forwarded_allow_ips': '*',
        },
        'cache': {
            'connector': 'valkey:main',
            'session_expires_minutes': 43200,
        },
        'permissions':       {
            'registration': {
                'users': True,
                'pipes': True,
                'plugins': True,
            },
            'actions': {
                'non_admin': True,
            },
            'chaining': {
                'insecure_parent_instance': False,
                'child_apis': False,
            },
        },
        'protocol': default_meerschaum_config['connectors']['api']['default']['protocol'],
    },
    'experimental': {
        'fetch': False,
        'cache': True,
        'space': False,
        'join_fetch': False,
        'inplace_sync': True,
        'uv_pip': True,
        'systemd_healthcheck': False,
        'valkey_session_cache': True,
    },
}
default_pipes_config = {
    'parameters': {
        'columns': {
            'datetime': None,
        },
        'fetch': {
            'backtrack_minutes': 1440,
        },
        'verify': {
            'bound_days': 366,
            'chunk_minutes': 43200,
        },
    },
    'attributes': {
        'local_cache_timeout_seconds': 60,
    },
    'sync': {
        'filter_params_index_limit': 250,
    },
}
default_plugins_config = {}
default_experimental_config = {
    'venv': True,
}



### build default config dictionary
default_config = {}
default_config['meerschaum'] = default_meerschaum_config
default_config['system'] = default_system_config
from meerschaum.config._formatting import default_formatting_config
default_config['formatting'] = default_formatting_config
from meerschaum.config._shell import default_shell_config
default_config['shell'] = default_shell_config
default_config['pipes'] = default_pipes_config
default_config['plugins'] = default_plugins_config
from meerschaum.config._jobs import default_jobs_config
default_config['jobs'] = default_jobs_config
### add configs from other packages
try:
    import meerschaum.config.stack
except ImportError as e:
    pass
finally:
    from meerschaum.config.stack import default_stack_config
default_config['stack'] = default_stack_config
from meerschaum.config._dash import default_dash_config
default_config['dash'] = default_dash_config

default_header_comment = """
#####################################################################
#                                                                   #
#  Edit or add credentials for connectors.                          #
#  You can read more about connectors at https://meerschaum.io.     #
#                                                                   #
#  Connectors inherit from `default`, and flavor-dependent defaults #
#  are defined for SQL connectors (e.g. port 5432 for PostgreSQL).  #
#                                                                   #
#####################################################################

"""
