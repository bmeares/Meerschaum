#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define internal static config (formerly `meerschaum.config.static`).
"""

import os
import random
import string
from typing import Dict, Any

__all__ = ('STATIC_CONFIG',)

_default_create_engine_args = {
    #  'method': 'multi',
    'pool_size': (os.cpu_count() or 5),
    'max_overflow': (os.cpu_count() or 10),
    'pool_recycle': 3600,
    'connect_args': {},
}
_default_db_requirements = {
    'username',
    'password',
    'host',
    'database',
}
SERVER_ID: str = os.environ.get('MRSM_SERVER_ID', ''.join(random.sample(string.ascii_lowercase+string.digits, 6)))
STATIC_CONFIG: Dict[str, Any] = {
    'api': {
        'endpoints': {
            'index': '/',
            'favicon': '/favicon.ico',
            'plugins': '/plugins',
            'pipes': '/pipes',
            'metadata': '/metadata',
            'actions': '/actions',
            'jobs': '/jobs',
            'logs': '/logs',
            'users': '/users',
            'tokens': '/tokens',
            'login': '/login',
            'connectors': '/connectors',
            'version': '/version',
            'chaining': '/chaining',
            'websocket': '/ws',
            'dash': '/dash',
            'webterm': r'/webterm/{session_id}',
            'webterm_websocket': r'/websocket/{session_id}',
            'info': '/info',
            'healthcheck': '/healthcheck',
            'docs': '/docs',
            'redoc': '/redoc',
            'openapi': '/openapi.json',
        },
        'oauth': {
            'token_expires_minutes': 720,
        },
        'webterm_job_name': '_webterm',
        'default_timeout': 600,
        'jobs': {
            'stdin_message': 'MRSM_STDIN',
            'stop_message': 'MRSM_STOP',
            'metadata_cache_seconds': 5,
            'temp_prefix': '.api-temp-',
        },
    },
    'sql': {
        'internal_schema': '_mrsm_internal',
        'instance_schema': 'mrsm',
        'default_create_engine_args': _default_create_engine_args,
        'create_engine_flavors': {
            'timescaledb': {
                'engine': 'postgresql+psycopg',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {},
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 5432,
                },
            },
            'timescaledb-ha': {
                'engine': 'postgresql+psycopg',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {},
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 5432,
                },
            },
            'postgresql': {
                'engine': 'postgresql+psycopg',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {},
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 5432,
                },
            },
            'postgis': {
                'engine': 'postgresql+psycopg',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {},
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 5432,
                },
            },
            'citus': {
                'engine': 'postgresql+psycopg',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {},
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 5432,
                },
            },
            'mssql': {
                'engine': 'mssql+pyodbc',
                'create_engine': {
                    'fast_executemany': True,
                    'use_insertmanyvalues': False,
                    'isolation_level': 'AUTOCOMMIT',
                    'use_setinputsizes': False,
                    'pool_pre_ping': True,
                    'ignore_no_transaction_on_rollback': True,
                },
                'omit_create_engine': {'method',},
                'to_sql': {
                    'method': None,
                },
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 1433,
                    'options': (
                        "driver=ODBC Driver 18 for SQL Server"
                        "&UseFMTONLY=Yes"
                        "&TrustServerCertificate=yes"
                        "&Encrypt=no"
                        "&MARS_Connection=yes"
                    ),
                },
            },
            'mysql': {
                'engine': 'mysql+pymysql',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {
                    'method': 'multi',
                },
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 3306,
                },
            },
            'mariadb': {
                'engine': 'mysql+pymysql',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {
                    'method': 'multi',
                },
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 3306,
                },
            },
            'oracle': {
                'engine': 'oracle+oracledb',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {
                    'method': None,
                },
                'requirements': _default_db_requirements,
                'defaults': {
                    'port': 1521,
                },
            },
            'sqlite': {
                'engine': 'sqlite',
                'create_engine': _default_create_engine_args,
                'omit_create_engine': {'method',},
                'to_sql': {
                    'method': 'multi',
                },
                'requirements': {'database'},
                'defaults': {},
            },
            'duckdb': {
                'engine': 'duckdb',
                'create_engine': {},
                'omit_create_engine': {'ALL',},
                'to_sql': {
                    'method': 'multi',
                },
                'requirements': '',
                'defaults': {},
            },
            'cockroachdb': {
                'engine': 'cockroachdb',
                'omit_create_engine': {'method',},
                'create_engine': _default_create_engine_args,
                'to_sql': {
                    'method': 'multi',
                },
                'requirements': {'host'},
                'defaults': {
                    'port': 26257,
                    'database': 'defaultdb',
                    'username': 'root',
                    'password': 'admin',
                },
            },
        },
    },
    'valkey': {
        'colon': '-_',
    },
    'environment': {
        'config': 'MRSM_CONFIG',
        'config_dir': 'MRSM_CONFIG_DIR',
        'patch': 'MRSM_PATCH',
        'root': 'MRSM_ROOT_DIR',
        'plugins': 'MRSM_PLUGINS_DIR',
        'venvs': 'MRSM_VENVS_DIR',
        'runtime': 'MRSM_RUNTIME',
        'work_dir': 'MRSM_WORK_DIR',
        'user': 'MRSM_USER',
        'dep_group': 'MRSM_DEP_GROUP',
        'home': 'MRSM_HOME',
        'src': 'MRSM_SRC',
        'uid': 'MRSM_UID',
        'gid': 'MRSM_GID',
        'noask': 'MRSM_NOASK',
        'noninteractive': 'MRSM_NONINTERACTIVE',
        'id': 'MRSM_SERVER_ID',
        'daemon_id': 'MRSM_DAEMON_ID',
        'systemd_log_path': 'MRSM_SYSTEMD_LOG_PATH',
        'systemd_stdin_path': 'MRSM_SYSTEMD_STDIN_PATH',
        'systemd_result_path': 'MRSM_SYSTEMD_RESULT_PATH',
        'systemd_delete_job': 'MRSM_SYSTEMD_DELETE_JOB',
        'uri_regex': r'MRSM_([a-zA-Z0-9]*)_(\d*[a-zA-Z][a-zA-Z0-9-_+]*$)',
        'prefix': 'MRSM_',
    },
    'config': {
        'default_filetype': 'json',
        'symlinks_key': '_symlinks',
    },
    'system': {
        'arguments': {
            'sub_decorators': (
                '[',
                ']'
            ),
            'underscore_standin': '<UNDERSCORE>', ### Temporary replacement for parsing.
            'failure_key': '_argparse_exception',
            'and_key': '+',
            'escaped_and_key': '++',
            'pipeline_key': ':',
            'escaped_pipeline_key': '::',
        },
        'urls': {
            'get-pip.py': 'https://bootstrap.pypa.io/get-pip.py',
        },
        'success': {
            'ignore': (
                'Success',
                'success'
                'Succeeded',
                '',
                None,
            ),
        },
        'prompt': {
            'web': False,
        },
        'fetch_pipes_keys': {
            'negation_prefix': '_',
        },
    },
    'connectors': {
        'default_label': 'main',
    },
    'dtypes': {
        'datetime': {
            'default_precision_unit': 'microsecond',
        },
    },
    'stack': {
        'dollar_standin': '<DOLLAR>', ### Temporary replacement for docker-compose.yaml.
    },
    'users': {
        'password_hash': {
            'algorithm_name': 'sha256',
            'salt_bytes': 16,
            'schemes': [
                'pbkdf2_sha256',
            ],
            'default': 'pbkdf2_sha256',
            'pbkdf2_sha256__default_rounds': 1_000_000,
        },
        'min_username_length': 1,
        'max_username_length': 60,
        'min_password_length': 5,
    },
    'plugins': {
        'repo_separator': '@',
        'lock_sleep_total': 1.0,
        'lock_sleep_increment': 0.1,
    },
    'pipes': {
        'dtypes': {
            'min_ratio_columns_changed_for_full_astype': 0.5,
        },
        'max_bound_time_days': 36525,
    },
    'jobs': {
        'check_restart_seconds': 1.0,
    },
    'tokens': {
        'minimum_length': 24,
        'maximum_length': 32,
        'hash_rounds': 100_000,
        'scopes': {
            'pipes:read': "Read pipes' parameters and the contents of target tables.",
            'pipes:write': "Update pipes' parameters and sync to target tables.",
            'pipes:drop': "Drop target tables.",
            'pipes:delete': "Delete pipes' parameters and drop target tables.",
            'actions:execute': "Execute arbitrary actions.",
            'connectors:read': "Read the available connectors.",
            'jobs:read': "Read jobs' properties",
            'jobs:write': "Write jobs' properties",
            'jobs:execute': "Run jobs.",
            'jobs:delete': "Delete jobs.",
            'logs:read': "Read jobs' logs.",
            'jobs:stop': "Stop running jobs.",
            'jobs:pause': "Pause running jobs.",
            'instance:read': "Read an instance's system-level metadata.",
            'instance:chain': "Allow chaining API instances using the associated credentials.",
            'plugins:write': "Register and update plugins' metadata.",
            'plugins:read': "Read attributes of registered plugins.",
            'plugins:delete': "Delete plugins (owned by user) from the repository.",
            'users:read': "Read metadata about the associated account.",
            'users:write': "Write metadata for the associated account.",
            'users:register': "Register new user accounts.",
            'users:delete': "Delete the associated user account (or other users for admins).",
        },
    },
    
}
