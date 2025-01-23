#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Insert non-user-editable configuration files here.
"""

import os
from typing import Dict, Any
from meerschaum.utils.misc import generate_password

__all__ = ['STATIC_CONFIG']

SERVER_ID: str = os.environ.get('MRSM_SERVER_ID', generate_password(6))
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
        'exists_timeout_seconds': 5.0,
        'static_schema_cache_seconds': 60.0,
        'max_bound_time_days': 36525,
    },
    'jobs': {
        'check_restart_seconds': 1.0,
    },
    'setup': {
        'name': 'meerschaum',
        'formal_name': 'Meerschaum',
        'app_id': 'io.meerschaum',
        'description': 'Sync Time-Series Pipes with Meerschaum',
        'url': 'https://meerschaum.io',
        'project_urls': {
            'Documentation': 'https://docs.meerschaum.io',
            'Changelog': 'https://meerschaum.io/news/changelog',
            'GitHub': 'https://github.com/bmeares/Meerschaum',
            'Homepage': 'https://meerschaum.io',
            'Donate': 'https://github.com/sponsors/bmeares',
            'Discord': 'https://discord.gg/8U8qMUjvcc',
        },
        'author': 'Bennett Meares',
        'author_email': 'bennett.meares@gmail.com',
        'maintainer_email': 'bennett.meares@gmail.com',
        'license': 'Apache Software License 2.0',
    },
}


def _static_config():
    """
    Alias function for the global `STATIC_CONFIG` dictionary.
    """
    return STATIC_CONFIG
