#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Insert non-user-editable configuration files here.
"""

static_config = None

def _static_config():
    global static_config
    if static_config is None:
        static_config = {
            'api' : {
                'endpoints' : {
                    'index' : '/',
                    'favicon' : '/favicon.ico',
                    'plugins' : '/plugins',
                    'pipes' : '/pipes',
                    'metadata' : '/metadata',
                    'actions' : '/actions',
                    'users' : '/users',
                    'login' : '/login',
                    'connectors' : '/connectors',
                    'version' : '/version',
                }
            },
            'config' : {
                'default_filetype' : 'json',
                'environment_key' : 'MEERSCHAUM_CONFIG',
            },
            'system' : {
                'arguments' : {
                    'sub_decorators' : (
                        '[',
                        ']'
                    ),
                },
            },
            'connectors' : {
                'default_label' : 'main',
            },
            'users' : {
                'password_hash' : {
                    'schemes' : [
                        'pbkdf2_sha256',
                    ],
                    'default' : 'pbkdf2_sha256',
                    'pbkdf2_sha256__default_rounds' : 30000,
                },
            },
        }
    return static_config
