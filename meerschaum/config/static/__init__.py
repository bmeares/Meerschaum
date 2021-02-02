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
            }
        }
    return static_config
