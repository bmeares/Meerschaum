#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Stack configuration for Grafana
"""

import os
from meerschaum.config.stack import db_host, db_user, db_pass, db_port, db_base

default_datasource = {
    'apiVersion' : 1,
    'datasources' : [
        {
            'name' : 'Meerschaum Main',
            'type' : 'postgres',
            'jsonData' : {
                'sslmode' : 'disable',
                'postgresVersion' : 1400,
                'timescaledb' : True,
            },
            'user' : db_user,
            'secureJsonData' : {
                'password' : db_pass,
            },
            'database' : db_base,
            'url' : db_host + ':' + str(db_port),
            'isDefault' : True,
            'editable' : True,
        }
    ],
}

default_dashboard = {
    'apiVersion' : 1,
    'providers' : [
        {
            'name' : 'Default',
            'folder' : 'Meerschaum Dashboards',
            'options' : {
                'path' : '/etc/grafana/provisioning/dashboards',
            },
        }
    ],
}

### build config dictionary
default_grafana_config = {}
default_grafana_config['dashboard'] = default_dashboard
default_grafana_config['datasource'] = default_datasource

def edit_grafana(
        action: list = [''],
        debug: bool = False,
        **kw
    ):
    """Open Grafana configuration files for editing."""

    from meerschaum.config._edit import general_edit_config
    from meerschaum.config._paths import GRAFANA_DATASOURCE_PATH, GRAFANA_DASHBOARD_PATH
    files = {
        'data' : GRAFANA_DATASOURCE_PATH,
        'datasource' : GRAFANA_DATASOURCE_PATH,
        'datasources' : GRAFANA_DATASOURCE_PATH,
        'datasource.yaml' : GRAFANA_DATASOURCE_PATH,
        'datasources.yaml' : GRAFANA_DATASOURCE_PATH,
        'dash' : GRAFANA_DASHBOARD_PATH,
        'dashboard' : GRAFANA_DASHBOARD_PATH,
        'dashboard.yaml' : GRAFANA_DASHBOARD_PATH,
    }
    return general_edit_config(action=action, files=files, default='datasource', debug=debug)

