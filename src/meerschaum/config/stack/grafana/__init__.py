#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Stack configuration for Grafana
"""

import os
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


grafana_context_manager = pkg_resources.path('meerschaum.config.stack', 'grafana')
with grafana_context_manager as file_path:
    grafana_path = file_path

grafana_resources_path = os.path.join(grafana_path, 'resources')
grafana_provisioning_path = os.path.join(grafana_resources_path, 'provisioning')
grafana_datasources_dir_path = os.path.join(grafana_provisioning_path, 'datasources')
grafana_datasource_yaml_path = os.path.join(grafana_datasources_dir_path, 'datasource.yaml')

grafana_dashboards_dir_path = os.path.join(grafana_provisioning_path, 'dashboards')
grafana_dashboard_yaml_path = os.path.join(grafana_dashboards_dir_path, 'dashboard.yaml')

from meerschaum.config.stack import db_host, db_user, db_pass, db_port, db_base

datasource = {
    'apiVersion' : 1,
    'datasources' : [
        {
            'name' : 'Meerschaum Main',
            'type' : 'postgres',
            'jsonData' : {
                'sslmode' : 'disable',
                'postgresVersion' : 1200,
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

dashboard = {
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

def edit_grafana(
        action : list = [''],
        debug : bool = False,
        **kw
    ):
    """
    Open Grafana configuration files for editing
    """

    from meerschaum.config._edit import general_edit_config
    files = {
        'data' : grafana_datasource_yaml_path,
        'datasource' : grafana_datasource_yaml_path,
        'datasources' : grafana_datasource_yaml_path,
        'datasource.yaml' : grafana_datasource_yaml_path,
        'datasources.yaml' : grafana_datasource_yaml_path,
        'dash' : grafana_dashboard_yaml_path,
        'dashboard' : grafana_dashboard_yaml_path,
        'dashboard.yaml' : grafana_dashboard_yaml_path,
    }
    return general_edit_config(action=action, files=files, default='datasource', debug=debug)

