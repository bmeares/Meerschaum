#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Directory of necessary packages

packages dictionary is structured in the following schema:
    {
        <group> : {
            <import_name> : <install_name>
        }
    }
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict

packages : Dict[str, Dict[str, str]] = {
    'required' : {
        'wheel'                      : 'wheel>=0.34.2',
        'yaml'                       : 'PyYAML>=5.3.1',
        'virtualenv'                 : 'virtualenv>=20.1.0',
    },
    'minimal' : {},
    'formatting' : {
        'pprintpp'                   : 'pprintpp>=0.4.0',
        'asciitree'                  : 'asciitree>=0.3.3',
        'typing_extensions'          : 'typing_extensions>=3.7.4.3',
        'pygments'                   : 'pygments>=2.7.2',
        'colorama'                   : 'colorama>=0.4.3',
        'rich'                       : 'rich>=10.12.0',
        'more_termcolor'             : 'more-termcolor>=1.1.3',
        'humanfriendly'              : 'humanfriendly>=10.0',
    },
    '_required': {
        'pip'                        : 'pip>=21.0.1',
        'update_checker'             : 'update-checker>=0.18.0',
        'semver'                     : 'semver>=2.13.0',
        'dateutil'                   : 'python-dateutil>=2.7.5',
        'requests'                   : 'requests>=2.23.0',
        'binaryornot'                : 'binaryornot>=0.4.4',
        'dill'                       : 'dill>=0.3.3',
        'pyvim'                      : 'pyvim>=3.0.2',
        'aiofiles'                   : 'aiofiles>=0.6.0',
        'packaging'                  : 'packaging>=20.4',
        'prompt_toolkit'             : 'prompt-toolkit>=3.0.11',
        'more_itertools'             : 'more-itertools>=8.7.0',
        'daemoniker'                 : 'daemoniker>=0.2.3',
        'psutil'                     : 'psutil>=5.8.0',
        'watchgod'                   : 'watchgod>=0.7',
        'pygtail'                    : 'pygtail>=0.11.1',
    },
    'iot': {
        'paho': 'paho-mqtt>=1.5.1',
    },
    'drivers': {
        'psycopg2'                   : 'psycopg2-binary>=2.8.6',
        'pymysql'                    : 'PyMySQL>=0.9',
        'aiomysql'                   : 'aiomysql>=0.0.21',
        'sqlalchemy_cockroachdb'     : 'sqlalchemy-cockroachdb>=1.4.2',
        'duckdb'                     : 'duckdb>=0.3.2.dev579',
        'duckdb_engine'              : 'duckdb-engine>=0.1.3',
        #  'pyodbc'        : 'pyodbc>=4.0.30', ### Not included due to Docker image issues.
    },
    'cli': {
        'pgcli'                      : 'pgcli>=3.1.0',
        'mycli'                      : 'mycli>=1.23.2',
        'litecli'                    : 'litecli>=1.5.0',
        'mssqlcli'                   : 'mssql-cli>=1.0.0',
    },
    'stack': {
        #  'docker'                     : 'docker>=4.3.1',
        'compose'                    : 'docker-compose>=1.27.4',
    },
    'build': {
        'cx_Freeze'                  : 'cx_Freeze>=6.5.1',
        'PyInstaller'                : 'pyinstaller>=5.0.dev0',
    },
    'dev-tools': {
        'twine'                      : 'twine>=3.2.0',
        'tuna'                       : 'tuna>=0.5.3',
        'snakeviz'                   : 'snakeviz>=2.1.0',
        'mypy'                       : 'mypy>=0.812',
        'pytest'                     : 'pytest>=6.2.2',
        'heartrate'                  : 'heartrate>=0.2.1',
        'pyheat'                     : 'py-heat>=0.0.6',
    },
    'setup': {
    },
    'docs': {
        'mkdocs'                     : 'mkdocs>=1.1.2',
        'pdoc'                       : 'pdoc3>=0.9.2',
        'mkdocs_material'            : 'mkdocs-material>=6.2.5',
        'mkdocs_autolinks_plugin'    : 'mkdocs-autolinks-plugin>=0.2.0',
        'mkdocs_awesome_pages_plugin': 'mkdocs-awesome-pages-plugin>=2.5.0',
        'mkdocs_rss_plugin'          : 'mkdocs-rss-plugin>=0.16.1'
    },
    'portable': {
        'pyreadline3'                : 'pyreadline3>=3.3; platform_system == "Windows"',
        'gnureadline'                : 'gnureadline>=8.0.0; platform_system != "Windows"',
    },
    'gui': {
        'toga'                       : 'toga>=0.3.0.dev29',
        'terminado'                  : 'terminado>=0.12.1',
        'tornado'                    : 'tornado>=6.1',
        'tornado_xstatic'            : 'tornado-xstatic>=0.2',
        'xstatic'                    : 'XStatic>=1.0.2',
        'xstatic.pkg.termjs'         : 'XStatic-term.js>=0.0.7.0',
        'webview'                    : 'pywebview>=3.5',
    },
    'extras': {
        'cmd2'                       : 'cmd2>=1.4.0',
        'ruamel.yaml'                : 'ruamel.yaml>=0.16.12',
        'pandasgui'                  : 'pandasgui>=0.2.9',      
        'modin'                      : 'modin[ray]>=0.8.3'
    },
}
packages['sql'] = {
    'numpy'                          : 'numpy>=1.18.5',
    'pandas'                         : 'pandas>=1.1.4',
    'joblib'                         : 'joblib>=0.17.0',
    'sqlalchemy'                     : 'sqlalchemy>=1.4.17',
    'sqlalchemy_utils'               : 'sqlalchemy-utils>=0.37.3',
    'databases'                      : 'databases>=0.4.0',
    'aiosqlite'                      : 'aiosqlite>=0.16.0',
    'asyncpg'                        : 'asyncpg>=0.21.0',
}
packages['sql'].update(packages['drivers'])
packages['dash'] = {
    'dash'                           : 'dash>=2.0.0',
    'dash_bootstrap_components'      : 'dash-bootstrap-components>=1.0.1rc1',
    'dash_ace'                       : 'dash-ace>=0.2.1',
    'dash_extensions'                : 'dash-extensions>=0.0.51',
    'dash_daq'                       : 'dash-daq>=0.5.0',
    'ansi2html'                      : 'ansi2html>=1.6.0',
    'flask_compress'                 : 'Flask-Compress>=1.10.1',
}
packages['api'] = {
    'uvicorn'                        : 'uvicorn[standard]>=0.13.4',
    'websockets'                     : 'websockets>=8.1',
    'fastapi'                        : 'fastapi>=0.61.2',
    'fastapi_jwt_auth'               : 'fastapi-jwt-auth>=0.5.0',
    'passlib'                        : 'passlib>=1.7.4',
    'fastapi_login'                  : 'fastapi-login>=1.7.2',
    'multipart'                      : 'python-multipart>=0.0.5',
}
packages['api'].update(packages['sql'])
packages['api'].update(packages['_required'])
packages['api'].update(packages['formatting'])
packages['api'].update(packages['dash'])

all_packages = dict()
for group, import_names in packages.items():
    all_packages.update(import_names)

skip_groups = {'docs', 'build', 'cli', 'dev-tools', 'portable', 'extras', 'stack'}
full = list()
_full = dict()
for group, import_names in packages.items():
    ### omit 'cli' and 'docs' from 'full'
    if group in skip_groups:
        continue
    full += [ install_name for import_name, install_name in import_names.items() ]
    for import_name, install_name in import_names.items():
        _full[import_name] = install_name
packages['full'] = _full

