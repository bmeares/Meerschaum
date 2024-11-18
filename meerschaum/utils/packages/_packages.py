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

_MRSM_PACKAGE_ARCHIVES_PREFIX: str = "https://meerschaum.io/files/archives/wheels/"

packages: Dict[str, Dict[str, str]] = {
    'required': {},
    'minimal': {},
    'formatting': {
        'pprintpp'                   : 'pprintpp>=0.4.0',
        'asciitree'                  : 'asciitree>=0.3.3',
        'typing_extensions'          : 'typing-extensions>=4.7.1',
        'pygments'                   : 'pygments>=2.7.2',
        'colorama'                   : 'colorama>=0.4.3',
        'rich'                       : 'rich>=13.4.2',
        'more_termcolor'             : 'more-termcolor>=1.1.3',
        'humanfriendly'              : 'humanfriendly>=10.0.0',
    },
    'core': {
        'wheel'                      : 'wheel>=0.34.2',
        'setuptools'                 : 'setuptools>=63.3.0',
        'yaml'                       : 'PyYAML>=5.3.1',
        'pip'                        : 'pip>=22.0.4',
        'update_checker'             : 'update-checker>=0.18.0',
        'semver'                     : 'semver>=3.0.2',
        'pathspec'                   : 'pathspec>=0.9.0',
        'dateutil'                   : 'python-dateutil>=2.7.5',
        'requests'                   : 'requests>=2.32.3',
        'certifi'                    : 'certifi>=2024.8.30',
        'idna'                       : 'idna>=3.10.0',
        'binaryornot'                : 'binaryornot>=0.4.4',
        'pyvim'                      : 'pyvim>=3.0.2',
        'ptpython'                   : 'ptpython>=3.0.27',
        'aiofiles'                   : 'aiofiles>=0.6.0',
        'packaging'                  : 'packaging>=21.3.0',
        'prompt_toolkit'             : 'prompt-toolkit>=3.0.39',
        'more_itertools'             : 'more-itertools>=8.7.0',
        'fasteners'                  : 'fasteners>=0.19.0',
        'virtualenv'                 : 'virtualenv>=20.1.0',
        'attrs'                      : 'attrs>=24.2.0',
        'uv'                         : 'uv>=0.2.11',
    },
    '_internal'                      : {
        'apscheduler'                : (
                                       f"{_MRSM_PACKAGE_ARCHIVES_PREFIX}"
                                       "APScheduler-4.0.0a5.post75+mrsm-py3-none-any.whl>=4.0.0a5"
        ),
        'dataclass_wizard'           : 'dataclass-wizard>=0.28.0',
    },
    'jobs': {
        'dill'                       : 'dill>=0.3.3',
        'daemon'                     : 'python-daemon>=0.2.3',
        'watchfiles'                 : 'watchfiles>=0.21.0',
        'psutil'                     : 'psutil>=5.8.0',
    },
    'drivers': {
        'cryptography'               : 'cryptography>=38.0.1',
        'psycopg'                    : 'psycopg[binary]>=3.2.1',
        'pymysql'                    : 'PyMySQL>=0.9.0',
        'aiomysql'                   : 'aiomysql>=0.0.21',
        'sqlalchemy_cockroachdb'     : 'sqlalchemy-cockroachdb>=2.0.0',
        'duckdb'                     : 'duckdb>=1.0.0',
        'duckdb_engine'              : 'duckdb-engine>=0.13.0',
    },
    'drivers-extras': {
        'pyodbc'                     : 'pyodbc>=4.0.30',
        'cx_Oracle'                  : 'cx_Oracle>=8.3.0',
    },
    'cli': {
        'pgcli'                      : 'pgcli>=3.1.0',
        'mycli'                      : 'mycli>=1.23.2',
        'litecli'                    : 'litecli>=1.5.0',
        'mssqlcli'                   : 'mssql-cli>=1.0.0',
        'gadwall'                    : 'gadwall>=0.2.0',
    },
    'stack': {
        'compose'                    : 'docker-compose>=1.29.2',
    },
    'build': {
        'cx_Freeze'                  : 'cx_Freeze>=7.0.0',
        'PyInstaller'                : 'pyinstaller>6.6.0',
    },
    'dev-tools': {
        'twine'                      : 'twine>=3.2.0',
        'tuna'                       : 'tuna>=0.5.3',
        'snakeviz'                   : 'snakeviz>=2.1.0',
        'mypy'                       : 'mypy>=0.812.0',
        'pytest'                     : 'pytest>=6.2.2',
        'pytest_xdist'               : 'pytest-xdist>=3.2.1',
        'heartrate'                  : 'heartrate>=0.2.1',
        'build'                      : 'build>=1.2.1',
        'attrs'                      : 'attrs>=24.2.0',
    },
    'setup': {
    },
    'docs': {
        'mkdocs'                     : 'mkdocs>=1.1.2',
        'mkdocs_material'            : 'mkdocs-material>=6.2.5',
        'mkdocs_material_extensions' : 'mkdocs-material-extensions>=1.0.3',
        'mkdocs_autolinks_plugin'    : 'mkdocs-autolinks-plugin>=0.2.0',
        'mkdocs_awesome_pages_plugin': 'mkdocs-awesome-pages-plugin>=2.5.0',
        'mkdocs_section_index'       : 'mkdocs-section-index>=0.3.3',
        'mkdocs_linkcheck'           : 'mkdocs-linkcheck>=1.0.6',
        'mkdocs_redirects'           : 'mkdocs-redirects>=1.0.4',
        'jinja2'                     : 'jinja2==3.0.3',
    },
    'gui': {
        'toga'                       : 'toga>=0.3.0-dev29',
        'webview'                    : 'pywebview>=3.6.3',
        'pycparser'                  : 'pycparser>=2.21.0',
    },
    'extras': {
        'cmd2'                       : 'cmd2>=1.4.0',
        'ruamel.yaml'                : 'ruamel.yaml>=0.16.12',
        'modin'                      : 'modin[ray]>=0.8.3',
        'nanoid'                     : 'nanoid>=2.0.0',
        'importlib_metadata'         : 'importlib-metadata>=4.12.0',
    },
}
packages['sql'] = {
    'numpy'                          : 'numpy>=1.18.5',
    'pandas'                         : 'pandas[parquet]>=2.0.1',
    'pyarrow'                        : 'pyarrow>=16.1.0',
    'dask'                           : 'dask[complete]>=2024.5.1',
    'partd'                          : 'partd>=1.4.2',
    'pytz'                           : 'pytz',
    'joblib'                         : 'joblib>=0.17.0',
    'sqlalchemy'                     : 'SQLAlchemy>=2.0.5',
    'databases'                      : 'databases>=0.4.0',
    'aiosqlite'                      : 'aiosqlite>=0.16.0',
    'asyncpg'                        : 'asyncpg>=0.21.0',
}
packages['sql'].update(packages['drivers'])
packages['sql'].update(packages['core'])
packages['dash'] = {
    'flask_compress'                 : 'Flask-Compress>=1.10.1',
    'dash'                           : 'dash>=2.6.2',
    'dash_bootstrap_components'      : 'dash-bootstrap-components>=1.2.1',
    'dash_ace'                       : 'dash-ace>=0.2.1',
    'dash_extensions'                : 'dash-extensions>=1.0.4',
    'dash_daq'                       : 'dash-daq>=0.5.0',
    'terminado'                      : 'terminado>=0.12.1',
    'tornado'                        : 'tornado>=6.1.0',
}
packages['api'] = {
    'uvicorn'                        : 'uvicorn[standard]>=0.29.0',
    'gunicorn'                       : 'gunicorn>=22.0.0',
    'dotenv'                         : 'python-dotenv>=0.20.0',
    'websockets'                     : 'websockets>=11.0.3',
    'fastapi'                        : 'fastapi>=0.111.0',
    'fastapi_login'                  : 'fastapi-login>=1.7.2',
    'multipart'                      : 'python-multipart>=0.0.9',
    'httpx'                          : 'httpx>=0.27.2',
    'httpcore'                       : 'httpcore>=1.0.6',
    'valkey'                         : 'valkey>=6.0.0',
}
packages['api'].update(packages['sql'])
packages['api'].update(packages['formatting'])
packages['api'].update(packages['dash'])
packages['api'].update(packages['jobs'])

all_packages = {}
for group, import_names in packages.items():
    all_packages.update(import_names)
install_names = {}
def get_install_names():
    """
    Initialize the mapping between install names and import names.
    """
    if not install_names:
        from meerschaum.utils.packages import get_install_no_version
        for _import_name, _install_name in all_packages.items():
            install_names[get_install_no_version(_install_name)] = _import_name
    return install_names


skip_groups = {
    'docs',
    'build',
    'cli',
    'dev-tools',
    'portable',
    'extras',
    'stack',
    'drivers-extras',
    '_internal',
}
full = []
_full = {}
for group, import_names in packages.items():
    ### omit 'cli' and 'docs' from 'full'
    if group in skip_groups or group.startswith('_'):
        continue
    full += [ install_name for import_name, install_name in import_names.items() ]
    for import_name, install_name in import_names.items():
        _full[import_name] = install_name
packages['full'] = _full
