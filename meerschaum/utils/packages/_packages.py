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

from typing import Dict, List

_MRSM_PACKAGE_ARCHIVES_PREFIX: str = "https://meerschaum.io/files/archives/wheels/"

packages: Dict[str, Dict[str, str]] = {
    ### Meerschaum installs its own dependencies into the `mrsm` venv at runtime,
    ### so the base package requires nothing.
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
        'tabulate'                   : 'tabulate>=0.9.0',
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
        'requests'                   : 'requests>=2.32.5',
        'certifi'                    : 'certifi>=2024.8.30',
        'idna'                       : 'idna>=3.10.0',
        'binaryornot'                : 'binaryornot>=0.4.4',
        'pyvim'                      : 'pyvim>=3.0.2',
        'ptpython'                   : 'ptpython>=3.0.27',
        'aiofiles'                   : 'aiofiles>=0.6.0',
        'packaging'                  : 'packaging>=21.3.0',
        'prompt_toolkit'             : 'prompt-toolkit>=3.0.39',
        'more_itertools'             : 'more-itertools>=8.7.0',
        'virtualenv'                 : 'virtualenv>=20.1.0',
        'attrs'                      : 'attrs>=24.2.0',
        'uv'                         : 'uv>=0.2.11',
        'pydantic'                   : 'pydantic>=2.11.7',
        'annotated-types'            : 'annotated-types>=0.7.0',
        'importlib_metadata'         : 'importlib-metadata>=4.12.0',
    },
    '_internal'                      : {
        'dataclass_wizard'           : 'dataclass-wizard>=0.35.0',
    },
    'jobs': {
        'dill'                       : 'dill>=0.4.0',
        'daemon'                     : 'python-daemon>=3.1.2',
        'watchfiles'                 : 'watchfiles>=1.1.0',
        'psutil'                     : 'psutil>=7.0.0',
    },
    'drivers': {
        'cryptography'               : 'cryptography>=38.0.1',
        'psycopg'                    : 'psycopg[binary]>=3.2.9',
        'psycopg2'                   : 'psycopg2-binary>=2.9.11',
        'pymysql'                    : 'PyMySQL>=0.9.0',
        'aiomysql'                   : 'aiomysql>=0.0.21',
        'sqlalchemy_cockroachdb'     : 'sqlalchemy-cockroachdb>=2.0.0',
        'duckdb'                     : 'duckdb>=1.5.6',
        'duckdb_engine'              : 'duckdb-engine>=0.17.0',
    },
    'drivers-extras': {
        'pyodbc'                     : 'pyodbc>=4.0.30',
        'oracledb'                   : 'oracledb>=2.5.0',
        'adbc_driver_postgresql'     : 'adbc-driver-postgresql>=1.7.0',
        'adbc_driver_sqlite'         : 'adbc-driver-sqlite>=1.7.0',
    },
    'cli': {
        'pgcli'                      : 'pgcli>=3.1.0',
        'mycli'                      : 'mycli>=1.23.2',
        'litecli'                    : 'litecli>=1.5.0',
        'mssqlcli'                   : 'mssql-cli>=1.0.0',
        'gadwall'                    : 'gadwall>=0.2.0',
    },
    'gis'                            : {
        'pyproj'                     : 'pyproj>=3.7.1',
        'geopandas'                  : 'geopandas>=1.0.1', 
        'shapely'                    : 'shapely>=2.0.7',
        'geoarrow'                   : 'geoarrow-types>=0.3.0',
    },
    'stack': {
        'compose'                    : 'docker-compose>=1.29.2',
    },
    'compose': {
        'envyaml'                    : 'envyaml>=1.10.211231',
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
        'zensical'                   : 'zensical>=0.0.30',
    },
    'gui': {
        'webview'                    : 'pywebview>=3.6.3',
        'pycparser'                  : 'pycparser>=2.21.0',
    },
    'extras': {
        'cmd2'                       : 'cmd2>=1.4.0',
        'ruamel.yaml'                : 'ruamel.yaml>=0.16.12',
        'modin'                      : 'modin[ray]>=0.8.3',
        'nanoid'                     : 'nanoid>=2.0.0',
    },
}
packages['sql'] = {
    'numpy'                          : 'numpy>=2.3.1',
    'pandas'                         : 'pandas[parquet]>=3.0.0',
    'pyarrow'                        : 'pyarrow>=20.0.0',
    'polars'                         : 'polars>=1.43.2',
    'dask'                           : 'dask[complete]>=2024.12.1',
    'partd'                          : 'partd>=1.4.2',
    'pytz'                           : 'pytz',
    'joblib'                         : 'joblib>=1.5.1',
    'sqlalchemy'                     : 'SQLAlchemy>=2.0.41',
    'geoalchemy2'                    : 'GeoAlchemy2>=0.18.0',
    'databases'                      : 'databases>=0.9.0',
    'aiosqlite'                      : 'aiosqlite>=0.21.0',
    'asyncpg'                        : 'asyncpg>=0.30.0',
}
packages['sql'].update(packages['drivers'])
packages['sql'].update(packages['core'])
packages['sql'].update(packages['gis'])
packages['polars'] = {
    'polars'                         : packages['sql']['polars'],
    'pandas'                         : packages['sql']['pandas'],
    'pyarrow'                        : packages['sql']['pyarrow'],
    'geoarrow'                       : packages['gis']['geoarrow'],
}
### The web console (Plotly Dash, served by the API host; design D21). `dash_daq`, `flask_compress`,
### `terminado` and `tornado` (v4's console) are not used any more: the terminal is the Rust server's.
packages['dash'] = {
    'dash'                           : 'dash>=4.1.0',
    'pygments'                       : 'pygments>=2.21.0',
    'dash_bootstrap_components'      : 'dash-bootstrap-components>=2.0.4',
    'dash_ace'                       : 'dash-ace>=0.2.1',
    'dash_extensions'                : 'dash-extensions>=2.0.5',
    'a2wsgi'                         : 'a2wsgi>=1.10.0',
}
packages['api'] = {
    'uvicorn'                        : 'uvicorn[standard]>=0.35.0',
    'gunicorn'                       : 'gunicorn>=23.0.0',
    'dotenv'                         : 'python-dotenv>=1.1.1',
    'websockets'                     : 'websockets>=15.0.1',
    'fastapi'                        : 'fastapi>=0.128.0',
    'fastapi_login'                  : 'fastapi-login>=1.10.3',
    'multipart'                      : 'python-multipart>=0.0.20',
    'httpx'                          : 'httpx>=0.28.1',
    'httpcore'                       : 'httpcore>=1.0.9',
    'valkey'                         : 'valkey>=6.1.0',
    'jose'                           : 'python-jose>=3.5.0',
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

### The v5 distribution extras (docs/design/v5.0.0.md §12.1, D20): `pip install meerschaum[<extra>]`.
### This is the source for `[project.optional-dependencies]` in the root pyproject.toml;
### `scripts/check_packaging.py` fails if the two drift apart. `{import_name: install_name}`.
### The v4 groups above stay for the transitional Python code (`attempt_import`, `mrsm upgrade
### packages <group>`, `mrsm show packages <group>`): the groups that share a name with an extra
### (`sql`, `api`, `dash`, `polars`, `gui`, `full`) keep their v4 contents until that code moves
### to Rust, while the new-only names (`pandas`, `geo`, `shell`, `cli-tools`, `sql-*`) are real
### groups too.
_extras_packages: Dict[str, Dict[str, str]] = {
    'pandas': {
        'pandas'                     : 'pandas>=3.0.0',
        'pyarrow'                    : 'pyarrow>=20.0.0',
    },
    'polars': {
        'polars'                     : 'polars>=1.43.2',
    },
    'geo': {
        'shapely'                    : 'shapely>=2.0.7',
        'pyproj'                     : 'pyproj>=3.7.1',
        'geopandas'                  : 'geopandas>=1.0.1',
        'geoarrow'                   : 'geoarrow-types>=0.3.0',
    },
    'sql': {
        'sqlalchemy'                 : 'SQLAlchemy>=2.0.41',
        'psycopg'                    : 'psycopg[binary]>=3.2.9',
        'pymysql'                    : 'PyMySQL>=0.9.0',
    },
    'sql-mssql': {
        'pyodbc'                     : 'pyodbc>=4.0.30',
    },
    'sql-oracle': {
        'oracledb'                   : 'oracledb>=2.5.0',
    },
    'sql-duckdb': {
        'duckdb'                     : 'duckdb>=1.5.6',
        'duckdb_engine'              : 'duckdb-engine>=0.17.0',
    },
    'sql-cockroachdb': {
        'sqlalchemy_cockroachdb'     : 'sqlalchemy-cockroachdb>=2.0.0',
    },
    'api': {
        'fastapi'                    : 'fastapi>=0.128.0',
        'uvicorn'                    : 'uvicorn>=0.35.0',
        'a2wsgi'                     : 'a2wsgi>=1.10.0',
        'multipart'                  : 'python-multipart>=0.0.20',
        'jwt'                        : 'PyJWT>=2.8.0',
        'websockets'                 : 'websockets>=15.0.1',
    },
    'dash': {
        'dash'                       : 'dash>=4.1.0',
        'pygments'                   : 'pygments>=2.21.0',
        'a2wsgi'                     : 'a2wsgi>=1.10.0',
        'dash_bootstrap_components'  : 'dash-bootstrap-components>=2.0.4',
        'dash_ace'                   : 'dash-ace>=0.2.1',
        ### The console builds its app with `dash_extensions.enrich.DashProxy` (`Trigger` and
        ### duplicate outputs, which plugins written for v4's console use).
        'dash_extensions'            : 'dash-extensions>=2.0.5',
    },
    'shell': {
        'ptpython'                   : 'ptpython>=3.0.27',
    },
    'cli-tools': {
        'pgcli'                      : 'pgcli>=3.1.0',
        'mycli'                      : 'mycli>=1.23.2',
        'litecli'                    : 'litecli>=1.5.0',
        'gadwall'                    : 'gadwall>=0.2.0',
    },
    'gui': {
        'webview'                    : 'pywebview>=3.6.3',
    },
}
### `[full]` is the union of these extras (`cli-tools` and `gui` are excluded, as in v4).
_full_extras = ('pandas', 'polars', 'geo', 'sql', 'api', 'dash', 'shell')

### Make the new-only groups available to `show/upgrade packages` and the install-name lookup.
for _group, _import_names in _extras_packages.items():
    if _group not in packages:
        packages[_group] = dict(_import_names)
    for _import_name, _install_name in _import_names.items():
        all_packages.setdefault(_import_name, _install_name)

extras: Dict[str, List[str]] = {
    group: list(dict.fromkeys(_import_names.values()))
    for group, _import_names in _extras_packages.items()
}
### The console runs in the API host and shows pipe data as pandas frames, so `[dash]` brings the
### `api` and `pandas` extras with it (`pip install meerschaum[dash]` is a working console).
extras['dash'] = list(dict.fromkeys(extras['dash'] + extras['api'] + extras['pandas']))
extras['full'] = list(dict.fromkeys(
    install_name
    for group in _full_extras
    for install_name in extras[group]
))
