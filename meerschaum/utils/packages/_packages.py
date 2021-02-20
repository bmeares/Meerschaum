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

packages = {
    'required' : {
        'wheel' : 'wheel',
        'yaml' : 'PyYAML>=5.3.1',
        'cascadict' : 'cascadict',
    },
    'minimal' : {},
    'formatting' : {
        'pprintpp'          : 'pprintpp',
        'asciitree'         : 'asciitree',
        'typing_extensions' : 'typing_extensions',
        'pygments'          : 'pygments',
        'colorama'          : 'colorama',
        'rich'              : 'rich>=9.8.0',
        'more_termcolor'    : 'more-termcolor',

    },
    '_required' : {
        'pip'               : 'pip',
        'update_checker'    : 'update-checker',
        'semver'            : 'semver',
        'dateutil'          : 'python-dateutil',
        'requests'          : 'requests',
        'pyvim'             : 'pyvim',
        'aiofiles'          : 'aiofiles',
        'packaging'         : 'packaging',
        'prompt_toolkit'    : 'prompt-toolkit',
    },
    'iot' : {
        'paho' : 'paho-mqtt',
    },
    'drivers' : {
        'psycopg2' : 'psycopg2-binary',
        'pymysql'  : 'pymysql',
    },
    'cli' : {
        'pgcli'    : 'pgcli',
        'mycli'    : 'mycli',
        'litecli'  : 'litecli',
        'mssqlcli' : 'mssqlcli',
    },
    'stack' : {
        'docker'  : 'docker',
        'compose' : 'docker-compose',
    },
    'build' : {
        'cx_Freeze'   : 'cx_Freeze>=6.5.1',
        'PyInstaller' : 'pyinstaller',
    },
    'dev-tools' : {
        'tuna'     : 'tuna',
        'snakeviz' : 'snakeviz',
        'mypy' : 'mypy',
        'pytest' : 'pytest',
    },
    'docs' : {
        'mkdocs'                      : 'mkdocs',
        'pdoc'                        : 'pdoc3',
        'mkdocs_material'             : 'mkdocs-material',
        'mkdocs_autolinks_plugin'     : 'mkdocs-autolinks-plugin',
        'mkdocs_awesome_pages_plugin' : 'mkdocs-awesome-pages-plugin',
    },
    'portable' : {
        'pyreadline' : 'pyreadline; platform_system == "Windows"',
        'gnureadline' : 'gnureadline; platform_system != "Windows"',
    },
    'extras' : {
        'virtualenv' : 'virtualenv',
        'cmd2' : 'cmd2',
        'ruamel.yaml' : 'ruamel.yaml',
    },
}
packages['sql'] = {
    'numpy'      : 'numpy',
    'pandas'     : 'pandas',
    'sqlalchemy' : 'sqlalchemy',
    'databases'  : 'databases',
    'aiosqlite'  : 'aiosqlite',
    'asyncpg'    : 'asyncpg',
}
packages['sql'].update(packages['drivers'])
packages['api'] = {
    'uvicorn'       : 'uvicorn',
    'websockets'    : 'websockets',
    'fastapi'       : 'fastapi',
    'jinja2'        : 'jinja2',
    'passlib'       : 'passlib',
    'fastapi_login' : 'fastapi-login',
    'multipart'     : 'python-multipart',
}
packages['api'].update(packages['sql'])
packages['api'].update(packages['_required'])
packages['api'].update(packages['formatting'])

all_packages = dict()
for group, import_names in packages.items():
    all_packages.update(import_names)

skip_groups = {'docs', 'build', 'cli', 'dev-tools', 'portable', 'extras'}
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

