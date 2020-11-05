#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools
from setuptools.command.install import install

exec(open('meerschaum/config/_version.py').read())

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        from meerschaum.config._paths import CONFIG_PATH, PERMANENT_PATCH_PATH
        import os, shutil
        if CONFIG_PATH.exists():
            print(f"Found existing configuration in {CONFIG_PATH}")
            print(f"Moving to {PERMANENT_PATCH_PATH} and patching default configuration with existing configuration")
            shutil.move(CONFIG_PATH, PERMANENT_PATCH_PATH)
        else:
            print(f"Configuration not found: {CONFIG_PATH}")

required = [
    'python-dateutil',
    'PyYAML',
    'cascadict',
    'pprintpp',
    'requests',
    'pyvim',
    'colorama',
    'more_termcolor',
    'aiofiles',
    'cmd2',
]
iot = [
    'paho-mqtt',
]
### TODO bake drivers into Docker image
drivers = [
    'psycopg2-binary',
    'pymysql',
    #  'pyodbc',
]
cli = [
    'pgcli',
    'mycli',
    'litecli',
]
analysis = [
    #  'pandasgui',
]
sql = drivers + [
    'pandas',
    'sqlalchemy',
    'databases',
    'aiosqlite',
    'asyncpg',
]
api = sql + [
    'uvicorn',
    'fastapi',
    #  'graphene',
    'jinja2',
]
stack = [
    'docker',
    'splitgraph',
    'docker-compose',
]
extras = {
    'drivers' : drivers,
    'cli' : cli,
    'sql' : sql,
    'api' : api,
    'iot' : iot,
    'analysis' : analysis,
    'stack' : stack,
}
full = set()
for k, dependencies in extras.items():
    if k == 'cli': continue
    for dependency in dependencies:
        full.add(dependency)
extras['full'] = list(full)

with open('README.md', 'r') as f:
    readme = f.read()

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'Create and Manage Pipes with Meerschaum',
    long_description = readme,
    long_description_content_type = 'text/markdown',
    url = 'https://meerschaum.io',
    author = 'Bennett Meares',
    author_email = 'bennett.meares@gmail.com',
    maintainer = 'Bennett Meares',
    maintainer_email = 'bennett.meares@gmail.com',
    license = 'MIT',
    packages = setuptools.find_packages(),
    install_requires = required,
    extras_require = extras,
    entry_points = {
        'console_scripts' : [
            'meerschaum = meerschaum.__main__:main',
            'Meerschaum = meerschaum.__main__:main',
            'mrsm = meerschaum.__main__:main',
        ],
    },
    cmdclass = {
        'install' : PostInstallCommand,
    },
    zip_safe = True,
    package_data = {'' : ['*.html', '*.css', '*.js']},
    python_requires = '>=3.7',
    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
    ]
)
