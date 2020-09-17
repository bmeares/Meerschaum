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
        from meerschaum.actions import entry
        from meerschaum.config._paths import CONFIG_PATH, PATCH_PATH
        import os, shutil
        if CONFIG_PATH.is_file():
            print(f"Found existing configuration in {CONFIG_PATH}")
            print(f"Moving to {PATCH_PATH} and patching default configuration with existing configuration")
            shutil.copy(CONFIG_PATH, PATCH_PATH)
        else:
            print(f"Configuration not found: {CONFIG_PATH}")

        #  entry(['bootstrap', 'config', '--yes', '--force'])
        #  entry(['bootstrap', 'stack', '--yes', '--force'])

required = [
    'pyyaml',
    'cascadict',
    'pprintpp',
    'requests',
    'pyvim',
]

extras = {
    'full' : [
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',
        'uvicorn',
        'fastapi',
        'databases',
        'aiosqlite',
        'asyncpg',
        'graphene',
        'starlette',
    ],
}

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'Create and Manage Pipes with Meerschaum',
    url = 'https://github.com/bmeares/Meerschaum',
    author = 'Bennett Meares',
    author_email = 'bennett.meares@gmail.com',
    license = 'MIT',
    packages = setuptools.find_packages(),
    install_requires = required,
    extras_require = extras,
    entry_points = {
        'console_scripts' : [
            'meerschaum = meerschaum.__main__:main',
            'Meerschaum = meerschaum.__main__:main',
            'mrsm = meerschaum.__main__:main'
        ],
    },
    cmdclass = {
        'install' : PostInstallCommand,
    },
    zip_safe = True,
    package_data = {'' : ['*.yaml', '*.env']},
    python_requires = '>=3.8'
)
