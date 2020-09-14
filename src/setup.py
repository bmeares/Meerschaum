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
        entry(['bootstrap', 'config', '--debug', '--yes', '--force'])
        entry(['bootstrap', 'stack', '--debug', '--yes', '--force'])

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'The Meerschaum Project software library',
    url = 'https://github.com/bmeares/Meerschaum',
    author = 'Bennett Meares',
    author_email = 'bennett.meares@gmail.com',
    license = 'MIT',
    packages = setuptools.find_packages(),
    install_requires = [
        'sqlalchemy',
        'pandas',
        'psycopg2-binary',
        'pyyaml',
        'lazy_import',
        'fastapi',
        'uvicorn',
        'databases',
        'aiosqlite',
        'graphene',
        'asyncpg',
        'cascadict',
        'pprintpp',
        'requests',
    ],
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
