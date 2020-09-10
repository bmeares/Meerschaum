#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools

exec(open('meerschaum/config/_version.py').read())

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'The Meerschaum Project software library',
    url = '#',
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
    ],
    entry_points = {
        'console_scripts' : [
            'meerschaum = meerschaum.__main__:main',
            'Meerschaum = meerschaum.__main__:main',
            'mrsm = meerschaum.__main__:main'
        ],
    },
    zip_safe = True,
    package_data = {'' : ['*.yaml']},
    python_requires = '>=3.8'
)
