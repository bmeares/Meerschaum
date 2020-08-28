#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools

exec(open('meerschaum/_version.py').read())

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'The Meerschaum Project software library',
    url = '#',
    author = 'Bennett Meares',
    author_email = 'bennett.meares@gmail.com',
    license = '',
    packages = setuptools.find_packages(),
    install_requires = [
        'sqlalchemy',
    ],
    entry_points = {
        'console_scripts' : [
            'meerschaum = meerschmaum.main:main',
            'Meerschaum = meerschmaum.main:main',
            'mrsm = meerschmaum.main:main'
        ],
    },
    zip_safe = True,
    package_data = {},
    python_requires = '>=3.8'
)
