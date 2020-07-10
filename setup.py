#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools

exec(open('meerschaum/_version.py').read())

with open('README.md', 'r') as readmefile:
    long_desc = readmefile.read()

setuptools.setup(
    name = 'meerschaum',
    version = __version__,
    description = 'The Meerschaum Project software library',
    url = '#',
    author = 'Bennett Meares',
    author_email = 'bennett.meares@gmail.com',
    license = 'MIT',
    packages = setuptools.find_packages(),
    install_requires = [],
    zip_safe = True,
    package_data = {},
    python_requires = '>=3.8'
)
