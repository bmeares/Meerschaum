#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import sys
from pathlib import Path
from setuptools import setup
from setuptools.command.install import install

sys.path.insert(0, str(Path(__file__).resolve().parent))

with open('meerschaum/utils/packages/_packages.py', encoding='utf-8') as packages_file:
    exec(packages_file.read())

setup(
    install_requires = extras.get('required', []),
    extras_require = extras,
)
