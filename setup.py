#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools, sys, platform
from setuptools import setup
from setuptools.command.install import install
cx_Freeze, Executable = None, None

with open('meerschaum/config/_version.py', encoding='utf-8') as version_file:
    exec(version_file.read())
with open('meerschaum/utils/packages/_packages.py', encoding='utf-8') as packages_file:
    exec(packages_file.read())
with open('meerschaum/config/static/__init__.py', encoding='utf-8') as static_file:
    exec(static_file.read())
with open('README.md', 'r', encoding='utf-8') as readme_file:
    readme = readme_file.read()

setup_cf = STATIC_CONFIG['setup']

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        from meerschaum.actions import actions
        try:
            actions['verify'](['packages'], debug=False)
        except Exception:
            pass

extras = {}
### NOTE: package dependencies are defined in meerschaum.utils.packages._packages
for group in packages:
    if group.startswith('_'):
        continue
    extras[group] = [ install_name for import_name, install_name in packages[group].items() ]

setup_kw_args = {
    'name'                          : setup_cf['name'],
    'version'                       : __version__,
    'description'                   : setup_cf['description'],
    'long_description'              : readme,
    'long_description_content_type' : 'text/markdown',
    'url'                           : setup_cf['url'],
    'project_urls'                  : setup_cf['project_urls'],
    'author'                        : setup_cf['author'],
    'author_email'                  : setup_cf['author_email'],
    'maintainer_email'              : setup_cf['maintainer_email'],
    'license'                       : setup_cf['license'],
    'packages'                      : setuptools.find_packages(exclude=['*test*']),
    'install_requires'              : extras['required'],
    'extras_require'                : extras,
    'entry_points'                  : {
        'console_scripts'           : [
            'mrsm = meerschaum.__main__:main',
            'meerschaum = meerschaum.__main__:main',
        ],
    },
    'cmdclass'                      : {
        'install'                   : PostInstallCommand,
    },
    'zip_safe'                      : True,
    'package_data'                  : {'' : ['*.html', '*.css', '*.js', '*.png', '*.ico']},
    'python_requires'               : '>=3.8',
    'classifiers'                   : [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Database",
        "Natural Language :: English",
    ],
}

setup(**setup_kw_args)
