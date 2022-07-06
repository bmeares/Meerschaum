#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools, sys, platform
from setuptools import setup
from setuptools.command.install import install
cx_Freeze, Executable = None, None

### read values from within the package
exec(open('meerschaum/config/_version.py').read())
exec(open('meerschaum/utils/packages/_packages.py').read())
exec(open('meerschaum/config/static/__init__.py').read())
setup_cf = _static_config()['setup']

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        from meerschaum.actions import actions
        try:
            actions['verify'](['packages'], debug=False)
        except Exception as e:
            pass

extras = {}
### NOTE: package dependencies are defined in meerschaum.utils.packages._packages
for group in packages:
    extras[group] = [ install_name for import_name, install_name in packages[group].items() ]

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

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
    'packages'                      : setuptools.find_packages(exclude=['*tests*']),
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
    'python_requires'               : '>=3.7',
    'classifiers'                   : [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Natural Language :: English",
    ],
}

setup(**setup_kw_args)
