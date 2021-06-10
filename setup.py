#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import setuptools, sys
from setuptools import setup
from setuptools.command.install import install
cx_Freeze, Executable = None, None
#  if 'build_exe' in sys.argv or 'build_msi' in sys.argv or 'build_dmg' in sys.argv:
    #  try:
        #  import cx_Freeze
        #  from cx_Freeze import setup, Executable
    #  except ImportError:
        #  cx_Freeze, Executable = None, None

### read values from within the package
exec(open('meerschaum/config/_version.py').read())
exec(open('meerschaum/utils/packages/_packages.py').read())

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        from meerschaum.config._edit import write_default_config
        from meerschaum.config._paths import CONFIG_DIR_PATH, PERMANENT_PATCH_DIR_PATH
        import os, shutil

        if CONFIG_DIR_PATH.exists():
            print(f"Found existing configuration in '{CONFIG_DIR_PATH}'")
            print(f"Moving to '{PERMANENT_PATCH_PATH}' and patching default configuration with existing configuration")
            shutil.move(CONFIG_DIR_PATH, PERMANENT_PATCH_DIR_PATH)
        else:
            print(f"Configuration not found: '{CONFIG_DIR_PATH}'")

extras = {}
### NOTE: package dependencies are defined in meerschaum.utils.packages._packages
for group in packages:
    extras[group] = [ install_name for import_name, install_name in packages[group].items() ]

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

setup_kw_args = {
    'name'                          : 'meerschaum',
    'version'                       : __version__,
    'description'                   : 'Create and Manage Pipes with Meerschaum',
    'long_description'              : readme,
    'long_description_content_type' : 'text/markdown',
    'url'                           : 'https://meerschaum.io',
    'author'                        : 'Bennett Meares',
    'author_email'                  : 'bennett.meares@gmail.com',
    'maintainer_email'              : 'bennett.meares@gmail.com',
    'license'                       : 'Apache Software License 2.0',
    'packages'                      : setuptools.find_packages(exclude=['*tests*']),
    'install_requires'              : extras['required'],
    'extras_require'                : extras,
    'entry_points'                  : {
        'console_scripts'           : [
            'mrsm = meerschaum.__main__:main',
            'meerschaum = meerschaum.__main__:main',
        ],
    },
    #  'cmdclass'                      : {
        #  'install'                   : PostInstallCommand,
    #  },
    'zip_safe'                      : True,
    'package_data'                  : {'' : ['*.html', '*.css', '*.js', '*.png', '*.ico']},
    'python_requires'               : '>=3.7',
    'classifiers'                   : [
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
    ],
}
#  if cx_Freeze is not None and Executable is not None:
    #  setup_kw_args['options'] = {
        #  'build_exe' : {
            #  'packages' : setuptools.find_packages(),
            #  'includes' : [
                #  'pip', 'venv', 'os', 'sys', 'importlib', 'pathlib',
            #  ],
            #  'optimize' : 0,
        #  },
    #  }
    #  setup_kw_args['executables'] = [
        #  Executable(
            #  "meerschaum/__main__.py",
            #  target_name = 'mrsm',
        #  )
    #  ]


setup(**setup_kw_args)
