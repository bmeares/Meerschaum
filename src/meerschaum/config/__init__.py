#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import and update configuration dictionary
and if interactive, print the welcome message
"""

from meerschaum.config._version import __version__
from meerschaum.config._read_yaml import config, config_filename, config_path

### developer-specified values
system_config = {
    'connectors' : {
        'all' : {
            ### pandas implementation
            ### (change to modin.pandas when to_sql works)
            'pandas' : 'pandas',
        },
        'sql' : {
            'bulk_insert_threshold' : 10000,
            'method' : 'multi',
            'chunksize' : 1000,
            'pool_size' : 5,
            'max_overflow' : 10,
            'pool_recycle': 3600,
            'poolclass' : 'sqlalchemy.pool.QueuePool',
        },
        'api' : {
        },
    },
    'shell' : {
        'timeout' : 15,
    }
}
config['system'] = system_config

### if interactive shell, print welcome header
import sys
__doc__ = f"Meerschaum v{__version__}"
try:
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False
if interactive:
    print(__doc__)

