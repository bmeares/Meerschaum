#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The default configuration values to write to config.yaml.
"""

#  from meerschaum.utils.misc import generate_password
import yaml, sys, os.path
default_meerschaum_config = {
    'connectors' : {
        'sql' : {
            'default'      : {
                'username' : 'meerschaum',
                'password' : 'meerschaum',
            },
            'main'         : {
                'username' : 'meerschaum',
                'password' : 'meerschaum',
                'flavor'   : 'timescaledb',
                'host'     : 'localhost',
                'database' : 'mrsm_main',
                'port'     : 5432,
            },
            'meta'         : {},
            'local'        : {
                'flavor'   : 'sqlite',
            },
        },
        'api' : {
            'default'      : {
                'username' : 'meerschaum',
                'password' : 'meerschaum',
                'protocol' : 'http',
                'port'     : 8000,
            },
            'main'         : {
                'host'     : 'localhost',
                'port'     : 8000,
            },
            'local'        : {
                'host'     : 'localhost',
            },
        },
    },
}
default_system_config = {
    'connectors' : {
        'all' : {
            ### pandas implementation
            ### (change to modin.pandas when to_sql works)
            'pandas'       : 'pandas',
        },
        'sql' : {
            'method'       : 'multi',
            'chunksize'    : 1000,
            'pool_size'    : 5,
            'max_overflow' : 10,
            'pool_recycle' : 3600,
            'poolclass'    : 'sqlalchemy.pool.QueuePool',
            'connect_args' : {},
        },

        'api' : {
        },
    },
    'shell' : {
        'timeout'          : 60,
        'prompt'           : '𝚖𝚛𝚜𝚖 ➤ ',
        'ruler'            : '─',
        'close_message'    : 'Thank you for using Meerschaum!',
        'doc_header'       : 'Meerschaum actions (`help <action>` for usage):',
        'undoc_header'     : 'Unimplemented actions:',
        'max_history'      : 1000,
    },
    ### not to be confused with system_config['connectors']['api']
    'api' : {
        'uvicorn'          : {
            'app'          : 'meerschaum.api:fast_api',
            'port'         : default_meerschaum_config['connectors']['api']['default']['port'],
            'host'         : '0.0.0.0',
            'workers'      : 4,
        },
        'username'         : default_meerschaum_config['connectors']['api']['default']['username'],
        'password'         : default_meerschaum_config['connectors']['api']['default']['password'],
        'protocol'         : default_meerschaum_config['connectors']['api']['default']['protocol'],
        'endpoints'        : {
            'mrsm'         : '/mrsm',
        },
    },
    'arguments' : {
        'sub_decorators'   : ['[', ']'],
    },
}

### if using Windows, switch to ASCII
### TODO define fancy graphics mode in formatting module
if 'win' in sys.platform:
    default_system_config['shell']['prompt'] = 'mrsm > '

from meerschaum.config._paths import RESOURCES_PATH, DEFAULT_CONFIG_PATH

### build default config dictionary
default_config = dict()
default_config['meerschaum'] = default_meerschaum_config
default_config['system'] = default_system_config
### add configs from other packages
from meerschaum.config.stack import default_stack_config
default_config['stack'] = default_stack_config

default_header_comment = """
##################################################################################
#                                                                                #
#  Edit the credentials below for the `main` connectors or add new connectors.   #
#                                                                                #
#  Connectors inherit from `default`, and flavor-dependent defaults are defined  #
#  for SQL connectors (e.g. port 5432 for PostgreSQL).                           #
#                                                                                #
#################################################################################

"""

