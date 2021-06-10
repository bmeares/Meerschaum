#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The default configuration values to write to config.yaml.
"""

import sys, os, multiprocessing

from meerschaum.connectors import attributes as connector_attributes

default_meerschaum_config = {
    'instance' : 'sql:main',
    'api_instance' : 'MRSM{meerschaum:instance}',
    'web_instance' : 'MRSM{meerschaum:instance}',
    'default_repository' : 'api:mrsm',
    'connectors' : {
        'sql' : {
            #  'default'      : connector_attributes['sql']['flavors']['timescaledb']['defaults'],
            'default'      : {},
            'main'         : {
                'username' : 'mrsm',
                'password' : 'mrsm',
                'flavor'   : 'timescaledb',
                'host'     : 'localhost',
                'database' : 'meerschaum',
                'port'     : 5432,
            },
            'local'        : {
                'flavor'   : 'sqlite',
            },
            #  'mrsm'         : {
                #  'host'     : 'mrsm.io',
            #  },
        },
        'api' : {
            'default'      : connector_attributes['api']['default'],
            'main'         : {
                'host'     : 'localhost',
                'port'     : 8000,
            },
            'local'        : {
                'host'     : 'localhost',
            },
            'mrsm'         : {
                'host'     : 'api.mrsm.io',
                'port'     : 443,
                'protocol' : 'https',
            },
        },
        'mqtt' : {
            'default'      : connector_attributes['mqtt']['default'],
            'main'         : {
                'host'     : 'localhost',
                'port'     : 1883,
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
            'chunksize'    : 900,
            'poolclass'    : 'sqlalchemy.pool.QueuePool',
            'create_engine' : {
                'method'       : 'multi',
                'pool_size'    : 5,
                'max_overflow' : 10,
                'pool_recycle' : 3600,
                'connect_args' : {},
            },
        },

        'api' : {
        },
    },
    ### not to be confused with system_config['connectors']['api'], this is the configuration
    ### for the API server itself.
    'api' : {
        'uvicorn'          : {
            'app'          : 'meerschaum.api:app',
            'port'         : default_meerschaum_config['connectors']['api']['default']['port'],
            'host'         : '0.0.0.0',
            'workers'      : max(int(multiprocessing.cpu_count() / 2), 1),
        },
        'permissions':       {
            'registration' : {
                'users'    : False,
                'pipes'    : True,
                'plugins'  : True,
            },
            'actions'      : {
                'non_admin': False,
            },
            'chaining' : {
                'insecure_parent_instance' : False,
                'child_apis' : False,
            },
        },
        'protocol'         : default_meerschaum_config['connectors']['api']['default']['protocol'],
    },
    'experimental': {
        'fetch': False,
        'cache': True,
        'space': False,
        'join_fetch': False,
    },
}
default_pipes_config       = {
    'parameters'           : {
        'columns'          : {
            'datetime'     : None,
            'id'           : None,
        },
    },
}
default_plugins_config     = {}
default_experimental_config = {
    'venv' : True,
}



### build default config dictionary
default_config = dict()
default_config['meerschaum'] = default_meerschaum_config
default_config['system'] = default_system_config
from meerschaum.config._formatting import default_formatting_config
default_config['formatting'] = default_formatting_config
from meerschaum.config._shell import default_shell_config
default_config['shell'] = default_shell_config
default_config['pipes'] = default_pipes_config
default_config['plugins'] = default_plugins_config
from meerschaum.config._jobs import default_jobs_config
default_config['jobs'] = default_jobs_config
#  default_config['experimental'] = default_experimental_config
### add configs from other packages
try:
    import meerschaum.config.stack
except ImportError as e:
    pass
finally:
    from meerschaum.config.stack import default_stack_config
default_config['stack'] = default_stack_config

default_header_comment = """
#####################################################################
#                                                                   #
#  Edit or add credentials for connectors.                          #
#  You can read more about connectors at https://meerschaum.io.     #
#                                                                   #
#  Connectors inherit from `default`, and flavor-dependent defaults #
#  are defined for SQL connectors (e.g. port 5432 for PostgreSQL).  #
#                                                                   #
#####################################################################

"""
