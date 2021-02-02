#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The default configuration values to write to config.yaml.
"""

import sys, os, multiprocessing

default_unicode, default_ansi = True, True
import platform
if platform.system() == 'Windows':
    default_unicode, default_ansi = False, True

from meerschaum.connectors import attributes as connector_attributes

default_meerschaum_config = {
    'instance' : 'sql:main',
    'api_instance' : 'sql:main',
    'default_repository' : 'api:mrsm',
    'connectors' : {
        'sql' : {
            'default'      : connector_attributes['sql']['flavors']['timescaledb']['defaults'],
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
                'host'     : 'mrsm.io',
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
            'method'       : 'multi',
            'chunksize'    : 900,
            'pool_size'    : 5,
            'max_overflow' : 10,
            'pool_recycle' : 3600,
            'poolclass'    : 'sqlalchemy.pool.QueuePool',
            'connect_args' : {},
        },

        'api' : {
        },
    },
    ### control output colors and Unicode vs ASCII
    'formatting'           : {
        'unicode'          : default_unicode,
        'ansi'             : default_ansi,
        'emoji'            : {
            'hand'         : '👋',
            'error'        : '🛑',
            'failure'      : '💢',
            'success'      : '🎉',
            'info'         : '💬',
            'debug'        : '🐞',
            'question'     : '❓',
        },
    },
    'shell' : {
        'ansi'             : {
            'intro'        : {
                #  'style'    : "bold bright_blue",
                'color'    : [
                    'bold',
                    'bright blue',
                ],
            },
            'close_message': {
                #  'style'    : "bright_blue",
                'color'    : [
                    'bright blue',
                ],
            },
            'doc_header': {
                #  'style'    : "bright_blue",
                'color'    : [
                    'bright blue',
                ],
            },
            'undoc_header': {
                #  'style'    : "bright_blue",
                'color'    : [
                    'bright blue',
                ],
            },
            'ruler': {
                #  'style'    : "bright_blue",
                'color'    : [
                    'bold',
                    'bright blue',
                ],
            },
            'prompt': {
                #  'style'    : "green",
                'color'    : [
                    'green',
                ],
            },
            'instance' : {
                #  'style'    : "cyan",
                'color'    : [
                    'cyan',
                ],
            },
            'username' : {
                #  'style'    : "white",
                'color'    : [
                    'white',
                ],
            },
        },
        'ascii'            : {
            'intro'        : """       ___  ___  __   __   __
 |\/| |__  |__  |__) /__` /  ` |__|  /\  |  |  |\/|
 |  | |___ |___ |  \ .__/ \__, |  | /~~\ \__/  |  |\n""",
            'prompt'       : '\n [ {username}@{instance} ] > ',
            'ruler'        : '-',
            'close_message': 'Thank you for using Meerschaum!',
            'doc_header'   : 'Meerschaum actions (`help <action>` for usage):',
            'undoc_header' : 'Unimplemented actions:',
        },
        'unicode'          : {
            'intro'        : """
 █▄ ▄█ ██▀ ██▀ █▀▄ ▄▀▀ ▄▀▀ █▄█ ▄▀▄ █ █ █▄ ▄█
 █ ▀ █ █▄▄ █▄▄ █▀▄ ▄██ ▀▄▄ █ █ █▀█ ▀▄█ █ ▀ █\n""",
            'prompt'       : '\n [ {username}@{instance} ] ➤ ',
            'ruler'        : '─',
            'close_message': ' MRSM{system:formatting:emoji:hand} Thank you for using Meerschaum! ',
            'doc_header'   : 'Meerschaum actions (`help <action>` for usage):',
            'undoc_header' : 'Unimplemented actions:',
        },
        'timeout'          : 60,
        'max_history'      : 1000,
        'clear_screen'     : True,
        'cmd'              : 'cmd2',
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
        },
        'protocol'         : default_meerschaum_config['connectors']['api']['default']['protocol'],
    },
    'arguments'            : {
        'sub_decorators'   : ['[', ']'],
    },
    'warnings'             : {
        'unicode'          : {
            'icon'         : '⚠',
        },
        'ascii'            : {
            'icon'         : 'WARNING',
        },
        'ansi'             : {
            'color'        : [
                'bold',
                'yellow',
            ],
        },
    },
    'success'              : {
        'unicode'          : {
            'icon'         : 'MRSM{system:formatting:emoji:success}',
        },
        'ascii'            : {
            'icon'         : '+',
        },
        'ansi'             : {
            'color'        : [
                'bold',
                'bright green',
            ],
        },
    },
    'failure'              : {
        'unicode'          : {
            'icon'         : 'MRSM{system:formatting:emoji:failure}',
        },
        'ascii'            : {
            'icon'         : '-',
        },
        'ansi'             : {
            'color'        : [
                'bold',
                'red',
                ],
        },
    },
    'errors'               : {
        'unicode'          : {
            'icon'         : 'MRSM{system:formatting:emoji:error}',
        },
        'ascii'            : {
            'icon'         : 'ERROR',
        },
        'ansi'             : {
            'color'        : [
                'bold',
                'red',
            ],
        },
    },
    'info'                 : {
        'unicode'          : {
            'icon'         : 'MRSM{system:formatting:emoji:info}',
        },
        'ascii'            : {
            'icon'         : 'INFO',
        },
        'ansi'             : {
            'color'        : [
                'bold',
                'bright magenta',
            ],
        },
    },
    'question'             : {
        'unicode'          : {
            'icon'         : 'MRSM{system:formatting:emoji:question}',
        },
        'ascii'            : {
            'icon'         : '',
        },
        'ansi'             : {
            'color'        : [
                'green',
            ],
        },
    },
    'debug'                : {
        'unicode'          : {
            'icon'       : 'MRSM{system:formatting:emoji:debug}',
        },
        'ascii'            : {
            'icon'       : 'DEBUG',
        },
        'ansi'             : {
            'color'        : [
                'cyan',
            ],
        },
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

from meerschaum.config._paths import RESOURCES_PATH, DEFAULT_CONFIG_PATH

### build default config dictionary
default_config = dict()
default_config['meerschaum'] = default_meerschaum_config
default_config['system'] = default_system_config
default_config['pipes'] = default_pipes_config
default_config['plugins'] = default_plugins_config
#  default_config['experimental'] = default_experimental_config
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
##################################################################################

"""
