#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define default values for the formatting key.
"""

default_unicode, default_ansi = True, True
import platform
if platform.system() == 'Windows':
    default_unicode, default_ansi = False, True

default_formatting_config = {
    'unicode'              : default_unicode,
    'ansi'                 : default_ansi,
    'emoji'                : {
        'hand'             : '👋',
        'error'            : '🛑',
        'failure'          : '💢',
        'failure_calm'     : '❌',
        'success'          : '🎉',
        'success_calm'     : '✅',
        'warning'          : '🔔',
        'info'             : '💬',
        'debug'            : '🐞',
        'question'         : '❓',
        'connector'        : '🔌',
        'metric'           : '📊',
        'location'         : '📍',
        'key'              : '🔑',
        'idea'             : '💡',
        'connected'        : '🟢',
        'disconnected'     : '🔴',
        'running'          : '🟢',
        'paused'           : '🟡',
        'stopped'          : '🔴',
        'tag'              : '🔖',
        'announcement'     : '📢',
    },
    'pipes'                : {
        'unicode'          : {
            'icons'        : {
                'connector': 'MRSM{formatting:emoji:connector} ',
                'metric'   : 'MRSM{formatting:emoji:metric} ',
                'location' : 'MRSM{formatting:emoji:location} ',
                'key'      : 'MRSM{formatting:emoji:key} ',
                'tag'      : 'MRSM{formatting:emoji:tag} ',
            },
        },
        'ascii'            : {
            'icons'        : {
                'connector': '',
                'metric'   : '',
                'location' : '',
                'key'      : '',
                'tag'      : '',
            },
        },
        'ansi'             : {
            'styles'       : {
                'connector': 'green',
                'metric'   : 'bright_blue',
                'location' : 'magenta',
                'key'      : '',
                'guide'    : 'dim',
                'none'     : 'black on magenta',
                'tags'     : 'bold yellow underline',
            },
        },
        '__repr__'         : {
            'ansi'         : {
                'styles'   : {
                    'Pipe': 'bold white',
                    'punctuation': 'white',
                    'connector': 'MRSM{formatting:pipes:ansi:styles:connector}',
                    'metric': 'MRSM{formatting:pipes:ansi:styles:metric}',
                    'location': 'MRSM{formatting:pipes:ansi:styles:location}',
                    'instance': '#d177a4',
                },
            },
        },
    },
    'warnings'             : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:warning}',
        },
        'ascii'            : {
            'icon'         : 'WARNING',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'bold yellow',
            },
        },
    },
    'success'              : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:success}',
        },
        'ascii'            : {
            'icon'         : '+',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'bold bright_green',
            },
        },
    },
    'failure'              : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:failure}',
        },
        'ascii'            : {
            'icon'         : '-',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'bold red',
            },
        },
    },
    'success_calm'         : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:success_calm}',
        },
        'ascii'            : {
            'icon'         : '+',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'pale_green3',
            },
        },
    },
    'failure_calm'         : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:failure_calm}',
        },
        'ascii'            : {
            'icon'         : 'x',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'indian red',
            },
        },
    },
    'errors'               : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:error}',
        },
        'ascii'            : {
            'icon'         : 'ERROR',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'bold red',
            },
        },
    },
    'info'                 : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:info}',
        },
        'ascii'            : {
            'icon'         : 'INFO',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'bright_magenta',
            },
        },
    },
    'question'             : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:question}',
        },
        'ascii'            : {
            'icon'         : '',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'green',
            },
        },
    },
    'debug'                : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:debug}',
        },
        'ascii'            : {
            'icon'         : 'DEBUG',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'cyan',
            },
        },
    },
    'connected'            : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:connected}',
        },
        'ascii'            : {
            'icon'         : '',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'green',
            },
        },
    },
    'disconnected'         : {
        'unicode'          : {
            'icon'         : 'MRSM{formatting:emoji:disconnected}',
        },
        'ascii'            : {
            'icon'         : '',
        },
        'ansi'             : {
            'rich'         : {
                'style'    : 'red',
            },
        },
    },

}
