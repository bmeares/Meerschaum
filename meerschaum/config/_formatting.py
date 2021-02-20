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
    'unicode'             : default_unicode,
    'ansi'                : default_ansi,
    'emoji'               : {
        'hand'            : '👋',
        'error'           : '🛑',
        'failure'         : '💢',
        'success'         : '🎉',
        'warning'         : '⚠️',
        'info'            : '💬',
        'debug'           : '🐞',
        'question'        : '❓',
        'connector'       : '🔌',
        'metric'          : '📊',
        'location'        : '📍',
        'key'             : '🔑',
        'idea'            : '💡',
    },
    'warnings'            : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:warning}',
        },
        'ascii'           : {
            'icon'        : 'WARNING',
        },
        'ansi'            : {
            'color'       : [
                'bold',
                'yellow',
            ],
        },
    },
    'success'             : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:success}',
        },
        'ascii'           : {
            'icon'        : '+',
        },
        'ansi'            : {
            'color'       : [
                'bold',
                'bright green',
            ],
        },
    },
    'failure'             : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:failure}',
        },
        'ascii'           : {
            'icon'        : '-',
        },
        'ansi'            : {
            'color'       : [
                'bold',
                'red',
            ],
        },
    },
    'errors'              : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:error}',
        },
        'ascii'           : {
            'icon'        : 'ERROR',
        },
        'ansi'            : {
            'color'       : [
                'bold',
                'red',
            ],
        },
    },
    'info'                : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:info}',
        },
        'ascii'           : {
            'icon'        : 'INFO',
        },
        'ansi'            : {
            'color'       : [
                'bold',
                'bright magenta',
            ],
        },
    },
    'question'            : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:question}',
        },
        'ascii'           : {
            'icon'        : '',
        },
        'ansi'            : {
            'color'       : [
                'green',
            ],
        },
    },
    'debug'               : {
        'unicode'         : {
            'icon'        : 'MRSM{formatting:emoji:debug}',
        },
        'ascii'           : {
            'icon'        : 'DEBUG',
        },
        'ansi'            : {
            'color'       : [
                'cyan',
            ],
        },
    },

}
