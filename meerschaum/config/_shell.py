#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default configuration for the Meerschaum shell.
"""

default_cmd = 'cmd'

default_shell_config = {
    'ansi'               : {
        'intro'          : {
            'rich'       : {
                'style'  : "bold bright_blue",
            },
            'color'      : [
                'bold',
                'bright blue',
            ],
        },
        'close_message'  : {
            'rich'       : {
                'style'  : 'bright_blue',
            },
            'color'      : [
                'bright blue',
            ],
        },
        'doc_header'     : {
            'rich'       : {
                'style'  : 'bright_blue',
            },
            'color'      : [
                'bright blue',
            ],
        },
        'undoc_header'   : {
            'rich'       : {
                'style'  : 'bright_blue',
            },
            'color'      : [
                'bright blue',
            ],
        },
        'ruler'          : {
            'rich'       : {
                'style'  : 'bold bright_blue',
            },
            'color'      : [
                'bold',
                'bright blue',
            ],
        },
        'prompt'         : {
            'rich'       : {
                'style'  : 'green',
            },
            'color'      : [
                'green',
            ],
        },
        'instance'       : {
            'rich'       : {
                'style'  : 'cyan',
            },
            'color'      : [
                'cyan',
            ],
        },
        'repo'           : {
            'rich'       : {
                'style'  : 'magenta',
            },
            'color'      : [
                'magenta',
            ],
        },
        'executor'       : {
            'rich'       : {
                'style'  : 'yellow',
            },
        },
        'username'       : {
            'rich'       : {
                'style'  : 'white',
            },
            'color'      : [
                'white',
            ],
        },
        'connected'      : {
            'rich'       : {
                'style'  : 'green',
            },
            'color'      : [
                'green',
            ],
        },
        'disconnected'   : {
            'rich'       : {
                'style'  : 'red',
            },
            'color'      : [
                'red',
            ],
        },
        'update_message' : {
            'rich'       : {
                'style'  : 'red',
            },
            'color'      : [
                'red',
            ],
        },
    },
    'ascii'              : {
        'intro'          : r"""       ___  ___  __   __   __
 |\/| |__  |__  |__) /__` /  ` |__|  /\  |  |  |\/|
 |  | |___ |___ |  \ .__/ \__, |  | /~~\ \__/  |  |""" + '\n',
        'prompt'         : '\n [ {username}@{instance} | {executor_keys} ] > ',
        'ruler'          : '-',
        'close_message'  : 'Thank you for using Meerschaum!',
        'doc_header'     : 'Meerschaum actions (`help <action>` for usage):',
        'undoc_header'   : 'Unimplemented actions:',
        'update_message' : "Update available!",
    },
    'unicode'            : {
        'intro'          : """
 █▄ ▄█ ██▀ ██▀ █▀▄ ▄▀▀ ▄▀▀ █▄█ ▄▀▄ █ █ █▄ ▄█
 █ ▀ █ █▄▄ █▄▄ █▀▄ ▄██ ▀▄▄ █ █ █▀█ ▀▄█ █ ▀ █\n""",
        'prompt'         : '\n [ {username}@{instance} | {executor_keys} ] ➤ ',
        'ruler'          : '─',
        'close_message'  : ' MRSM{formatting:emoji:hand} Thank you for using Meerschaum! ',
        'doc_header'     : 'Meerschaum actions (`help <action>` for usage):',
        'undoc_header'   : 'Unimplemented actions:',
        'update_message' : "MRSM{formatting:emoji:announcement} Update available!",
    },
    'timeout'            : 60,
    'max_history'        : 1000,
    'clear_screen'       : True,
    'bottom_toolbar'     : {
        'enabled'        : True,
    },
    'updates'            : {
        'check_remote'   : True,
        'refresh_minutes': 180,
    },
}
