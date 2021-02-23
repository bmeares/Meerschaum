#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default configuration for the Meerschaum shell.
"""

import platform
default_cmd = 'cmd' if platform.system() != 'Windows' else 'cmd2'

default_shell_config = {
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
        'close_message': ' MRSM{formatting:emoji:hand} Thank you for using Meerschaum! ',
        'doc_header'   : 'Meerschaum actions (`help <action>` for usage):',
        'undoc_header' : 'Unimplemented actions:',
    },
    'timeout'          : 60,
    'max_history'      : 1000,
    'clear_screen'     : True,
    'cmd'              : default_cmd,
}
