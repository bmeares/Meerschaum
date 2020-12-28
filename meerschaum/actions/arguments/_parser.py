#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module creates the argparse Parser
"""

import argparse
from meerschaum.config import __doc__ as doc
from meerschaum.utils.misc import string_to_dict
import json

parser = argparse.ArgumentParser(
    description="Create and Build Pipes with Meerschaum",
    usage="mrsm [action with optional arguments] {options}"
)

groups = dict()
groups['actions'] = parser.add_argument_group(title='Actions options')
groups['pipes'] = parser.add_argument_group(title='Pipes options')
groups['sync'] = parser.add_argument_group(title='Sync options')
groups['api'] = parser.add_argument_group(title='API options')
groups['plugins'] = parser.add_argument_group(title='Plugins options')
groups['misc'] = parser.add_argument_group(title='Miscellaneous options')


### Actions options
groups['actions'].add_argument(
    'action', nargs='+', help="Actions list to execute. E.g. `api start`"
)
groups['actions'].add_argument(
    '--loop', action="store_true", help="Loop the specified action forever (only for select actions)"
)
groups['actions'].add_argument(
    '-y', '--yes', action="store_true", help="Agree to the default choices for prompts"
)
groups['actions'].add_argument(
    '-f', '--force', action="store_true", help="Override safety checks"
)
groups['actions'].add_argument(
    '-A', '--sub-args', nargs=argparse.REMAINDER,
    help=(
        "Provide a list of arguments for subprocesses. You can also type sub-arguments in [] instead." + 
        " E.g. `stack -A='--version'`, `ls [-lh]`, `echo -A these are sub-arguments`"
    )
)

### Pipes options
groups['pipes'].add_argument(
    '-c', '-C', '--connector-keys', nargs='+', help="List of connector keys to search for. e.g. -C sql:main api:main"
)
groups['pipes'].add_argument(
    '-m', '-M', '--metric-keys', nargs='+', help="List of metric keys to include in the search. e.g. -M "
)
groups['pipes'].add_argument(
    '-l', '-L', '--location-keys', nargs='+', help="List of location keys to include in the search."
)
groups['pipes'].add_argument(
    '-i', '-I', '--mrsm-instance', '--instance', help=(
        "Connector Keys (type:label) to the Meerschaum instance for Pipe objects. " +
        "If label is omitted, use the configured default interface (usually 'sql:main')"
    )
)

### Sync options
groups['sync'].add_argument(
    '--min-seconds', type=int, help="The minimum number of seconds between syncing laps"
)
groups['sync'].add_argument(
    '--unblock', action="store_true", help="Run the action asynchronously, if possible.",
)
groups['sync'].add_argument(
    '--async', action="store_true", help="Run the action asynchronously, if possible. Alias for --unblock",
)

### API options
groups['api'].add_argument(
    '-p', '--port', type=int, help="The port on which to run the Web API server"
)
groups['api'].add_argument(
    '-w', '--workers', type=int,
    help="How many workers to run a concurrent action (e.g. running the API or syncing pipes)"
)

### Plugins options
groups['plugins'].add_argument(
    '-r', '--repository', '--repo', type=str,
    help="Meerschaum plugins repository to connect to. Specify an API label (default: 'mrsm')"
)

### Miscellaneous arguments
groups['misc'].add_argument(
    '-d', '--debug', action="store_true", help="Print debug statements (max verbosity)"
)
groups['misc'].add_argument(
    '-V', '--version', action="version", version=doc
)
groups['misc'].add_argument(
    '--patch', action="store_true", help="Patch parameters instead of overwriting."
)
groups['misc'].add_argument(
    '--nopretty', action="store_true", help="Print elements without 'pretty' formatting"
)
groups['misc'].add_argument(
    '-P', '--params', type=string_to_dict, help=(
        "Parameters dictionary in JSON format or simple format. Simple format is for one-depth dictionaries and does not need braces or quotes.\nJSON Example:\n"
        "--params '{\"meerschaum\" : { \"connectors\" : \"main\" : { \"host\" : \"localhost\" } } }'"
        "\n\nSimple example:\n"
        "--params key1:value1,key2:value2"
    )
)
groups['misc'].add_argument(
    '--config', type=string_to_dict, help=(
        "Temporarily update configuration for a single command. "
        "See --params on formatting details."
    )
)
groups['misc'].add_argument(
    '--gui', action='store_true', help="Open a DataFrame in an interactive pandasgui or matplotlib window."
)
groups['misc'].add_argument(
    '--shell', action='store_true', help="Open a Meerschaum shell and execute the provided action"
)
