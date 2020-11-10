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

parser.add_argument(
    'action', nargs='+', help="Actions list to execute. E.g. `api start`"
)
parser.add_argument(
    '-C', '--connector-keys', nargs='+', help="List of connector keys to search for. e.g. -k sql:main api:main"
)
parser.add_argument(
    '-M', '--metric-keys', nargs='+', help="List of metric keys to include in the search."
)
parser.add_argument(
    '-L', '--location-keys', nargs='+', help="List of location keys to include in the search."
)
parser.add_argument(
    '-P', '--params', type=string_to_dict, help=(
        "Parameters dictionary in JSON format or simple format. Simple format is for one-depth dictionaries and does not need braces or quotes.\nJSON Example:\n"
        "--params '{\"meerschaum\" : { \"connectors\" : \"main\" : { \"host\" : \"localhost\" } } }'"
        "\n\nSimple example:\n"
        "--params key1:value1,key2:value2"
    )
)
parser.add_argument(
    '-I', '--mrsm-instance', help=(
        "Connector Keys (type:label) to the Meerschaum instance for Pipe objects. If label is omitted, assume `meta` for `sql` and `main` for `api`."
    )
)
parser.add_argument(
    '-A', '--sub-args', nargs='+', help="Provide a list of arguments for subprocesses. You can also type sub-arguments in [] instead. E.g. `stack -A='--version'`, `ls [-lh]`, `echo -A these are sub-arguments`"
)
parser.add_argument(
    '-l', '--loop', action="store_true", help="Loop the specified action forever (only for select actions)"
)
parser.add_argument(
    '--min-seconds', type=int, help="The minimum number of seconds between Pipe syncs"
)
parser.add_argument(
    '-d', '--debug', action="store_true", help="Print debug statements (max verbosity)"
)
parser.add_argument(
    '-V', '--version', action="version", version=doc
)
parser.add_argument(
    '--patch', action="store_true", help="Patch parameters instead of overwriting."
)
parser.add_argument(
    '--nopretty', action="store_true", help="Print elements without 'pretty' formatting"
)
parser.add_argument(
    '-y', '--yes', action="store_true", help="Agree to the default choices for prompts"
)
parser.add_argument(
    '-f', '--force', action="store_true", help="Override safety checks"
)
parser.add_argument(
    '-p', '--port', type=int, help="The port on which to run the Web API server"
)
parser.add_argument(
    '-w', '--workers', type=int, help="How many workers to run a concurrent task"
)
parser.add_argument(
    '--unblock', action="store_true", help="Run the action asynchronously, if possible.",
)
parser.add_argument(
    '--async', action="store_true", help="Run the action asynchronously, if possible. Alias for --unblock",
)
parser.add_argument(
    '-c', '--config', type=string_to_dict, help=(
        "Temporarily update configuration for a single command. "
        "See --params on formatting details."
    )
)
parser.add_argument(
    '--gui', action='store_true', help="Open a DataFrame in an interactive pandasgui window."
)
