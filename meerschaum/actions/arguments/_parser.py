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
    description="Meerschaum actions parser",
    usage="mrsm [action]"
)

parser.add_argument(
    'action', nargs='+', help="Action to execute"
)
parser.add_argument(
    '--debug', '-d', action="store_true", help="Print debug statements (max verbosity)"
)
parser.add_argument(
    '--version', '-V', action="version", version=doc
)
parser.add_argument(
    '--nopretty', action="store_true", help="Print elements without 'pretty' formatting"
)
parser.add_argument(
    '--yes', '-y', action="store_true", help="Agree to the default choices for prompts"
)
parser.add_argument(
    '--force', '-f', action="store_true", help="Override safety checks"
)
parser.add_argument(
    '--port', '-p', type=int, help="The port on which to run the Web API server"
)
parser.add_argument(
    '--workers', '-w', type=int, help="How many workers to run a concurrent task"
)
parser.add_argument(
    '--params', '-P', type=string_to_dict, help=(
        "Parameters dictionary in JSON format or simple format. Simple format is for one-depth dictionaries and does not need braces or quotes.\nJSON Example:\n"
        "--params '{\"meerschaum\" : { \"connectors\" : \"main\" : { \"host\" : \"localhost\" } } }'"
        "\n\nSimple example:\n"
        "--params key1:value1,key2:value2"
    )
)
parser.add_argument(
    '--config', '-c', type=string_to_dict, help=(
        "Temporarily update configuration for a single command. "
        "See --params on formatting details."
    )
)
parser.add_argument(
    '--sub-args', '-A', nargs='+', help="Provide a list of arguments for subprocesses. You can also type sub-arguments in [] instead. E.g. `stack -A='--version'`, `ls [-lh]`, `echo -A these are sub-arguments`"
)
