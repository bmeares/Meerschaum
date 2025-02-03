#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module creates the argparse Parser.
"""

from __future__ import annotations
import sys
import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from meerschaum.utils.typing import Union, Dict, List, Any, Tuple, Callable
from meerschaum.utils.misc import string_to_dict
from meerschaum.utils.warnings import error

class InvalidArgumentException(Exception):
    """
    Raised when argparse failes to parse arguments.
    """

_original_argparse_error = argparse.ArgumentParser.error
_original_argparse_parse_known_args = argparse.ArgumentParser.parse_known_args
def _new_argparse_error(message: str):
    error(message, InvalidArgumentException, stack=False)

class ArgumentParser(argparse.ArgumentParser):
    """Override the built-in `argparse` error handling."""

    def parse_known_args(self, *args, exit_on_error: bool = False, **kw):
        _error_bkp = self.error
        if not exit_on_error:
            self.error = _new_argparse_error
        result = _original_argparse_parse_known_args(self, *args, **kw)
        self.error = _error_bkp
        return result


def parse_datetime(dt_str: str) -> Union[datetime, int, str]:
    """Parse a string into a datetime."""
    from meerschaum.utils.misc import is_int, round_time
    if is_int(dt_str):
        return int(dt_str)

    if dt_str in ('None', '[None]'):
        return 'None'

    from meerschaum.utils.packages import attempt_import
    dateutil_parser, dateutil_relativedelta = attempt_import(
        'dateutil.parser', 'dateutil.relativedelta'
    )
    relativedelta = dateutil_relativedelta.relativedelta

    ago_pattern = r'(\d+)\s+(days?|minutes?|seconds?|hours?|weeks?|months?|years?)\s+ago'
    round_pattern = r'(\d+)\s+(days?|minutes?|seconds?|hours?|weeks?|years?)'
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    ago_matches = re.findall(ago_pattern, dt_str.lower())

    try:
        if dt_str.lower() == 'now':
            dt = now
        elif ago_matches:
            val_str, unit_str = ago_matches[0]
            if not unit_str.endswith('s'):
                unit_str += 's'
            val = int(val_str) if is_int(val_str) else float(val_str)
            ago_delta = relativedelta(**{unit_str: val})
            round_part = dt_str.lower().split('ago ')[-1]
            round_delta = None
            if round_part:
                round_matches = re.findall(round_pattern, round_part)
                if round_matches:
                    round_val_str, round_unit_str = round_matches[0]
                    if not round_unit_str.endswith('s'):
                        round_unit_str += 's'
                    round_val = (
                        int(round_val_str)
                        if is_int(round_val_str)
                        else float(round_val_str)
                    )
                    round_delta = timedelta(**{round_unit_str: round_val})

            dt = now - ago_delta
            if round_delta is not None:
                dt = round_time(dt, round_delta)
        else:
            dt = dateutil_parser.parse(dt_str)
    except Exception:
        dt = None
    if dt is None:
        from meerschaum.utils.warnings import error
        error(f"'{dt_str}' is not a valid datetime format.", stack=False)

    if isinstance(dt, datetime):
        from meerschaum.utils.dtypes import coerce_timezone
        dt = coerce_timezone(dt)

    return dt


def parse_executor_keys(executor_keys_str: str) -> Union[str, None]:
    """
    Ensure that only API keys are provided for executor_keys.
    """
    if executor_keys_str in ('local', 'systemd'):
        return executor_keys_str

    if executor_keys_str.lower() == 'none':
        return 'local'

    if not executor_keys_str.startswith('api:'):
        from meerschaum.utils.warnings import error
        error(f"Invalid exectutor keys '{executor_keys_str}'.", stack=False)

    return executor_keys_str


def parse_help(sysargs: Union[List[str], Dict[str, Any]]) -> None:
    """Parse the `--help` flag to determine which help message to print."""
    from meerschaum._internal.arguments._parse_arguments import parse_arguments, parse_line
    from meerschaum.actions import actions, get_subactions
    import textwrap
    from copy import deepcopy
    if isinstance(sysargs, list):
        args = parse_arguments(sysargs)
    elif isinstance(sysargs, dict):
        args = sysargs
    elif isinstance(sysargs, str):
        args = parse_line(sysargs)
    _args = deepcopy(args)
    del _args['action']
    if len(args['action']) == 0:
        return actions['show'](['help'], **_args)
    if args['action'][0] not in actions:
        return actions['show'](['actions'], **_args)

    ### Check for subactions.
    if len(args['action']) > 1:
        try:
            subaction = get_subactions(args['action'][0])[args['action'][1]]
        except Exception:
            subaction = None
        if subaction is not None:
            return print(textwrap.dedent(subaction.__doc__))

    try:
        doc = actions[args['action'][0]].__doc__
    except Exception:
        doc = None
    if doc is None:
        doc = "No help available for '" + f"{args['action'][0]}" + "'."
    return print(textwrap.dedent(doc))


def parse_version(sysargs: List[str]):
    """Print the Meerschaum version."""
    from meerschaum.config import __version__ as version
    from meerschaum.config import __doc__ as doc
    if '--nopretty' in sysargs:
        return print(version)
    return print(doc)


def parse_name(name_str: str) -> Union[str, None]:
    """
    Ensure that `--name` is not an empty string.
    """
    if not name_str:
        return None
    return name_str

def get_arguments_triggers() -> Dict[str, Tuple[str]]:
    """ """
    triggers = {}
    _actions = parser._actions
    for _a in _actions:
        triggers[_a.dest] = tuple(_a.option_strings)
    return triggers


parser = ArgumentParser(
    prog = 'mrsm',
    description = "Create and Build Pipes with Meerschaum.",
    usage = "mrsm [actions] {options}",
    add_help = False,
)
_seen_plugin_args = {}

groups = {}
groups['actions'] = parser.add_argument_group(title='Actions options')
groups['jobs'] = parser.add_argument_group(title='Jobs options')
groups['pipes'] = parser.add_argument_group(title='Pipes options')
groups['sync'] = parser.add_argument_group(title='Sync options')
groups['api'] = parser.add_argument_group(title='API options')
groups['plugins'] = parser.add_argument_group(title='Plugins options')
groups['packages'] = parser.add_argument_group(title='Packages options')
groups['debug'] = parser.add_argument_group(title='Debugging options')
groups['misc'] = parser.add_argument_group(title='Miscellaneous options')


### Actions options
groups['actions'].add_argument(
    'action', nargs='*', help="Actions list to execute. E.g. `start api`"
)
groups['actions'].add_argument(
    '-h', '--help', action='store_true', help="Print a help message for an action."
)
groups['actions'].add_argument(
    '--loop',
    action="store_true", help="Loop the specified action forever (only for select actions)"
)
groups['actions'].add_argument(
    '-y', '--yes', action="store_true", help="Agree to the default choices for prompts"
)
groups['actions'].add_argument(
    '-f', '--force', action="store_true", help="Override safety checks"
)
groups['actions'].add_argument(
    '--noask', '--no-ask', action='store_true',
    help="Automatically choose the defaults answers to questions. Does not result in data loss.",
)
groups['actions'].add_argument(
    '-A', '--sub-args', nargs=argparse.REMAINDER,
    help = (
        "Provide a list of arguments for subprocesses. " +
        "You can also type sub-arguments in [] instead." +
        " E.g. `stack -A='--version'`, `ls [-lh]`, `echo -A these are sub-arguments`"
    )
)

### Jobs options
groups['jobs'].add_argument(
    '-d', '--daemon', action='store_true',
    help = "Run an action as a background daemon to create a job."
)
groups['jobs'].add_argument(
    '--name', '--job-name', type=parse_name, help=(
        "Assign a name to a job. If no name is provided, a random name will be assigned."
    ),
)
groups['jobs'].add_argument(
    '-s', '--schedule', '--cron', type=str,
    help = (
        "Continue executing the action according to a schedule (e.g. 'every 1 seconds'). \n"
        + "Often used with `-d`. See this page for scheduling syntax:\n "
        + "https://red-engine.readthedocs.io/en/stable/condition_syntax/index.html"
    )
)
groups['jobs'].add_argument(
    '--restart', action='store_true',
    help=("Restart a job if not stopped manually."),
)
groups['jobs'].add_argument(
    '-e', '--executor-keys', type=parse_executor_keys,
    help=(
        "Execute jobs locally or remotely. "
        "Supported values are 'local', 'systemd', and 'api:{label}'."
    ),
)
groups['jobs'].add_argument(
    '--rm', action='store_true', help="Delete a job once it has finished executing."
)
### Pipes options
groups['pipes'].add_argument(
    '-c', '-C', '--connector-keys', nargs='+',
    help="List of connector keys to search for. e.g. -c sql:main api:main"
)
groups['pipes'].add_argument(
    '-m', '-M', '--metric-keys', nargs='+', help="List of metric keys to include in the search"
)
groups['pipes'].add_argument(
    '-l', '-L', '--location-keys', nargs='+', help="List of location keys to include in the search"
)
groups['pipes'].add_argument(
    '-i', '-I', '--mrsm-instance', '--instance', help=(
        "Connector Keys (type:label) to the Meerschaum instance for Pipe objects. " +
        "If label is omitted, use the configured default interface (usually 'sql:main')"
    )
)
groups['pipes'].add_argument(
    '-t', '--tags', nargs='+', help="Only include pipes with these tags.",
)


### Sync options
groups['sync'].add_argument(
    '--min-seconds', '--cooldown', type=float, help=(
        "The minimum number of seconds between syncing laps. Defaults to 1."
    )
)
groups['sync'].add_argument(
    '--timeout-seconds', '--timeout', type=float,
    help="The maximum number of seconds before cancelling a pipe's syncing job. Defaults to 300."
)
groups['sync'].add_argument(
    '--unblock', action="store_true", help="Run the action asynchronously, if possible.",
)
groups['sync'].add_argument(
    '--async', action="store_true",
    help="Run the action asynchronously, if possible. Alias for --unblock",
)
groups['sync'].add_argument(
    '--begin', type=parse_datetime, help="The begin datetime when syncing, fetching data."
)
groups['sync'].add_argument(
    '--end', type=parse_datetime, help="The end datetime when syncing, fetching data."
)
groups['sync'].add_argument(
    '--chunksize', type=int, help="How many rows per chunk. Defaults to 100,000."
)
groups['sync'].add_argument(
    '--batchsize', type=int, help=(
        "How many chunks to process in parallel. Defaults to number of CPUs."
    ),
)
groups['sync'].add_argument(
    '--chunk-minutes', type=int, help=(
        "Specify the chunk interval in minutes for verification syncs. Defaults to 1440."
    ),
)
groups['sync'].add_argument(
    '--chunk-hours', type=int, help=(
        "Specify the chunk interval in days for verification syncs. See `--chunk-minutes`."
    ),
)
groups['sync'].add_argument(
    '--chunk-days', type=int, help=(
        "Specify the chunk interval in days for verification syncs. See `--chunk-minutes`."
    ),
)
groups['sync'].add_argument(
    '--verify', action="store_true", help="Completely resync a pipe's entire interval.",
)
groups['sync'].add_argument(
    '--deduplicate', '--dedup', action="store_true",
    help="Remove duplicate rows from a pipe's table.",
)
groups['sync'].add_argument(
    '--bounded', '--bound', action="store_true",
    help=(
        "When verifying, do not sync outside "
        "the existing oldest and newest datetime bounds."
    )
)
groups['sync'].add_argument(
    '--skip-check-existing', '--allow-duplicates', action='store_true',
    help=(
        "Skip checking for duplicate rows when syncing. "
        "This improves performance when all rows to be synced are unique. "
    )
)
groups['sync'].add_argument(
    '--skip-enforce-dtypes', action='store_true',
    help=(
        "Skip enforcing incoming data to a pipe's dtypes. "
        "This improves performance when all rows are expected to already be of the correct type."
    ),
)
groups['sync'].add_argument(
    '--skip-chunks-with-greater-rowcounts', action='store_true',
    help="When verifying, skip chunks with rowcounts greater than the remote's."
)
groups['sync'].add_argument(
    '--check-rowcounts-only', action='store_true', help=(
        "Only compare row-counts when verifying pipes."
    ),
)
groups['sync'].add_argument(
    '--skip-hooks', action='store_true', help=(
        "Skip calling the sync hooks."
    )
)
groups['sync'].add_argument(
    '--cache', action='store_true',
    help=(
        "When syncing or viewing a pipe's data, sync to a local database for later analysis."
    )
)

### API options
groups['api'].add_argument(
    '-p', '--port', type=int, help="The port on which to run the Web API server"
)
groups['api'].add_argument(
    '--host', type=str,
    help="The host address to bind to for the API server. Defaults to '0.0.0.0'."
)
groups['api'].add_argument(
    '-w', '--workers', type=int,
    help = "How many workers to run a concurrent action (e.g. running the API or syncing pipes)"
)
groups['api'].add_argument(
    '--no-dash', '--nodash', action='store_true',
    help = 'When starting the API, do not start the Web interface.',
)
groups['api'].add_argument(
    '--private', '--private-mode', action='store_true',
    help = 'Require authentication for all endpoints.',
)
groups['api'].add_argument(
    '--secure', action='store_true',
    help = "Start the API in secure mode, only allowing `admin` users to execute actions.",
)
groups['api'].add_argument(
    '--no-auth', '--noauth', '--insecure', action='store_true',
    help = 'When starting the API, do not require authentication. WARNING: This is dangerous!',
)
groups['api'].add_argument(
    '--production', '--gunicorn', action='store_true',
    help = 'Start the API with the Gunicorn process manager.'
)
groups['api'].add_argument(
    '--keyfile', type=str,
    help = "Start the API server with this keyfile (requires --certfile).",
)
groups['api'].add_argument(
    '--certfile', type=str,
    help = "Start the API server with this certfile (requires --keyfile).",
)
### Plugins options
groups['plugins'].add_argument(
    '-r', '--repository', '--repo', type=str,
    help="Meerschaum plugins repository to connect to. Specify an API label (default: 'mrsm')"
)

### Packages options
groups['packages'].add_argument(
    '--venv', type=str,
    help="Choose which virtual environment to target when installing or removing packages.",
)
### Debugging Arguments
groups['debug'].add_argument(
    '--debug', action="store_true", help="Print debug statements (max verbosity)"
)
groups['debug'].add_argument(
    '--trace', action="store_true",
    help="Trace execution and open browser windows to visualize the stack."
)

### Miscellaneous arguments
groups['misc'].add_argument(
    '-V', '--version', action="store_true",
    help="Print the Meerschaum version and exit. Has no effect from within the shell."
)
groups['misc'].add_argument(
    '--patch', action="store_true", help="Patch parameters instead of overwriting."
)
groups['misc'].add_argument(
    '--nopretty', action="store_true", help="Print elements without 'pretty' formatting"
)
groups['misc'].add_argument(
    '--skip-deps', action="store_true", help="Skip dependencies when installing plugins.",
)
groups['misc'].add_argument(
    '-P', '--params', type=string_to_dict, help=(
        "Parameters dictionary in JSON format or simple format. " +
        "Simple format is for one-depth dictionaries and does not need braces or quotes." +
        "\nJSON Example:\n" +
        "--params '{\"meerschaum\" : { \"connectors\" : " +
        "\"main\" : { \"host\" : \"localhost\" } } }'" +
        "\n\nSimple example:\n" +
        "--params key1:value1,key2:value2"
    )
)
groups['misc'].add_argument(
    '--temporary', '--temp',
    action = 'store_true',
    help = (
        "Skip creating or modifying instance tables when working with pipes "
        + "(plugins and users still trigger table creation)."
    ),
)
groups['misc'].add_argument(
    '--gui', action='store_true',
    help="Open a DataFrame in an interactive pandasgui or matplotlib window."
)
groups['misc'].add_argument(
    '--shell', action='store_true',
    help="Open the Meerschaum shell and execute the provided action."
)
groups['misc'].add_argument(
    '--use-bash', action='store_true',
    help="Execute non-implemented actions via `bash -c`. Default behavior is to execute directly."
)
groups['misc'].add_argument(
    '--allow-shell-job', action='store_true',
    help = (
        "Allow shell commands to be run as background jobs. "
        + "Default behavior is to only allow recognized actions."
    )
)
