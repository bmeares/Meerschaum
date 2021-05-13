#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for parsing arguments
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Dict, Any, Optional

from meerschaum.actions.arguments._parser import parser

def parse_arguments(sysargs : List[str]) -> dict[str, Any]:
    """
    Parse a list of arguments into standard Meerschaum arguments.
    Returns a dictionary of argument_name -> argument_value.

    :param sysargs:
        List of command-line arguments to process. Does not include the executable.
        E.g. ['show', 'version', '--nopretty']
    """
    import copy
    from meerschaum.config.static import _static_config

    sub_arguments = []
    sub_arg_indices = []
    begin_decorator, end_decorator = _static_config()['system']['arguments']['sub_decorators']
    found_begin_decorator = False
    for i, word in enumerate(sysargs):
        is_sub_arg = False
        if not found_begin_decorator:
            found_begin_decorator = word.startswith(begin_decorator)
            found_end_decorator = word.endswith(end_decorator)

        if found_begin_decorator:
            ### check if sub arg is ever closed
            for a in sysargs[i:]:
                if a.endswith(end_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        elif found_end_decorator:
            for a in sysargs[:i]:
                if a.startswith(begin_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        if is_sub_arg:
            ### remove decorators
            sa = word
            if sa.startswith(begin_decorator):
                sa = sa[len(begin_decorator):]
            if sa.endswith(end_decorator):
                sa = sa[:-1 * len(end_decorator)]
            sub_arguments.append(sa)
            ### remove sub-argument from action list
            sub_arg_indices.append(i)

    ### rebuild sysargs without sub_arguments
    filtered_sysargs = [
        word for i, word in enumerate(sysargs)
            if i not in sub_arg_indices
    ]

    args, unknown = parser.parse_known_args(filtered_sysargs)

    ### if --config is not empty, cascade down config
    ### and update new values on existing keys / add new keys/values
    if args.config is not None:
        from meerschaum.config._patch import write_patch, apply_patch_to_config
        from meerschaum.config._paths import PATCH_DIR_PATH
        from meerschaum.utils.packages import reload_package
        import os, meerschaum.config, shutil
        write_patch(args.config)
        reload_package('meerschaum')
        ### clean up patch so it's not loaded next time
        if PATCH_DIR_PATH.exists():
            shutil.rmtree(PATCH_DIR_PATH)

    args_dict = vars(args)
    args_dict['sysargs'] = sysargs
    args_dict['filtered_sysargs'] = filtered_sysargs
    ### append decorated arguments to sub_arguments list
    if 'sub_args' not in args_dict:
        args_dict['sub_args'] = []
    if args_dict['sub_args'] is None:
        args_dict['sub_args'] = []
    sub_arguments = args_dict['sub_args'] + sub_arguments
    parsed_sub_arguments = []
    for sub_arg in sub_arguments:
        if ' ' in sub_arg:
            parsed_sub_arguments += sub_arg.split(' ')
        else:
            parsed_sub_arguments.append(sub_arg)
    args_dict['sub_args'] = parsed_sub_arguments
    ### In case of empty subargs
    if args_dict['sub_args'] == ['']:
        args_dict['sub_args'] = []

    ### remove None (but not False) args
    none_args = []
    for a, v in args_dict.items():
        if v is None:
            none_args.append(a)
    for a in none_args:
        del args_dict[a]

    ### location_key '[None]' or 'None' -> None
    if 'location_keys' in args_dict:
        args_dict['location_keys'] = [
            None if lk in ('[None]', 'None')
            else lk for lk in args_dict['location_keys'] 
        ]

    return parse_synonyms(args_dict)

def parse_line(line : str) -> dict:
    """
    Parse a line of text into standard Meerschaum arguments.

    line: str
        Line of text to be parsed

    returns: dict of arguments
    """
    import shlex
    try:
        args_list = shlex.split(line)
        return parse_arguments(
            shlex.split(line)
        )
    except Exception as e:
        return {'action' : [], 'text' : line,}

def parse_synonyms(
        args_dict : Dict[str, Any]
    ) -> Dict[str, Any]:
    """
    Check for synonyms (e.g. force = True -> yes = True)
    """
    if args_dict.get('force', None):
        args_dict['yes'] = True
    if args_dict.get('async', None):
        args_dict['unblock'] = True
    if args_dict.get('mrsm_instance', None):
        args_dict['instance'] = args_dict['mrsm_instance']
    if args_dict.get('skip_check_existing', None):
        args_dict['check_existing'] = False
    return args_dict

def parse_dict_to_sysargs(
        args_dict : Dict[str, Any]
    ) -> List[str]:
    """
    Revert an arguments dictionary back to a command line list.
    """
    import json
    from meerschaum.actions.arguments._parser import get_arguments_triggers
    sysargs = []
    sysargs += args_dict.get('action', [])
    allow_none_args = {'location_keys'}

    triggers = get_arguments_triggers()

    for a, t in triggers.items():
        if a == 'action' or a not in args_dict:
            continue
        ### Add boolean flags
        if isinstance(args_dict[a], bool):
            if args_dict[a] is True:
                sysargs += [t[0]]
        else:
            ### Add list flags
            if (
                isinstance(args_dict[a], list) or isinstance(args_dict[a], tuple)
            ):
                if len(args_dict[a]) > 0:
                    sysargs += [t[0]] + list(args_dict[a])

            ### Add dict flags
            elif isinstance(args_dict[a], dict):
                if len(args_dict[a]) > 0:
                    sysargs += [t[0], json.dumps(args_dict[a])]

            ### Account for None and other values
            elif (args_dict[a] is not None) or (args_dict[a] is None and a in allow_none_args):
                sysargs += [t[0], args_dict[a]]

    return sysargs
