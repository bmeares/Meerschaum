#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for parsing arguments
"""

from meerschaum.actions.arguments._parser import parser

def parse_arguments(sysargs : list) -> dict:
    """
    Parse a list of arguments into standard Meerschaum arguments.
    """
    from meerschaum.config import config as cf, get_config
    import copy

    sub_arguments = []
    sub_arg_indices = []
    begin_decorator, end_decorator = get_config('system', 'arguments', 'sub_decorators', patch=True)
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
            if sa.startswith(begin_decorator): sa = sa[len(begin_decorator):]
            if sa.endswith(end_decorator): sa = sa[:-1 *len(end_decorator)]
            sub_arguments.append(sa)
            ### remove sub-argument from action list
            sub_arg_indices.append(i)

    ### rebuild sysargs without sub_arguments
    sysargs = [
        word for i, word in enumerate(sysargs)
            if i not in sub_arg_indices
    ]

    args, unknown = parser.parse_known_args(sysargs)
    if unknown: print(f"Unknown arguments: {unknown}")

    ### if --config is not empty, cascade down config
    ### and update new values on existing keys / add new keys/values
    if args.config is not None:
        from meerschaum.config._patch import write_patch, apply_patch_to_config
        from meerschaum.config._paths import PATCH_PATH
        from meerschaum.utils.misc import reload_package
        import os, meerschaum.config
        write_patch(args.config)
        reload_package(meerschaum.config)
        reload_package(meerschaum.config)
        ### clean up patch so it's not loaded next time
        os.remove(PATCH_PATH)


    args_dict = vars(args)
    ### append decorated arguments to sub_arguments list
    if 'sub_args' not in args_dict: args_dict['sub_args'] = []
    if args_dict['sub_args'] is None: args_dict['sub_args'] = []
    sub_arguments = args_dict['sub_args'] + sub_arguments
    parsed_sub_arguments = []
    for sub_arg in sub_arguments:
        if ' ' in sub_arg:
            parsed_sub_arguments += sub_arg.split(' ')
        else:
            parsed_sub_arguments.append(sub_arg)
    args_dict['sub_args'] = parsed_sub_arguments

    ### remove None (but not False) args
    none_args = []
    for a, v in args_dict.items():
        if v is None:
            none_args.append(a)
    for a in none_args:
        del args_dict[a]

    ### location_key '[None]' or 'None' -> None
    if 'location_keys' in args_dict:
        args_dict['location_keys'] = [ None if lk in ('[None]', 'None') else lk for lk in args_dict['location_keys'] ]

    return parse_synonyms(args_dict)

def parse_line(line : str) -> dict:
    """
    Parse a line of text into standard Meerschaum arguments.

    line: str
        Line of text to be parsed

    returns: dict of arguments
    """
    import shlex
    args_list = shlex.split(line)
    return parse_arguments(
        shlex.split(line)
    )

def parse_synonyms(
        args_dict : dict
    ) -> dict:
    """
    Check for synonyms (e.g. force = True -> yes = True)
    """
    if args_dict['force']: args_dict['yes'] = True
    if args_dict['async']: args_dict['unblock'] = True
    return args_dict
