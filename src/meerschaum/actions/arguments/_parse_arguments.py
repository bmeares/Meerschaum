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
    import meerschaum.config
    args, unknown = parser.parse_known_args(sysargs)
    if unknown: print(f"Unknown arguments: {unknown}")

    ### if --config is not empty, cascade down config
    ### and update new values on existing keys / add new keys/values
    if args.config is not None:
        from meerschaum.config._patch import write_patch
        from meerschaum.config._paths import PATCH_PATH
        from meerschaum.utils.misc import reload_package
        import os
        write_patch(args.config)
        #  meerschaum.config.apply_patch(args.config)
        #  meerschaum.config.config = meerschaum.config.preprocess_config(meerschaum.config.config, patch=args.config)
        reload_package(meerschaum.config)
        reload_package(meerschaum.config)
        ### clean up patch so it's not loaded next time
        os.remove(PATCH_PATH)

    begin_decorator, end_decorator = meerschaum.config.config['system']['arguments']['sub_decorators']

    args_dict = vars(args)
    sub_arguments = []
    for i, action in enumerate(args_dict['action']):
        if action.startswith(begin_decorator) and action.endswith(end_decorator):
            ### remove decorators
            sub_arguments += action[1:-1].split(' ')
            ### remove sub-argument from action list
            del args_dict['action'][i]

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

    return parse_synonyms(vars(args))

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
    return args_dict
