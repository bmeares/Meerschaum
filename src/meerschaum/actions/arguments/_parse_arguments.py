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
    args, unknown = parser.parse_known_args(sysargs)
    if unknown: print(f"Unknown arguments: {unknown}")

    ### if --config is not empty, cascade down config
    ### and update new values on existing keys / add new keys/values
    if args.config is not None:
        import meerschaum.config
        from meerschaum.config._patch import write_patch
        from meerschaum.utils.misc import reload_package
        write_patch(args.config)
        #  meerschaum.config.apply_patch(args.config)
        #  meerschaum.config.config = meerschaum.config.preprocess_config(meerschaum.config.config, patch=args.config)
        reload_package(meerschaum.config)
        reload_package(meerschaum.config)

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
