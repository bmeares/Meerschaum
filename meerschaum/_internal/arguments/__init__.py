#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This package includes argument parsing utilities.
"""

from meerschaum._internal.arguments._parse_arguments import (
    parse_arguments, parse_line, remove_leading_action,
    parse_dict_to_sysargs,
)
from meerschaum._internal.arguments._parser import parser
from meerschaum.plugins import add_plugin_argument

__all__ = [
    'parser', 'parse_arguments', 'parse_line', 'add_plugin_argument', 'parse_dict_to_sysargs',
    'remove_leading_action',
]
