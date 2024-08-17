#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This package includes argument parsing utilities.
"""

from meerschaum._internal.arguments._parse_arguments import (
    parse_arguments, parse_line, remove_leading_action,
    parse_dict_to_sysargs, split_chained_sysargs, split_pipeline_sysargs,
    sysargs_has_api_executor_keys, get_pipeline_sysargs,
    compress_pipeline_sysargs, remove_api_executor_keys,
)
from meerschaum._internal.arguments._parser import parser
from meerschaum.plugins import add_plugin_argument
