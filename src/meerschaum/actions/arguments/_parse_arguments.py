#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for parsing arguments
"""

from meerschaum.actions.arguments._parser import parser
import argcomplete

def parse_arguments(sysargs : list) -> dict:
    argcomplete.autocomplete(parser)
    args = parser.parse_args(sysargs)
    return vars(args)


#  def add_autocomplete():

