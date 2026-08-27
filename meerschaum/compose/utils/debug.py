#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage debugging in the isolated environments.
"""

def get_debug_args(debug: bool = False):
    return ['--debug'] if debug else []
