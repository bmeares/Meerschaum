#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the Toga app in this module.
"""

from meerschaum.utils.packages import attempt_import
#  toga_gtk = attempt_import('toga_gtk', lazy=False, warn=False)
toga = attempt_import('toga', lazy=False, venv=None)

def build_app(app):
    box = toga.Box()

    return box
