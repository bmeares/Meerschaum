#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define syncing components.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
html = attempt_import('dash_html_components', warn=False)


