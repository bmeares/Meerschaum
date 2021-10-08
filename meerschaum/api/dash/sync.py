#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define syncing components.
"""

from __future__ import annotations
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False)
html, dcc = import_html(), import_dcc()
dbc = attempt_import('dash_bootstrap_components', lazy=False)


