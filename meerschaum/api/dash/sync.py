#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define syncing components.
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)


