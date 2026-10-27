#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
v4's dashboard is the Terminal page now (`meerschaum.api.dash.pages.terminal`, at
`/dash/terminal`); `/dash` is the Overview (`meerschaum.api.dash.pages.overview`).
This module keeps `pages.dashboard.layout` for code that imports it.
"""

from meerschaum.api.dash.pages.terminal import layout

__all__ = ('layout',)
