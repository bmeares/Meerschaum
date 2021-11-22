#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum GUI definition. Start the GUI with `start gui`.
"""

from meerschaum.config.static import _static_config
from meerschaum.utils.packages import attempt_import
from meerschaum.config import __version__
from meerschaum.config._paths import PACKAGE_ROOT_PATH
toga = attempt_import('toga', lazy=False, venv=None)

from meerschaum.gui.build import build_app

icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'

app = toga.App(
    _static_config()['setup']['formal_name'],
    _static_config()['setup']['name'],
    author = _static_config()['setup']['author'],
    description = _static_config()['setup']['description'],
    icon = icon_path,
    version = __version__,
    home_page = _static_config()['setup']['url'],
    startup = build_app,
)
