#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum GUI definition. Start the GUI with `start gui`.
"""

from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
from meerschaum.config import __version__
from meerschaum.config._paths import PACKAGE_ROOT_PATH
from meerschaum.utils.threading import Lock

from meerschaum._internal.gui.app import MeerschaumApp

icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'

locks = {'app': Lock()}
_app = None

def get_app(**kw) -> MeerschaumApp:
    """Instantiate and return the main app."""
    global _app
    if _app is None:
        with locks['app']:
            _app = build_app(**kw)
    return _app

def build_app(**kw) -> MeerschaumApp:
    """Construct and return an instance of the GUI application."""
    _kw = dict(
        formal_name = STATIC_CONFIG['setup']['formal_name'],
        app_id = STATIC_CONFIG['setup']['app_id'],
        app_name = STATIC_CONFIG['setup']['name'],
        author = STATIC_CONFIG['setup']['author'],
        description = STATIC_CONFIG['setup']['description'],
        icon = icon_path,
        version = __version__,
        home_page = STATIC_CONFIG['setup']['url'],
    )
    _kw.update(kw)
    return MeerschaumApp(**_kw)
